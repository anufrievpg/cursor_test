#!/usr/bin/env python3
"""
s3_upload.py
============
Загрузчик результата выгрузки 1С:ДО в S3-совместимое хранилище (MinIO / Ceph RadosGW).

Возможности:
  • Multipart upload для файлов любого размера (проверено вплоть до 2.5 ТБ):
      - размер части рассчитывается автоматически, чтобы уложиться в лимит S3 (9 999 частей)
      - параллельная загрузка частей (--part-threads)
      - возобновление прерванного multipart: UploadId + уже загруженные части
        сохраняются в state-файле и переживают перезапуск процесса
  • Версионирование:
      - все исторические версии загружаются в ОДИН S3-ключ в хронологическом порядке
        (старые первыми, текущая последней)
      - S3-версионирование создаёт отдельную запись для каждого PUT
      - каждая версия несёт метаданные: автор, дата, комментарий (S3 user metadata)
  • Объектная дедупликация по SHA-256:
      - индекс SHA → canonical_key хранится локально (.json.gz) и синхронизируется
        в S3 (_dedup/index.json.gz) при запуске и завершении
      - дубль заменяется ссылочным объектом (~0.5 КБ) вместо повторного хранения
      - дедупликация работает МЕЖДУ разными файлами;
        версии одного файла всегда загружаются полностью (чистая история версий)
  • Возобновляемая загрузка:
      - state-файл (JSON) хранит список завершённых файлов/версий
      - повторный запуск с теми же параметрами пропускает уже загруженное
  • Параллельная загрузка файлов (--threads)

Требования:
    pip install boto3 tqdm

Использование (MinIO):
    python s3_upload.py \
        --export-dir /mnt/export_1cdo \
        --endpoint-url http://minio.company.ru:9000 \
        --access-key ACCESS \
        --secret-key SECRET \
        --bucket 1cdo-archive \
        --prefix 1CDO/ \
        --threads 4 \
        --part-threads 8

Использование (Ceph RadosGW):
    python s3_upload.py \
        --export-dir /mnt/export_1cdo \
        --endpoint-url https://radosgw.company.ru \
        --access-key ACCESS \
        --secret-key SECRET \
        --bucket 1cdo-archive \
        --region default \
        --threads 4 \
        --part-threads 8
"""

import argparse
import gzip
import hashlib
import json
import logging
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

DEDUP_INDEX_S3_KEY  = "_dedup/index.json.gz"
DEDUP_REF_MAGIC     = "__dedup_ref__"     # маркер ссылочного объекта
MAX_S3_PARTS        = 9_999              # S3 позволяет до 10 000 частей
MIN_PART_BYTES      = 5 * 1024 * 1024   # 5 МиБ — минимум S3
PART_ALIGNMENT      = 8 * 1024 * 1024   # выравниваем части по 8 МиБ
DEFAULT_MULTIPART_THRESHOLD_MIB = 100   # 100 МиБ — порог multipart по умолчанию
S3_METADATA_MAX_BYTES = 1_900           # предел 2048, оставляем запас


# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

@dataclass
class Config:
    export_dir:         Path
    endpoint_url:       str
    access_key:         str
    secret_key:         str
    bucket:             str
    prefix:             str  = "1CDO/"
    region:             str  = "us-east-1"
    verify_ssl:         bool = True
    threads:            int  = 2       # параллельных файлов
    part_threads:       int  = 4       # параллельных частей в одном multipart
    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD_MIB * 1024 * 1024
    upload_versions:    bool = True    # загружать исторические версии
    enable_dedup:       bool = True
    state_file:         Path = field(default_factory=lambda: Path("s3_upload_state.json"))
    dedup_index_file:   Path = field(default_factory=lambda: Path("dedup_index.json.gz"))
    log_file:           Path = field(default_factory=lambda: Path("s3_upload.log"))
    dedup_save_interval: int = 500     # сохранять dedup-индекс каждые N файлов


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def truncate_utf8(s: str, max_bytes: int) -> str:
    """Обрезает строку так, чтобы её UTF-8 кодировка не превышала max_bytes."""
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def optimal_part_size(file_size: int) -> int:
    """
    Возвращает минимальный размер части, при котором количество частей
    не превысит MAX_S3_PARTS. Результат выровнен по PART_ALIGNMENT.
    """
    min_size = max(MIN_PART_BYTES, math.ceil(file_size / MAX_S3_PARTS))
    aligned  = math.ceil(min_size / PART_ALIGNMENT) * PART_ALIGNMENT
    # Проверяем: если выравнивание само по себе укладывает нас в лимит — OK.
    # Иначе делаем ещё один шаг вверх.
    while math.ceil(file_size / aligned) > MAX_S3_PARTS:
        aligned += PART_ALIGNMENT
    return aligned


def sha256_of_file(path: Path) -> str:
    """Потоковое вычисление SHA-256. Не загружает файл целиком в RAM."""
    h   = hashlib.sha256()
    buf = 4 * 1024 * 1024  # 4 МиБ буфер
    with open(path, "rb") as fh:
        while chunk := fh.read(buf):
            h.update(chunk)
    return h.hexdigest()


def build_s3_metadata(file_entry: dict, ver_info: dict, sha256: str) -> dict:
    """
    Формирует словарь S3 user-defined metadata из метаданных 1С:ДО.
    Ключи будут переданы boto3 и автоматически получат префикс x-amz-meta-.
    Суммарный размер не превышает S3_METADATA_MAX_BYTES.
    """
    def s(val: object, max_b: int = 200) -> str:
        return truncate_utf8(str(val or ""), max_b)

    meta = {
        "source-system":  "1c-do",
        "original-name":  s(file_entry.get("name"),          250),
        "author":         s(file_entry.get("author"),         120),
        "created-at":     s(file_entry.get("created_at"),      30),
        "modified-at":    s(file_entry.get("modified_at"),     30),
        "guid-1c":        s(file_entry.get("guid_1c"),         40),
        "is-deleted":     "true" if file_entry.get("is_deleted") else "false",
        "sha256":         sha256,
        "ver-number":     str(ver_info.get("number",   1)),
        "ver-total":      str(ver_info.get("total",    1)),
        "ver-author":     s(ver_info.get("version_author"),   120),
        "ver-date":       s(ver_info.get("version_date"),      30),
        "ver-comment":    s(ver_info.get("comment"),          300),
    }

    # Сокращаем комментарий если суммарно превышает лимит
    total = sum(len(k) + len(v) for k, v in meta.items())
    if total > S3_METADATA_MAX_BYTES:
        overflow = total - S3_METADATA_MAX_BYTES
        comment  = meta["ver-comment"]
        meta["ver-comment"] = comment[:max(0, len(comment) - overflow)]

    return meta


# ---------------------------------------------------------------------------
# Индекс дедупликации
# ---------------------------------------------------------------------------

class DedupIndex:
    """
    Хранит маппинг SHA-256 → {key, size, registered_at}.

    Жизненный цикл:
      1. load_local()         — загружаем из файла на диске (если есть)
      2. download_from_s3()   — скачиваем с S3 если локальный файл отсутствует
      3. lookup() / register() — используем в процессе загрузки
      4. save_local()         — периодически сохраняем локально
      5. upload_to_s3()       — финальная синхронизация в S3 по завершении
    """

    def __init__(self, local_path: Path):
        self._path  = local_path
        self._data: dict[str, dict] = {}
        self._lock  = threading.Lock()

    def load_local(self) -> bool:
        if not self._path.exists():
            return False
        try:
            with gzip.open(self._path, "rt", encoding="utf-8") as fh:
                self._data = json.load(fh)
            logging.info("Dedup index: loaded %d entries from disk", len(self._data))
            return True
        except Exception as exc:
            logging.warning("Dedup index: could not load from disk: %s", exc)
            return False

    def download_from_s3(self, s3_client, bucket: str) -> bool:
        try:
            resp = s3_client.get_object(Bucket=bucket, Key=DEDUP_INDEX_S3_KEY)
            raw  = gzip.decompress(resp["Body"].read())
            self._data = json.loads(raw)
            logging.info("Dedup index: downloaded %d entries from S3", len(self._data))
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404", "NoSuchBucket"):
                logging.info("Dedup index: not found in S3, starting fresh")
            else:
                logging.warning("Dedup index S3 download error: %s", exc)
        except Exception as exc:
            logging.warning("Dedup index download error: %s", exc)
        return False

    def save_local(self):
        try:
            with gzip.open(self._path, "wt", encoding="utf-8") as fh:
                json.dump(self._data, fh, ensure_ascii=False)
        except OSError as exc:
            logging.warning("Dedup index: local save failed: %s", exc)

    def upload_to_s3(self, s3_client, bucket: str):
        try:
            payload = gzip.compress(
                json.dumps(self._data, ensure_ascii=False).encode("utf-8")
            )
            s3_client.put_object(
                Bucket=bucket, Key=DEDUP_INDEX_S3_KEY,
                Body=payload,
                ContentType="application/json",
                ContentEncoding="gzip",
            )
            logging.info("Dedup index: uploaded to S3 (%d entries)", len(self._data))
        except Exception as exc:
            logging.error("Dedup index: S3 upload failed: %s", exc)

    def lookup(self, sha256: str) -> Optional[dict]:
        with self._lock:
            return self._data.get(sha256)

    def register(self, sha256: str, canonical_key: str, size: int):
        with self._lock:
            if sha256 not in self._data:
                self._data[sha256] = {
                    "key":  canonical_key,
                    "size": size,
                    "ts":   time.strftime("%Y-%m-%dT%H:%M:%S"),
                }


# ---------------------------------------------------------------------------
# Состояние загрузки (возобновление)
# ---------------------------------------------------------------------------

class UploadState:
    """
    Персистентное состояние в JSON-файле:
      done      — завершённые файлы/версии: state_key → {s3_key, sha256, ts}
      multipart — активные multipart-загрузки: state_key → {upload_id, parts:[]}

    Thread-safe.
    """

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()
        self._d: dict = {"done": {}, "multipart": {}}
        self._load()

    def _load(self):
        if self._path.exists():
            try:
                self._d = json.loads(self._path.read_text(encoding="utf-8"))
                logging.info("Upload state: %d completed items", len(self._d.get("done", {})))
            except Exception as exc:
                logging.warning("Upload state: could not load: %s", exc)

    def _flush(self):
        """Вызывается только внутри захваченного self._lock."""
        try:
            self._path.write_text(
                json.dumps(self._d, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass

    def is_done(self, key: str) -> bool:
        with self._lock:
            return key in self._d.get("done", {})

    def mark_done(self, state_key: str, s3_key: str, sha256: str,
                  is_ref: bool = False):
        with self._lock:
            self._d.setdefault("done", {})[state_key] = {
                "s3_key": s3_key, "sha256": sha256,
                "is_ref": is_ref, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            self._d.get("multipart", {}).pop(state_key, None)
            self._flush()

    def get_multipart(self, state_key: str) -> Optional[dict]:
        with self._lock:
            return self._d.get("multipart", {}).get(state_key)

    def save_multipart(self, state_key: str, upload_id: str, parts: list[dict]):
        with self._lock:
            self._d.setdefault("multipart", {})[state_key] = {
                "upload_id": upload_id, "parts": parts,
            }
            self._flush()

    def clear_multipart(self, state_key: str):
        with self._lock:
            self._d.get("multipart", {}).pop(state_key, None)
            self._flush()


# ---------------------------------------------------------------------------
# Пул S3-клиентов (один экземпляр boto3.client на поток)
# ---------------------------------------------------------------------------

class S3ClientPool:
    """
    boto3.client не является thread-safe.
    Этот класс создаёт и кэширует отдельный клиент для каждого потока.
    """

    def __init__(self, config: Config):
        self._cfg   = config
        self._local = threading.local()

    def get(self) -> "boto3.client":
        if not hasattr(self._local, "client"):
            self._local.client = boto3.client(
                "s3",
                endpoint_url=self._cfg.endpoint_url,
                aws_access_key_id=self._cfg.access_key,
                aws_secret_access_key=self._cfg.secret_key,
                region_name=self._cfg.region,
                verify=self._cfg.verify_ssl,
                config=BotoConfig(
                    retries={"max_attempts": 5, "mode": "adaptive"},
                    max_pool_connections=self._cfg.part_threads + 4,
                ),
            )
        return self._local.client


# ---------------------------------------------------------------------------
# Загрузчик (S3Uploader)
# ---------------------------------------------------------------------------

class S3Uploader:
    """
    Отвечает только за физическую запись объектов в S3:
      upload_object()       — простой PUT или multipart (выбирается автоматически)
      upload_dedup_ref()    — крошечный JSON-объект-ссылка вместо дубля
    """

    def __init__(self, cfg: Config, pool: S3ClientPool, state: UploadState):
        self._cfg   = cfg
        self._pool  = pool
        self._state = state

    # --- Основной метод ---

    def upload_object(
        self,
        local_path: Path,
        bucket:     str,
        key:        str,
        metadata:   dict,
        state_key:  str,
        file_size:  int,
    ) -> None:
        if file_size < self._cfg.multipart_threshold:
            self._simple_put(local_path, bucket, key, metadata, file_size)
        else:
            mp = self._state.get_multipart(state_key)
            self._multipart_upload(
                local_path, bucket, key, metadata, file_size, state_key,
                upload_id=mp["upload_id"] if mp else None,
                done_parts={p["PartNumber"]: p["ETag"] for p in (mp or {}).get("parts", [])},
            )

    # --- Ссылочный объект при дедупликации ---

    def upload_dedup_ref(
        self,
        bucket:        str,
        key:           str,
        sha256:        str,
        canonical_key: str,
        size_bytes:    int,
        metadata:      dict,
    ) -> None:
        body = json.dumps({
            DEDUP_REF_MAGIC: True,
            "sha256":        sha256,
            "canonical_key": canonical_key,
            "size_bytes":    size_bytes,
        }, ensure_ascii=False).encode("utf-8")

        self._pool.get().put_object(
            Bucket=bucket, Key=key, Body=body,
            ContentType="application/json",
            Metadata={**metadata, "is-dedup-ref": "true"},
        )

    # --- Simple PUT ---

    def _simple_put(self, local_path, bucket, key, metadata, file_size):
        with open(local_path, "rb") as fh:
            self._pool.get().put_object(
                Bucket=bucket, Key=key, Body=fh,
                ContentLength=file_size, Metadata=metadata,
            )

    # --- Multipart upload ---

    def _multipart_upload(
        self,
        local_path: Path,
        bucket:     str,
        key:        str,
        metadata:   dict,
        file_size:  int,
        state_key:  str,
        upload_id:  Optional[str],
        done_parts: dict,                # {part_number: etag}
    ) -> None:
        s3        = self._pool.get()
        part_size = optimal_part_size(file_size)
        num_parts = math.ceil(file_size / part_size)

        # Создаём или возобновляем multipart upload
        if upload_id is None:
            resp      = s3.create_multipart_upload(Bucket=bucket, Key=key, Metadata=metadata)
            upload_id = resp["UploadId"]
            logging.debug("Multipart created: %s  upload_id=%s", key, upload_id)
        else:
            logging.info("Multipart resume: %s  parts done=%d/%d", key, len(done_parts), num_parts)

        # Немедленно сохраняем UploadId (на случай краша до завершения)
        completed: list[dict] = [{"PartNumber": n, "ETag": e} for n, e in done_parts.items()]
        self._state.save_multipart(state_key, upload_id, completed)

        # Список частей, которые ещё нужно загрузить
        pending = [
            (pn,
             (pn - 1) * part_size,
             min(part_size, file_size - (pn - 1) * part_size))
            for pn in range(1, num_parts + 1)
            if pn not in done_parts
        ]

        bytes_already = len(done_parts) * part_size
        label = f"  {local_path.name[:45]}"
        pbar  = tqdm(total=file_size, initial=bytes_already,
                     unit="B", unit_scale=True, desc=label, leave=False) \
                if TQDM_AVAILABLE else None

        try:
            with ThreadPoolExecutor(max_workers=self._cfg.part_threads) as pool:
                futures = {
                    pool.submit(
                        self._upload_one_part,
                        local_path, bucket, key, upload_id,
                        pn, offset, length
                    ): pn
                    for pn, offset, length in pending
                }
                for fut in as_completed(futures):
                    pn, etag = fut.result()        # бросает исключение при ошибке
                    completed.append({"PartNumber": pn, "ETag": etag})
                    self._state.save_multipart(state_key, upload_id, completed)
                    if pbar:
                        pbar.update(part_size)
        except Exception:
            if pbar:
                pbar.close()
            raise   # UploadId сохранён в state → возобновление при следующем запуске

        if pbar:
            pbar.close()

        completed.sort(key=lambda x: x["PartNumber"])
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": completed},
        )
        self._state.clear_multipart(state_key)
        logging.debug("Multipart complete: %s", key)

    def _upload_one_part(
        self,
        local_path: Path,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        offset: int,
        length: int,
    ) -> tuple[int, str]:
        # Каждый поток открывает файл самостоятельно и переходит к нужному смещению
        with open(local_path, "rb") as fh:
            fh.seek(offset)
            data = fh.read(length)
        resp = self._pool.get().upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id,
            PartNumber=part_num, Body=data,
        )
        return part_num, resp["ETag"]


# ---------------------------------------------------------------------------
# Оркестратор (Migrator)
# ---------------------------------------------------------------------------

class Migrator:

    def __init__(self, cfg: Config):
        self.cfg      = cfg
        self._pool    = S3ClientPool(cfg)
        self.state    = UploadState(cfg.state_file)
        self.dedup    = DedupIndex(cfg.dedup_index_file)
        self.uploader = S3Uploader(cfg, self._pool, self.state)
        self.stats    = {
            "files_ok":         0,
            "files_skipped":    0,
            "files_error":      0,
            "versions_ok":      0,
            "dedup_refs":       0,
            "bytes_uploaded":   0,
            "bytes_dedup_saved": 0,
        }
        self._lock            = threading.Lock()
        self._since_last_save = 0

    # ------------------------------------------------------------------
    # Точка входа
    # ------------------------------------------------------------------

    def run(self):
        if not self.cfg.export_dir.is_dir():
            logging.error("Export dir not found: %s", self.cfg.export_dir)
            sys.exit(1)

        index_path = self.cfg.export_dir / "_export_index.json"
        if not index_path.exists():
            logging.error("_export_index.json not found in %s", self.cfg.export_dir)
            sys.exit(1)

        index = json.loads(index_path.read_text(encoding="utf-8"))
        files = index.get("files", [])
        logging.info("Index: %d files", len(files))

        s3 = self._pool.get()
        self._setup_bucket(s3)

        if self.cfg.enable_dedup:
            if not self.dedup.load_local():
                self.dedup.download_from_s3(s3, self.cfg.bucket)

        plan   = self._build_plan(files)
        active = [t for t in plan if not t["skip"]]
        logging.info("Tasks: %d active, %d skipped", len(active),
                     len(plan) - len(active))
        with self._lock:
            self.stats["files_skipped"] = len(plan) - len(active)

        if not active:
            logging.info("Nothing to do.")
        else:
            bar = tqdm(total=len(active), unit="file",
                       desc="Migration") if TQDM_AVAILABLE else None

            with ThreadPoolExecutor(max_workers=self.cfg.threads) as executor:
                futures = {executor.submit(self._process_file, t): t for t in active}
                for fut in as_completed(futures):
                    task = futures[fut]
                    try:
                        fut.result()
                        with self._lock:
                            self.stats["files_ok"] += 1
                            self._since_last_save  += 1
                            if self._since_last_save >= self.cfg.dedup_save_interval:
                                self.dedup.save_local()
                                self._since_last_save = 0
                    except Exception as exc:
                        with self._lock:
                            self.stats["files_error"] += 1
                        logging.error("ERROR [%s]: %s",
                                      task.get("relative_path", "?"), exc)
                    finally:
                        if bar:
                            bar.update(1)

            if bar:
                bar.close()

        if self.cfg.enable_dedup:
            self.dedup.save_local()
            self.dedup.upload_to_s3(s3, self.cfg.bucket)

        self._print_summary()

    # ------------------------------------------------------------------
    # Формирование плана
    # ------------------------------------------------------------------

    def _build_plan(self, files: list) -> list:
        return [
            {
                "entry":         f,
                "relative_path": f.get("relative_path", ""),
                "local_path":    self.cfg.export_dir
                                 / f.get("relative_path", "").lstrip("/").replace("/", os.sep),
                "skip":          self.state.is_done(f.get("relative_path", "")),
            }
            for f in files
        ]

    # ------------------------------------------------------------------
    # Обработка одного файла (все версии → один S3-ключ)
    # ------------------------------------------------------------------

    def _process_file(self, task: dict):
        entry    = task["entry"]
        cur_path = task["local_path"]
        file_key = task["relative_path"]          # state key для всего файла

        versions = self._collect_versions(entry, cur_path)
        if not versions:
            raise FileNotFoundError(f"No file found at {cur_path}")

        s3_key = (
            self.cfg.prefix.rstrip("/") + "/"
            + file_key.lstrip("/")
        ).replace("\\", "/")

        for idx, (local, ver_info) in enumerate(versions):
            # Уникальный state key для каждой версии позволяет возобновить
            # с прерванной версии, не перезагружая предыдущие
            ver_state_key = f"{file_key}:v{idx:04d}"
            if self.state.is_done(ver_state_key):
                continue
            self._upload_version(local, entry, ver_info, s3_key, ver_state_key)

        # Помечаем весь файл завершённым
        self.state.mark_done(file_key, s3_key, "")

    def _collect_versions(
        self, entry: dict, current_local: Path
    ) -> list[tuple[Path, dict]]:
        """
        Возвращает список (local_path, ver_info) в хронологическом порядке:
        старые версии первыми, текущая последней.
        """
        result = []
        stem   = current_local.stem
        suffix = current_local.suffix
        vdir   = current_local.parent / "_versions"

        if self.cfg.upload_versions and vdir.is_dir():
            for vf in sorted(vdir.glob(f"{stem}.v*{suffix}")):
                meta = self._read_sidecar(vf)
                result.append((vf, meta.get("version", {})))

        if current_local.exists():
            meta = self._read_sidecar(current_local)
            result.append((current_local, meta.get("version", {})))

        return result

    @staticmethod
    def _read_sidecar(file_path: Path) -> dict:
        sidecar = Path(str(file_path) + ".meta.json")
        if sidecar.exists():
            try:
                return json.loads(sidecar.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    # ------------------------------------------------------------------
    # Загрузка одной версии
    # ------------------------------------------------------------------

    def _upload_version(
        self,
        local_path: Path,
        file_entry: dict,
        ver_info:   dict,
        s3_key:     str,
        state_key:  str,
    ):
        if not local_path.exists():
            logging.warning("Missing file, skip: %s", local_path)
            return

        file_size = local_path.stat().st_size
        logging.debug("Hashing %s (%.0f MB)…", local_path.name, file_size / 1e6)
        sha256    = sha256_of_file(local_path)
        metadata  = build_s3_metadata(file_entry, ver_info, sha256)

        # --- Проверка дедупликации (только между разными файлами) ---
        if self.cfg.enable_dedup:
            existing = self.dedup.lookup(sha256)
            # Дедуп применяем только если канонический ключ ДРУГОЙ.
            # Версии одного файла идут в один S3-ключ → их не дедуплируем,
            # иначе нарушается история версий.
            if existing and existing["key"] != s3_key:
                self.uploader.upload_dedup_ref(
                    self.cfg.bucket, s3_key, sha256,
                    existing["key"], file_size, metadata,
                )
                self.state.mark_done(state_key, s3_key, sha256, is_ref=True)
                with self._lock:
                    self.stats["dedup_refs"]       += 1
                    self.stats["bytes_dedup_saved"] += file_size
                    self.stats["versions_ok"]       += 1
                logging.debug("DEDUP %s → %s", s3_key, existing["key"])
                return

        # --- Обычная загрузка ---
        self.uploader.upload_object(
            local_path, self.cfg.bucket, s3_key,
            metadata, state_key, file_size,
        )

        if self.cfg.enable_dedup:
            self.dedup.register(sha256, s3_key, file_size)

        self.state.mark_done(state_key, s3_key, sha256)
        with self._lock:
            self.stats["bytes_uploaded"] += file_size
            self.stats["versions_ok"]    += 1

        logging.debug("OK  %s  (%.0f MB)", s3_key, file_size / 1e6)

    # ------------------------------------------------------------------
    # Настройка бакета
    # ------------------------------------------------------------------

    def _setup_bucket(self, s3):
        try:
            s3.head_bucket(Bucket=self.cfg.bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                s3.create_bucket(Bucket=self.cfg.bucket)
                logging.info("Bucket '%s' created", self.cfg.bucket)
            else:
                raise

        try:
            s3.put_bucket_versioning(
                Bucket=self.cfg.bucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            logging.info("S3 versioning enabled on '%s'", self.cfg.bucket)
        except ClientError as exc:
            logging.warning("Could not enable bucket versioning: %s", exc)

    # ------------------------------------------------------------------
    # Итоговый отчёт
    # ------------------------------------------------------------------

    def _print_summary(self):
        s          = self.stats
        saved_gb   = s["bytes_dedup_saved"] / 1e9
        upload_gb  = s["bytes_uploaded"]    / 1e9
        total_gb   = saved_gb + upload_gb
        dedup_pct  = (saved_gb / total_gb * 100) if total_gb else 0

        logging.info("=" * 56)
        logging.info("Migration complete")
        logging.info("  Files OK:          %d", s["files_ok"])
        logging.info("  Files skipped:     %d", s["files_skipped"])
        logging.info("  Files errored:     %d", s["files_error"])
        logging.info("  Versions uploaded: %d", s["versions_ok"])
        logging.info("  Dedup references:  %d  (%.2f GB saved)", s["dedup_refs"], saved_gb)
        logging.info("  Bytes uploaded:    %.2f GB", upload_gb)
        logging.info("  Dedup ratio:       %.1f%%", dedup_pct)
        logging.info("=" * 56)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> Config:
    p = argparse.ArgumentParser(
        description="Загрузчик 1С:ДО → S3-совместимое хранилище",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    req = p.add_argument_group("Обязательные")
    req.add_argument("--export-dir",      required=True,
                     help="Каталог выгрузки из 1С:ДО (содержит _export_index.json)")
    req.add_argument("--endpoint-url",    required=True,
                     help="URL хранилища, напр. http://minio:9000")
    req.add_argument("--access-key",      required=True)
    req.add_argument("--secret-key",      required=True)
    req.add_argument("--bucket",          required=True, help="Имя S3-бакета")

    opt = p.add_argument_group("Опциональные")
    opt.add_argument("--prefix",           default="1CDO/",
                     help="Префикс для всех ключей в бакете")
    opt.add_argument("--region",           default="us-east-1",
                     help="Регион (для Ceph укажите 'default')")
    opt.add_argument("--no-ssl-verify",    action="store_true",
                     help="Отключить проверку TLS-сертификата")
    opt.add_argument("--threads",          type=int, default=2,
                     help="Параллельных файлов")
    opt.add_argument("--part-threads",     type=int, default=4,
                     help="Параллельных частей внутри одного multipart")
    opt.add_argument("--multipart-threshold", type=int,
                     default=DEFAULT_MULTIPART_THRESHOLD_MIB,
                     help="Порог multipart в МиБ")
    opt.add_argument("--no-versions",      action="store_true",
                     help="Не загружать исторические версии из _versions/")
    opt.add_argument("--no-dedup",         action="store_true",
                     help="Отключить дедупликацию по SHA-256")
    opt.add_argument("--state-file",       default="s3_upload_state.json")
    opt.add_argument("--dedup-index-file", default="dedup_index.json.gz")
    opt.add_argument("--log-file",         default="s3_upload.log")
    opt.add_argument("--dedup-save-interval", type=int, default=500,
                     help="Сохранять dedup-индекс каждые N файлов")

    a = p.parse_args()
    return Config(
        export_dir          = Path(a.export_dir),
        endpoint_url        = a.endpoint_url,
        access_key          = a.access_key,
        secret_key          = a.secret_key,
        bucket              = a.bucket,
        prefix              = a.prefix,
        region              = a.region,
        verify_ssl          = not a.no_ssl_verify,
        threads             = a.threads,
        part_threads        = a.part_threads,
        multipart_threshold = a.multipart_threshold * 1024 * 1024,
        upload_versions     = not a.no_versions,
        enable_dedup        = not a.no_dedup,
        state_file          = Path(a.state_file),
        dedup_index_file    = Path(a.dedup_index_file),
        log_file            = Path(a.log_file),
        dedup_save_interval = a.dedup_save_interval,
    )


def setup_logging(log_file: Path):
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )


if __name__ == "__main__":
    cfg = parse_args()
    setup_logging(cfg.log_file)
    Migrator(cfg).run()
