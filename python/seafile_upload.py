#!/usr/bin/env python3
"""
seafile_upload.py
=================
Загрузчик результата выгрузки 1С:ДО в Seafile.

Почему Seafile для данного масштаба (500 ГБ / 200 000 файлов / макс 2.5 ГБ):
  • Объектная дедупликация «из коробки»: Seafile разбивает файлы на блоки
    (chunk = 4 МБ), каждый блок хранится по SHA-1 хэшу единожды для всего
    сервера. Два одинаковых файла не занимают дополнительного дискового
    пространства — это фактически object-level dedup через block dedup.
  • Версионирование встроено: каждый PUT одного и того же пути создаёт новую
    запись в истории файла. Seafile хранит все версии.
  • Метаданные версии: Seafile API принимает comment при загрузке; мы
    кодируем в комментарии все метаданные из 1С:ДО (автор, дата, комментарий).
    Дополнительно рядом с каждым файлом загружается .meta.json сайдкар.
  • Размер файла до 2.5 ГБ: стандартный POST через upload-link, без
    сложного multipart. При необходимости сервер настраивается через
    MAX_UPLOAD_SIZE в seahub_settings.py.

СХЕМА ЗАГРУЗКИ ВЕРСИЙ
---------------------
Файл с N историческими версиями (из 1С:ДО export):
  _versions/Договор.v001.docx  ← версия 1 (старейшая)
  _versions/Договор.v002.docx
  Договор.docx                 ← текущая (версия N)

→ В Seafile загружается в один путь, N раз подряд:
  1-й  PUT Договор.docx  с comment "v1/N | author:... | date:... | ..."
  2-й  PUT Договор.docx  с comment "v2/N | ..."
  ...
  N-й  PUT Договор.docx  с comment "vN/N | ..."  ← текущая, видна пользователю

Seafile хранит всю историю; предыдущие версии доступны через «История файла».

ВОЗОБНОВЛЕНИЕ
-------------
Каждая версия файла имеет собственный ключ в state-файле (rel_path:v0000,
rel_path:v0001, …). При перезапуске уже загруженные версии пропускаются —
повторная загрузка версий не создаёт дубликатов в истории.

Требования:
    pip install requests tqdm

Использование:
    python seafile_upload.py \
        --export-dir /mnt/export_1cdo \
        --server https://seafile.company.ru \
        --username admin@company.ru \
        --password secret \
        --library "1С:ДО Архив" \
        --prefix /Миграция \
        --threads 4
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

@dataclass
class Config:
    export_dir:      Path
    server:          str           # https://seafile.company.ru
    username:        str
    password:        str
    library_name:    str           # имя библиотеки (repo); создаётся автоматически
    remote_prefix:   str  = ""    # /Миграция — папка внутри библиотеки
    threads:         int  = 4
    upload_versions: bool = True   # загружать исторические версии
    upload_sidecars: bool = True   # загружать .meta.json рядом с файлом
    request_timeout: int  = 600    # сек; для файлов до 2.5 ГБ нужен большой таймаут
    state_file:      Path = field(default_factory=lambda: Path("seafile_state.json"))
    log_file:        Path = field(default_factory=lambda: Path("seafile_upload.log"))


# Шаблон комментария к версии — несёт все метаданные из 1С:ДО
VERSION_COMMENT = (
    "v{ver_num}/{ver_total} | "
    "1c-author: {ver_author} | "
    "1c-date: {ver_date} | "
    "1c-comment: {comment}"
)


# ---------------------------------------------------------------------------
# Seafile HTTP-клиент
# ---------------------------------------------------------------------------

class SeafileClient:
    """
    Обёртка над Seafile Web API v2.x.

    Создаётся по одному экземпляру на рабочий поток (requests.Session не
    thread-safe). Токен аутентификации получается один раз через главный
    поток и передаётся в каждый экземпляр.

    Кэш созданных директорий общий (передаётся снаружи), чтобы избежать
    лишних API-вызовов при параллельной загрузке.
    """

    def __init__(self, cfg: Config, token: str, dir_cache: set):
        self._cfg       = cfg
        self._token     = token
        self._dir_cache = dir_cache         # разделяется между потоками (set + lock снаружи)
        self._session   = self._make_session()

    @staticmethod
    def _make_session() -> requests.Session:
        s     = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        s.mount("http://",  HTTPAdapter(max_retries=retry))
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def _h(self) -> dict:
        return {"Authorization": f"Token {self._token}"}

    def _url(self, path: str) -> str:
        return self._cfg.server.rstrip("/") + path

    def _get(self, path: str, **kw) -> requests.Response:
        r = self._session.get(self._url(path), headers=self._h(), timeout=30, **kw)
        r.raise_for_status()
        return r

    def _post(self, path: str, timeout: Optional[int] = None, **kw) -> requests.Response:
        t = timeout or self._cfg.request_timeout
        r = self._session.post(self._url(path), headers=self._h(), timeout=t, **kw)
        r.raise_for_status()
        return r

    # ------------------------------------------------------------------
    # Директории
    # ------------------------------------------------------------------

    def mkdir_p(self, repo_id: str, remote_path: str, dir_lock: threading.Lock):
        """Рекурсивно создаёт папку. Пропускает уже известные (dir_cache)."""
        parts = [p for p in remote_path.strip("/").split("/") if p]
        current = ""
        for part in parts:
            current = current + "/" + part
            cache_key = f"{repo_id}:{current}"
            with dir_lock:
                if cache_key in self._dir_cache:
                    continue
            try:
                self._post(
                    f"/api/v2.1/repos/{repo_id}/dir/",
                    timeout=30,
                    data={"operation": "mkdir", "path": current},
                )
            except requests.HTTPError as exc:
                # 400 / 409 = уже существует → не ошибка
                if exc.response.status_code not in (400, 409):
                    raise
            with dir_lock:
                self._dir_cache.add(cache_key)

    # ------------------------------------------------------------------
    # Загрузка файла
    # ------------------------------------------------------------------

    def get_upload_link(self, repo_id: str) -> str:
        """Одноразовая ссылка на fileserver для загрузки."""
        r = self._get(f"/api2/repos/{repo_id}/upload-link/")
        return r.json().strip('"')

    def upload_file(
        self,
        repo_id:    str,
        local_path: Path,
        remote_dir: str,
        filename:   str,
        comment:    str = "",
    ) -> None:
        """
        Загружает файл в Seafile.
        Если файл уже существует — создаёт новую версию (replace=1).

        Таймаут вычисляется от размера файла:
          минимум request_timeout сек; +1 сек на каждые 4 МБ.
        """
        file_size  = local_path.stat().st_size
        timeout    = max(self._cfg.request_timeout, file_size // (4 * 1024 * 1024) + 60)
        upload_url = self.get_upload_link(repo_id)

        with open(local_path, "rb") as fh:
            resp = self._session.post(
                upload_url,
                headers=self._h(),
                data={
                    "parent_dir": remote_dir,
                    "replace":    "1",        # перезапись = создание новой версии
                },
                files={"file": (filename, fh, "application/octet-stream")},
                timeout=timeout,
            )
        resp.raise_for_status()

        # Прикрепляем комментарий с метаданными 1С:ДО
        if comment:
            self._attach_comment(repo_id, remote_dir, filename, comment)

    def _attach_comment(
        self, repo_id: str, remote_dir: str, filename: str, comment: str
    ):
        """
        Добавляет комментарий к файлу через /api2/repos/{repo_id}/file/comments/.
        Комментарий несёт метаданные версии: автор, дата, текст из 1С:ДО.
        Ошибка не является критичной — логируем как предупреждение.
        """
        path = remote_dir.rstrip("/") + "/" + filename
        try:
            self._post(
                f"/api2/repos/{repo_id}/file/comments/",
                timeout=15,
                params={"p": path},
                data={"comment": comment},
            )
        except Exception as exc:
            logging.warning("Comment skipped for %s/%s: %s", remote_dir, filename, exc)


# ---------------------------------------------------------------------------
# Аутентификация (одна на весь запуск)
# ---------------------------------------------------------------------------

def authenticate(cfg: Config) -> str:
    """Возвращает API-токен Seafile."""
    resp = requests.post(
        cfg.server.rstrip("/") + "/api2/auth-token/",
        data={"username": cfg.username, "password": cfg.password},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def get_or_create_repo(cfg: Config, token: str) -> str:
    """Ищет библиотеку по имени; создаёт если не найдена. Возвращает repo_id."""
    headers = {"Authorization": f"Token {token}"}
    repos = requests.get(
        cfg.server.rstrip("/") + "/api2/repos/",
        headers=headers, timeout=30,
    ).json()
    for r in repos:
        if r.get("name") == cfg.library_name:
            logging.info("Library '%s' found: %s", cfg.library_name, r["id"])
            return r["id"]

    # Создаём
    resp = requests.post(
        cfg.server.rstrip("/") + "/api2/repos/",
        headers=headers,
        data={"name": cfg.library_name},
        timeout=30,
    )
    resp.raise_for_status()
    repo_id = resp.json()["repo_id"]
    logging.info("Library '%s' created: %s", cfg.library_name, repo_id)
    return repo_id


# ---------------------------------------------------------------------------
# Состояние загрузки (возобновление)
# ---------------------------------------------------------------------------

class UploadState:

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()
        self._done: set[str] = set()
        self._load()

    def _load(self):
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._done = set(data.get("done", []))
                logging.info("State: %d completed items", len(self._done))
            except Exception as exc:
                logging.warning("State load failed: %s", exc)

    def _flush(self):
        try:
            self._path.write_text(
                json.dumps({"done": sorted(self._done)}, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            pass

    def is_done(self, key: str) -> bool:
        with self._lock:
            return key in self._done

    def mark_done(self, key: str):
        with self._lock:
            self._done.add(key)
            self._flush()


# ---------------------------------------------------------------------------
# Оркестратор
# ---------------------------------------------------------------------------

class Migrator:

    def __init__(self, cfg: Config):
        self.cfg      = cfg
        self.state    = UploadState(cfg.state_file)
        self.stats    = {
            "files_ok":      0,
            "files_skipped": 0,
            "files_error":   0,
            "versions_ok":   0,
        }
        self._stats_lock = threading.Lock()
        self._dir_cache  = set()      # разделяется между потоками
        self._dir_lock   = threading.Lock()
        self._token:   Optional[str] = None
        self._repo_id: Optional[str] = None

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

        # Аутентификация и подготовка библиотеки (однократно)
        self._token   = authenticate(self.cfg)
        self._repo_id = get_or_create_repo(self.cfg, self._token)

        plan   = self._build_plan(files)
        active = [t for t in plan if not t["skip"]]
        logging.info("Tasks: %d active, %d skipped",
                     len(active), len(plan) - len(active))
        with self._stats_lock:
            self.stats["files_skipped"] = len(plan) - len(active)

        if not active:
            logging.info("Nothing to upload.")
            self._print_summary()
            return

        bar = tqdm(total=len(active), unit="file",
                   desc="Upload") if TQDM_AVAILABLE else None

        with ThreadPoolExecutor(max_workers=self.cfg.threads) as executor:
            futures = {
                executor.submit(self._process_file, task): task
                for task in active
            }
            for fut in as_completed(futures):
                task = futures[fut]
                try:
                    fut.result()
                    with self._stats_lock:
                        self.stats["files_ok"] += 1
                except Exception as exc:
                    with self._stats_lock:
                        self.stats["files_error"] += 1
                    logging.error("ERROR [%s]: %s",
                                  task.get("relative_path", "?"), exc)
                finally:
                    if bar:
                        bar.update(1)

        if bar:
            bar.close()

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
    # Обработка одного файла (все версии → один путь в Seafile)
    # ------------------------------------------------------------------

    def _process_file(self, task: dict):
        entry    = task["entry"]
        cur_path = task["local_path"]
        rel_path = task["relative_path"]

        # Каждый поток создаёт свой SeafileClient (session не thread-safe)
        client = SeafileClient(self.cfg, self._token, self._dir_cache)

        versions = self._collect_versions(entry, cur_path)
        if not versions:
            raise FileNotFoundError(f"No file at {cur_path}")

        # Строим путь в Seafile
        norm    = rel_path.lstrip("/").replace("\\", "/")
        full    = (self.cfg.remote_prefix.rstrip("/") + "/" + norm).lstrip("/")
        parts   = full.rsplit("/", 1)
        remote_dir = ("/" + parts[0]) if len(parts) > 1 else "/"
        filename   = parts[-1]

        client.mkdir_p(self._repo_id, remote_dir, self._dir_lock)

        for idx, (ver_local, ver_info) in enumerate(versions):
            ver_key = f"{rel_path}:v{idx:04d}"
            if self.state.is_done(ver_key):
                continue

            comment = VERSION_COMMENT.format(
                ver_num    = ver_info.get("number",         idx + 1),
                ver_total  = ver_info.get("total",          len(versions)),
                ver_author = ver_info.get("version_author", entry.get("author", "")),
                ver_date   = ver_info.get("version_date",   ""),
                comment    = ver_info.get("comment",        ""),
            )

            client.upload_file(
                self._repo_id, ver_local, remote_dir, filename, comment
            )
            self.state.mark_done(ver_key)
            with self._stats_lock:
                self.stats["versions_ok"] += 1
            logging.debug("OK  v%d/%d  %s", idx + 1, len(versions), full)

        # Сайдкар .meta.json загружаем один раз — после последней версии
        if self.cfg.upload_sidecars:
            self._upload_sidecar(client, cur_path, remote_dir, filename)

        self.state.mark_done(rel_path)

    def _upload_sidecar(
        self,
        client:     SeafileClient,
        cur_path:   Path,
        remote_dir: str,
        filename:   str,
    ):
        sidecar = Path(str(cur_path) + ".meta.json")
        if not sidecar.exists():
            return
        sidecar_key = f"sidecar:{remote_dir}/{filename}.meta.json"
        if self.state.is_done(sidecar_key):
            return
        try:
            client.upload_file(
                self._repo_id, sidecar, remote_dir,
                filename + ".meta.json",
                comment="metadata sidecar (1C:DO)",
            )
            self.state.mark_done(sidecar_key)
        except Exception as exc:
            logging.warning("Sidecar upload failed for %s: %s", filename, exc)

    # ------------------------------------------------------------------
    # Сбор версий
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_versions(entry: dict, current_local: Path) -> list[tuple[Path, dict]]:
        """
        Возвращает (local_path, ver_info) в хронологическом порядке.
        Исторические версии из _versions/ — первыми, текущая — последней.
        """
        result = []
        stem   = current_local.stem
        suffix = current_local.suffix
        vdir   = current_local.parent / "_versions"

        if vdir.is_dir():
            for vf in sorted(vdir.glob(f"{stem}.v*{suffix}")):
                meta = Migrator._read_sidecar(vf)
                result.append((vf, meta.get("version", {})))

        if current_local.exists():
            meta = Migrator._read_sidecar(current_local)
            result.append((current_local, meta.get("version", {})))

        return result

    @staticmethod
    def _read_sidecar(path: Path) -> dict:
        s = Path(str(path) + ".meta.json")
        if s.exists():
            try:
                return json.loads(s.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    # ------------------------------------------------------------------
    # Итоги
    # ------------------------------------------------------------------

    def _print_summary(self):
        s = self.stats
        logging.info("=" * 55)
        logging.info("Migration complete")
        logging.info("  Files OK:          %d", s["files_ok"])
        logging.info("  Files skipped:     %d", s["files_skipped"])
        logging.info("  Files errored:     %d", s["files_error"])
        logging.info("  Versions uploaded: %d", s["versions_ok"])
        logging.info("=" * 55)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> Config:
    p = argparse.ArgumentParser(
        description="Загрузчик 1С:ДО → Seafile",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    req = p.add_argument_group("Обязательные")
    req.add_argument("--export-dir",  required=True,
                     help="Каталог выгрузки 1С:ДО (содержит _export_index.json)")
    req.add_argument("--server",      required=True,
                     help="URL Seafile, напр. https://seafile.company.ru")
    req.add_argument("--username",    required=True, help="Email пользователя Seafile")
    req.add_argument("--password",    required=True)
    req.add_argument("--library",     required=True, dest="library_name",
                     help="Имя библиотеки в Seafile (создаётся если нет)")

    opt = p.add_argument_group("Опциональные")
    opt.add_argument("--prefix",       default="",  dest="remote_prefix",
                     help="Папка внутри библиотеки, напр. /Миграция/2024")
    opt.add_argument("--threads",      type=int, default=4,
                     help="Параллельных потоков загрузки")
    opt.add_argument("--no-versions",  action="store_true",
                     help="Не загружать исторические версии из _versions/")
    opt.add_argument("--no-sidecars",  action="store_true",
                     help="Не загружать .meta.json сайдкары")
    opt.add_argument("--timeout",      type=int, default=600,
                     help="Таймаут HTTP-запроса в сек (600 достаточно для файлов до 2.5 ГБ)")
    opt.add_argument("--state-file",   default="seafile_state.json")
    opt.add_argument("--log-file",     default="seafile_upload.log")

    a = p.parse_args()
    return Config(
        export_dir      = Path(a.export_dir),
        server          = a.server,
        username        = a.username,
        password        = a.password,
        library_name    = a.library_name,
        remote_prefix   = a.remote_prefix,
        threads         = a.threads,
        upload_versions = not a.no_versions,
        upload_sidecars = not a.no_sidecars,
        request_timeout = a.timeout,
        state_file      = Path(a.state_file),
        log_file        = Path(a.log_file),
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
