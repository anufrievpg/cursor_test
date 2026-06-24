#!/usr/bin/env python3
"""
upload_to_nextcloud.py
======================
Загрузчик файлов 1С:ДО → Nextcloud с поддержкой:
  - версионирования (каждая версия отдельно, затем финальная перезаписывается)
  - метаданных (теги Nextcloud + комментарии)
  - возобновления (пропускает уже загруженные файлы)
  - параллельной загрузки (thread pool)
  - подробного журнала

Требования:
    pip install requests tqdm

Использование:
    python upload_to_nextcloud.py \
        --export-dir /path/to/1cdo_export \
        --nextcloud-url https://cloud.example.com \
        --username admin \
        --password secret \
        --remote-dir /1CDO_Migration \
        --upload-versions \
        --threads 4
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin

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
    export_dir: Path
    nextcloud_url: str
    username: str
    password: str
    remote_dir: str = "/1CDO_Migration"
    upload_versions: bool = True
    upload_sidecar_json: bool = True
    add_tags: bool = True
    add_comments: bool = True
    threads: int = 2
    retry_count: int = 3
    chunk_size: int = 4 * 1024 * 1024      # 4 МБ — размер чанка для больших файлов
    state_file: Path = field(default_factory=lambda: Path("upload_state.json"))
    log_file: Path = field(default_factory=lambda: Path("upload.log"))

    def webdav_base(self) -> str:
        url = self.nextcloud_url.rstrip("/")
        return f"{url}/remote.php/dav/files/{quote(self.username)}"

    def ocs_base(self) -> str:
        return self.nextcloud_url.rstrip("/") + "/ocs/v2.php"


# ---------------------------------------------------------------------------
# HTTP-клиент с автоматическими повторами
# ---------------------------------------------------------------------------

def make_session(config: Config) -> requests.Session:
    session = requests.Session()
    session.auth = (config.username, config.password)
    session.headers.update({"OCS-APIREQUEST": "true"})

    retry = Retry(
        total=config.retry_count,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "MKCOL", "PROPFIND", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# WebDAV-операции
# ---------------------------------------------------------------------------

class WebDavClient:
    def __init__(self, config: Config, session: requests.Session):
        self.config = config
        self.session = session
        self._created_dirs: set[str] = set()

    def _url(self, remote_path: str) -> str:
        path = remote_path.lstrip("/")
        return self.config.webdav_base() + "/" + "/".join(
            quote(segment, safe="") for segment in path.split("/") if segment
        )

    def mkdir_p(self, remote_dir: str):
        """Создаёт папку рекурсивно (MKCOL), пропускает уже созданные."""
        parts = [p for p in remote_dir.strip("/").split("/") if p]
        current = ""
        for part in parts:
            current = current + "/" + part
            if current in self._created_dirs:
                continue
            url = self._url(current)
            resp = self.session.request("MKCOL", url, timeout=30)
            if resp.status_code in (201, 301, 405):  # 405 = Already Exists
                self._created_dirs.add(current)
            else:
                raise RuntimeError(f"MKCOL {current} → HTTP {resp.status_code}: {resp.text[:200]}")

    def exists(self, remote_path: str) -> bool:
        url = self._url(remote_path)
        resp = self.session.request("HEAD", url, timeout=15)
        return resp.status_code == 200

    def upload(self, local_path: Path, remote_path: str) -> None:
        """Загружает файл через PUT. Для больших файлов использует chunked upload."""
        url = self._url(remote_path)
        file_size = local_path.stat().st_size

        with open(local_path, "rb") as fh:
            resp = self.session.put(
                url,
                data=fh,
                headers={"Content-Length": str(file_size)},
                timeout=max(120, file_size // (100 * 1024)),  # минимум 2 мин, +1 сек на каждые 100 КБ
            )

        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"PUT {remote_path} → HTTP {resp.status_code}: {resp.text[:200]}")

    def get_file_id(self, remote_path: str) -> Optional[str]:
        """Получает внутренний ID файла в Nextcloud через PROPFIND."""
        url = self._url(remote_path)
        body = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop>
    <oc:fileid/>
  </d:prop>
</d:propfind>"""
        resp = self.session.request(
            "PROPFIND", url,
            data=body,
            headers={"Depth": "0", "Content-Type": "application/xml"},
            timeout=15,
        )
        if resp.status_code != 207:
            return None
        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)
        ns = {"oc": "http://owncloud.org/ns"}
        fileid = root.find(".//oc:fileid", ns)
        return fileid.text if fileid is not None else None


# ---------------------------------------------------------------------------
# Nextcloud OCS API (теги и комментарии)
# ---------------------------------------------------------------------------

class NextcloudOCS:
    def __init__(self, config: Config, session: requests.Session):
        self.config = config
        self.session = session
        self._tag_cache: dict[str, str] = {}

    def _ensure_tag(self, tag_name: str) -> Optional[str]:
        """Создаёт тег если не существует, возвращает его ID."""
        if tag_name in self._tag_cache:
            return self._tag_cache[tag_name]

        # Получаем существующие теги
        url = self.config.nextcloud_url.rstrip("/") + "/remote.php/dav/systemtags"
        resp = self.session.request(
            "PROPFIND", url,
            headers={"Depth": "1", "Content-Type": "application/xml"},
            data="""<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop><oc:display-name/><oc:id/></d:prop>
</d:propfind>""",
            timeout=15,
        )

        if resp.status_code == 207:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {"oc": "http://owncloud.org/ns", "d": "DAV:"}
            for response in root.findall("d:response", ns):
                name_el = response.find(".//oc:display-name", ns)
                id_el   = response.find(".//oc:id", ns)
                if name_el is not None and id_el is not None:
                    self._tag_cache[name_el.text] = id_el.text

        if tag_name in self._tag_cache:
            return self._tag_cache[tag_name]

        # Создаём новый тег
        create_url = self.config.nextcloud_url.rstrip("/") + "/remote.php/dav/systemtags"
        resp = self.session.post(
            create_url,
            json={"name": tag_name, "userVisible": True, "userAssignable": True},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code in (200, 201):
            location = resp.headers.get("Content-Location", "")
            tag_id = location.rstrip("/").split("/")[-1]
            self._tag_cache[tag_name] = tag_id
            return tag_id

        logging.warning("Не удалось создать тег '%s': HTTP %d", tag_name, resp.status_code)
        return None

    def assign_tag(self, file_id: str, tag_name: str) -> bool:
        tag_id = self._ensure_tag(tag_name)
        if not tag_id:
            return False
        url = (
            self.config.nextcloud_url.rstrip("/")
            + f"/remote.php/dav/systemtags-relations/files/{file_id}/{tag_id}"
        )
        resp = self.session.put(url, timeout=15)
        return resp.status_code in (200, 201, 409)

    def add_comment(self, file_id: str, message: str) -> bool:
        url = (
            self.config.nextcloud_url.rstrip("/")
            + f"/remote.php/dav/comments/files/{file_id}"
        )
        resp = self.session.post(
            url,
            json={"actorType": "users", "verb": "comment", "message": message},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        return resp.status_code in (200, 201)


# ---------------------------------------------------------------------------
# Состояние загрузки (для возобновления)
# ---------------------------------------------------------------------------

class UploadState:
    def __init__(self, state_file: Path):
        self.path = state_file
        self._uploaded: set[str] = set()
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self._uploaded = set(data.get("uploaded", []))
                logging.info("Возобновление: найдено %d уже загруженных файлов", len(self._uploaded))
            except Exception as exc:
                logging.warning("Не удалось прочитать файл состояния: %s", exc)

    def mark_done(self, key: str):
        self._uploaded.add(key)
        try:
            self.path.write_text(
                json.dumps({"uploaded": sorted(self._uploaded)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass

    def is_done(self, key: str) -> bool:
        return key in self._uploaded


# ---------------------------------------------------------------------------
# Основной класс миграции
# ---------------------------------------------------------------------------

class Migrator:
    def __init__(self, config: Config):
        self.config = config
        self.session = make_session(config)
        self.dav = WebDavClient(config, self.session)
        self.ocs = NextcloudOCS(config, self.session)
        self.state = UploadState(config.state_file)
        self.stats = {"ok": 0, "skipped": 0, "error": 0, "versions": 0}

    # ------------------------------------------------------------------
    # Точка входа
    # ------------------------------------------------------------------
    def run(self):
        export_dir = self.config.export_dir
        if not export_dir.is_dir():
            logging.error("Каталог выгрузки не найден: %s", export_dir)
            sys.exit(1)

        index_path = export_dir / "_export_index.json"
        if not index_path.exists():
            logging.error("Файл _export_index.json не найден. Проверьте путь к каталогу выгрузки.")
            sys.exit(1)

        index = json.loads(index_path.read_text(encoding="utf-8"))
        files = index.get("files", [])
        logging.info("Индекс загружен: %d файлов", len(files))

        # Создаём корневую папку в Nextcloud
        self.dav.mkdir_p(self.config.remote_dir)

        tasks = self._build_task_list(files, export_dir)
        logging.info("Задач на загрузку: %d (пропущено уже загруженных: %d)",
                     sum(1 for t in tasks if not t["skip"]),
                     sum(1 for t in tasks if t["skip"]))

        active_tasks = [t for t in tasks if not t["skip"]]

        if not active_tasks:
            logging.info("Все файлы уже загружены. Нечего делать.")
            return

        # Параллельная загрузка
        bar = tqdm(total=len(active_tasks), unit="file") if TQDM_AVAILABLE else None

        with ThreadPoolExecutor(max_workers=self.config.threads) as pool:
            futures = {pool.submit(self._upload_task, t): t for t in active_tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                    self.stats["ok"] += 1
                    self.state.mark_done(task["state_key"])
                except Exception as exc:
                    self.stats["error"] += 1
                    logging.error("ОШИБКА [%s]: %s", task["local_path"], exc)
                finally:
                    if bar:
                        bar.update(1)

        if bar:
            bar.close()

        self._print_summary()

    # ------------------------------------------------------------------
    # Формируем плоский список задач из индекса
    # ------------------------------------------------------------------
    def _build_task_list(self, files: list, export_dir: Path) -> list:
        tasks = []
        for file_entry in files:
            rel_path = file_entry.get("relative_path", "")
            local_path = export_dir / rel_path.lstrip("/").replace("/", os.sep)
            remote_path = self.config.remote_dir.rstrip("/") + "/" + rel_path.lstrip("/")

            state_key = rel_path
            tasks.append({
                "local_path":  local_path,
                "remote_path": remote_path,
                "meta":        file_entry,
                "state_key":   state_key,
                "skip":        self.state.is_done(state_key),
                "is_version":  False,
            })

            # Добавляем версии если нужно
            if self.config.upload_versions and file_entry.get("versions_count", 1) > 1:
                versions_dir_local  = local_path.parent / "_versions"
                versions_dir_remote = str(Path(remote_path).parent / "_versions").replace("\\", "/")

                if versions_dir_local.is_dir():
                    stem = local_path.stem
                    suffix = local_path.suffix
                    for ver_file in sorted(versions_dir_local.glob(f"{stem}.v*{suffix}")):
                        ver_rel = str(ver_file.relative_to(export_dir)).replace("\\", "/")
                        ver_remote = self.config.remote_dir.rstrip("/") + "/" + ver_rel
                        ver_key = ver_rel
                        tasks.append({
                            "local_path":  ver_file,
                            "remote_path": ver_remote,
                            "meta":        file_entry,
                            "state_key":   ver_key,
                            "skip":        self.state.is_done(ver_key),
                            "is_version":  True,
                        })

        return tasks

    # ------------------------------------------------------------------
    # Загрузка одного файла (запускается в пуле потоков)
    # ------------------------------------------------------------------
    def _upload_task(self, task: dict):
        local_path: Path = task["local_path"]
        remote_path: str = task["remote_path"]
        meta: dict = task["meta"]

        if not local_path.exists():
            raise FileNotFoundError(f"Файл не найден: {local_path}")

        # Создаём папку на сервере
        remote_dir = "/".join(remote_path.split("/")[:-1])
        self.dav.mkdir_p(remote_dir)

        # Загружаем файл
        self.dav.upload(local_path, remote_path)

        if not task["is_version"]:
            self.stats["versions"] += 1

            # JSON-sidecar (метаданные рядом с файлом)
            if self.config.upload_sidecar_json:
                sidecar_local = Path(str(local_path) + ".meta.json")
                if sidecar_local.exists():
                    sidecar_remote = remote_path + ".meta.json"
                    self.dav.upload(sidecar_local, sidecar_remote)

            # Теги и комментарии через OCS API
            if self.config.add_tags or self.config.add_comments:
                file_id = self.dav.get_file_id(remote_path)
                if file_id:
                    self._apply_metadata(file_id, meta)

        logging.info("OK  %s", remote_path)

    # ------------------------------------------------------------------
    # Применяем метаданные через Nextcloud OCS
    # ------------------------------------------------------------------
    def _apply_metadata(self, file_id: str, meta: dict):
        if self.config.add_tags:
            # Тег с именем автора
            author = meta.get("author", "").strip()
            if author:
                self.ocs.assign_tag(file_id, f"author:{author}")

            # Тег с именем папки 1С:ДО (первый уровень)
            folder = meta.get("folder", "").strip()
            if folder:
                folder_tag = folder.split("/")[0] if "/" in folder else folder
                if folder_tag:
                    self.ocs.assign_tag(file_id, f"1cdo:{folder_tag}")

            # Помеченные на удаление
            if meta.get("is_deleted"):
                self.ocs.assign_tag(file_id, "1cdo:deleted")

            # Тег о количестве версий
            versions_count = meta.get("versions_count", 1)
            if versions_count > 1:
                self.ocs.assign_tag(file_id, f"versions:{versions_count}")

        if self.config.add_comments:
            comment_parts = [
                f"Импортировано из 1С:ДО",
                f"Оригинальное имя: {meta.get('name', '')}",
                f"Автор: {meta.get('author', '')}",
                f"Создан: {meta.get('created', '')}",
                f"Изменён: {meta.get('modified', '')}",
                f"Версий в 1С:ДО: {meta.get('versions_count', 1)}",
            ]
            if meta.get("is_deleted"):
                comment_parts.append("⚠ Помечен на удаление в 1С:ДО")

            self.ocs.add_comment(file_id, "\n".join(comment_parts))

    # ------------------------------------------------------------------
    def _print_summary(self):
        logging.info("=" * 50)
        logging.info("Загрузка завершена")
        logging.info("  Успешно:  %d", self.stats["ok"])
        logging.info("  Пропущено: %d", self.stats["skipped"])
        logging.info("  Ошибок:   %d", self.stats["error"])
        logging.info("=" * 50)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Загрузчик файлов 1С:ДО → Nextcloud",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--export-dir",     required=True,  help="Локальный каталог выгрузки из 1С:ДО")
    parser.add_argument("--nextcloud-url",  required=True,  help="URL Nextcloud (https://cloud.example.com)")
    parser.add_argument("--username",       required=True,  help="Логин Nextcloud")
    parser.add_argument("--password",       required=True,  help="Пароль Nextcloud")
    parser.add_argument("--remote-dir",     default="/1CDO_Migration", help="Целевая папка в Nextcloud")
    parser.add_argument("--upload-versions", action="store_true", default=True,
                        help="Загружать все версии в _versions/")
    parser.add_argument("--no-versions",    action="store_true", help="Не загружать версии")
    parser.add_argument("--no-tags",        action="store_true", help="Не назначать теги")
    parser.add_argument("--no-comments",    action="store_true", help="Не добавлять комментарии")
    parser.add_argument("--no-sidecar",     action="store_true", help="Не загружать .meta.json файлы")
    parser.add_argument("--threads",        type=int, default=2, help="Число потоков загрузки")
    parser.add_argument("--state-file",     default="upload_state.json", help="Файл состояния (для возобновления)")
    parser.add_argument("--log-file",       default="upload.log", help="Файл лога")

    args = parser.parse_args()

    return Config(
        export_dir=Path(args.export_dir),
        nextcloud_url=args.nextcloud_url,
        username=args.username,
        password=args.password,
        remote_dir=args.remote_dir,
        upload_versions=not args.no_versions,
        upload_sidecar_json=not args.no_sidecar,
        add_tags=not args.no_tags,
        add_comments=not args.no_comments,
        threads=args.threads,
        state_file=Path(args.state_file),
        log_file=Path(args.log_file),
    )


def setup_logging(log_file: Path):
    handlers = [logging.StreamHandler(sys.stdout)]
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
    config = parse_args()
    setup_logging(config.log_file)
    migrator = Migrator(config)
    migrator.run()
