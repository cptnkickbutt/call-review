from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    db_path: Path
    cx_source_dir: Path
    vip_source_dir: Path
    archive_cx_dir: Path
    archive_vip_dir: Path
    file_stable_seconds: int
    worker_scan_interval: int
    worker_backlog_every: int
    web_host: str
    web_port: int
    dry_run: bool


def load_settings() -> Settings:
    return Settings(
        db_path=Path(os.getenv("CALLREVIEW_DB", "./data/callreview.db")),
        cx_source_dir=Path(os.getenv("CX_SOURCE_DIR", "./sample_data/cx_incoming")),
        vip_source_dir=Path(os.getenv("VIP_SOURCE_DIR", "./sample_data/vipvoice_incoming")),
        archive_cx_dir=Path(os.getenv("ARCHIVE_CX_DIR", "./sample_data/archive/WIOGEN-CX")),
        archive_vip_dir=Path(os.getenv("ARCHIVE_VIP_DIR", "./sample_data/archive/WIOGEN-TS")),
        file_stable_seconds=_env_int("FILE_STABLE_SECONDS", 30),
        worker_scan_interval=_env_int("WORKER_SCAN_INTERVAL", 15),
        worker_backlog_every=_env_int("WORKER_BACKLOG_EVERY", 5),
        web_host=os.getenv("WEB_HOST", "127.0.0.1"),
        web_port=_env_int("WEB_PORT", 5000),
        dry_run=_env_bool("DRY_RUN", True),
    )


settings = load_settings()