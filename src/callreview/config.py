from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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


def _env_path(name: str, default: str) -> Path:
    raw = os.getenv(name, default)
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


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
    worker_stale_processing_minutes: int
    web_host: str
    web_port: int
    dry_run: bool
    transcription_model: str
    transcription_device: str
    transcription_compute_type: str


def load_settings() -> Settings:
    return Settings(
        db_path=_env_path("CALLREVIEW_DB", "./data/callreview.db"),
        cx_source_dir=_env_path("CX_SOURCE_DIR", "./sample_data/cx_incoming"),
        vip_source_dir=_env_path("VIP_SOURCE_DIR", "./sample_data/vipvoice_incoming"),
        archive_cx_dir=_env_path("ARCHIVE_CX_DIR", "./sample_data/archive/WIOGEN-CX"),
        archive_vip_dir=_env_path("ARCHIVE_VIP_DIR", "./sample_data/archive/WIOGEN-TS"),
        file_stable_seconds=_env_int("FILE_STABLE_SECONDS", 30),
        worker_scan_interval=_env_int("WORKER_SCAN_INTERVAL", 15),
        worker_backlog_every=_env_int("WORKER_BACKLOG_EVERY", 5),
        worker_stale_processing_minutes=_env_int("WORKER_STALE_PROCESSING_MINUTES", 20),
        web_host=os.getenv("WEB_HOST", "127.0.0.1"),
        web_port=_env_int("WEB_PORT", 5000),
        dry_run=_env_bool("DRY_RUN", True),
        transcription_model=os.getenv("TRANSCRIPTION_MODEL", "small"),
        transcription_device=os.getenv("TRANSCRIPTION_DEVICE", "cpu"),
        transcription_compute_type=os.getenv("TRANSCRIPTION_COMPUTE_TYPE", "int8"),
    )


settings = load_settings()