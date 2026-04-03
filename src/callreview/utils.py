from __future__ import annotations

import hashlib
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional


AUDIO_EXTENSIONS = {".wav", ".mp3", ".gsm", ".ogg", ".m4a", ".mp4"}


def is_audio_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS


def file_is_stable(path: Path, stable_seconds: int) -> bool:
    if not path.exists() or not path.is_file():
        return False
    age = time.time() - path.stat().st_mtime
    return age >= stable_seconds


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def safe_move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))


def build_archive_path(
    *,
    archive_root: Path,
    recorded_dt: Optional[datetime],
    fallback_mtime: float,
    filename: str,
) -> Path:
    dt = recorded_dt or datetime.fromtimestamp(fallback_mtime)
    return archive_root / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}" / filename


def parse_datetime_from_path_parts(path: Path) -> Optional[datetime]:
    parts = [p for p in path.parts if p.isdigit()]
    if len(parts) >= 3:
        try:
            year = int(parts[-3])
            month = int(parts[-2])
            day = int(parts[-1])
            return datetime(year, month, day)
        except ValueError:
            return None
    return None