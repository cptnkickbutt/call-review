from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
import re

from callreview.config import settings
from callreview.db import get_call_by_current_path, insert_call, update_call_status
from callreview.utils import file_is_stable, is_audio_file, parse_datetime_from_path_parts


@dataclass
class DiscoveredFile:
    system: str
    path: Path
    source_path: Path
    recorded_at: Optional[datetime]
    file_size: int
    modified_ts: float


def walk_audio_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if is_audio_file(path):
            yield path


def discover_cx_files() -> list[DiscoveredFile]:
    items: list[DiscoveredFile] = []
    for path in walk_audio_files(settings.cx_source_dir):
        stat = path.stat()
        items.append(
            DiscoveredFile(
                system="cx",
                path=path,
                source_path=path,
                recorded_at=None,
                file_size=stat.st_size,
                modified_ts=stat.st_mtime,
            )
        )
    return items


def discover_vipvoice_files() -> list[DiscoveredFile]:
    items: list[DiscoveredFile] = []
    for path in walk_audio_files(settings.vip_source_dir):
        if path.name.lower().endswith(".playback.mp3"):
            continue
        stat = path.stat()
        filename_dt = parse_vip_filename_datetime(path.name)
        recorded_at = filename_dt or parse_datetime_from_path_parts(path.parent)
        items.append(
            DiscoveredFile(
                system="vipvoice",
                path=path,
                source_path=path,
                recorded_at=recorded_at,
                file_size=stat.st_size,
                modified_ts=stat.st_mtime,
            )
        )
    return items


def register_discoveries() -> int:
    discovered = discover_cx_files() + discover_vipvoice_files()
    inserted = 0

    for item in discovered:
        existing = get_call_by_current_path(str(item.path))
        if existing:
            continue

        call_time = item.recorded_at or datetime.fromtimestamp(item.modified_ts)

        insert_call(
            system=item.system,
            filename=item.path.name,
            source_path=str(item.source_path),
            current_path=str(item.path),
            file_size=item.file_size,
            modified_ts=item.modified_ts,
            recorded_at=item.recorded_at.isoformat() if item.recorded_at else None,
            call_time=call_time.isoformat() if call_time else None,
        )
        inserted += 1

    return inserted


def queue_stable_new_calls() -> int:
    from callreview.db import get_conn

    changed = 0
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, current_path
            FROM calls
            WHERE status = 'new'
            ORDER BY discovered_at ASC
            """
        ).fetchall()

    for row in rows:
        path = Path(row["current_path"])
        if file_is_stable(path, settings.file_stable_seconds):
            update_call_status(row["id"], status="queued")
            changed += 1

    return changed

def parse_vip_filename_datetime(filename: str) -> Optional[datetime]:
    # Example: aud-20260403134944056950-xxxx.wav
    match = re.search(r"aud-(\d{14})", filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        except ValueError:
            return None
    return None