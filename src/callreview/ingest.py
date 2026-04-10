from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
import re

from callreview.config import settings
from callreview.db import get_conn, upsert_call_discovery, update_call_status
from callreview.utils import (
    build_archive_path,
    file_is_stable,
    is_audio_file,
    parse_datetime_from_path_parts,
    safe_move,
)


@dataclass
class DiscoveredFile:
    system: str
    path: Path
    source_path: Path
    canonical_path: Path
    recorded_at: Optional[datetime]
    file_size: int
    modified_ts: float


def walk_audio_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if is_audio_file(path):
            yield path


def parse_vip_filename_datetime(filename: str) -> Optional[datetime]:
    match = re.search(r"aud-(\d{14})", filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        except ValueError:
            return None
    return None


def discover_cx_files() -> list[DiscoveredFile]:
    items: list[DiscoveredFile] = []
    for path in walk_audio_files(settings.cx_source_dir):
        if path.name.lower().endswith(".playback.mp3"):
            continue
        if not file_is_stable(path, settings.file_stable_seconds):
            continue

        stat = path.stat()
        recorded_at = datetime.fromtimestamp(stat.st_mtime)
        canonical_path = build_archive_path(
            archive_root=settings.archive_cx_dir,
            recorded_dt=recorded_at,
            fallback_mtime=stat.st_mtime,
            filename=path.name,
        )

        items.append(
            DiscoveredFile(
                system="cx",
                path=path,
                source_path=path,
                canonical_path=canonical_path,
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
                canonical_path=path,
                recorded_at=recorded_at,
                file_size=stat.st_size,
                modified_ts=stat.st_mtime,
            )
        )
    return items


def _move_cx_to_archive(item: DiscoveredFile) -> tuple[Path, int, float]:
    canonical_path = item.canonical_path

    if item.path.resolve() != canonical_path.resolve():
        if not settings.dry_run:
            safe_move(item.path, canonical_path)
        else:
            canonical_path.parent.mkdir(parents=True, exist_ok=True)

    stat_path = item.path if settings.dry_run else canonical_path
    stat = stat_path.stat()
    return canonical_path, stat.st_size, stat.st_mtime


def register_discoveries() -> int:
    discovered = 0

    for item in discover_cx_files():
        canonical_path, file_size, modified_ts = _move_cx_to_archive(item)
        call_time = datetime.fromtimestamp(modified_ts).isoformat()

        _call_id, inserted = upsert_call_discovery(
            system="cx",
            filename=canonical_path.name,
            source_path=str(item.source_path),
            current_path=str(canonical_path),
            archive_path=str(canonical_path),
            file_size=file_size,
            modified_ts=modified_ts,
            recorded_at=None,
            call_time=call_time,
            status="queued",
        )
        if inserted:
            discovered += 1

    for item in discover_vipvoice_files():
        call_time = item.recorded_at or datetime.fromtimestamp(item.modified_ts)
        _call_id, inserted = upsert_call_discovery(
            system="vipvoice",
            filename=item.path.name,
            source_path=str(item.source_path),
            current_path=str(item.canonical_path),
            archive_path=str(item.canonical_path),
            file_size=item.file_size,
            modified_ts=item.modified_ts,
            recorded_at=item.recorded_at.isoformat() if item.recorded_at else None,
            call_time=call_time.isoformat() if call_time else None,
            status="queued",
        )
        if inserted:
            discovered += 1

    return discovered


def queue_stable_new_calls() -> int:
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
        if path.exists() and path.is_file():
            update_call_status(row["id"], status="queued")
            changed += 1

    return changed
