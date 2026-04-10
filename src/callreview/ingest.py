from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import time as time_module
from pathlib import Path
from typing import Iterator, Optional
import re

from callreview.config import settings
from callreview.db import get_call_by_current_path, insert_call, update_call_paths, update_call_status
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
        if is_under(path, settings.vip_source_dir):
            continue
        if is_playback_file(path):
            continue
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
        mtime_dt = datetime.fromtimestamp(stat.st_mtime)

        filename_dt = parse_vip_filename_datetime(path.name)
        path_dt = parse_datetime_from_path_parts(path.parent)

        if filename_dt is not None:
            recorded_at = filename_dt
        elif path_dt is not None:
            recorded_at = path_dt.replace(
                hour=mtime_dt.hour,
                minute=mtime_dt.minute,
                second=mtime_dt.second,
                microsecond=0,
            )
        else:
            recorded_at = mtime_dt.replace(microsecond=0)

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


def resolve_moved_call_path(current_path: Path, archive_root: Path) -> Path | None:
    """
    Try to locate a moved call file by filename under the archive tree.
    Intended as a recovery path for stale current_path values.
    """
    if current_path.exists() and current_path.is_file():
        return current_path

    if not archive_root.exists() or not archive_root.is_dir():
        return None

    matches = list(archive_root.rglob(current_path.name))
    if not matches:
        return None

    # Prefer the newest match if more than one somehow exists.
    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0]


def queue_stable_new_calls() -> int:
    from callreview.db import get_conn
    from callreview.logging_utils import setup_logging

    logger = setup_logging(
        name="callreview.queue",
        log_dir=settings.log_dir,
        log_filename=settings.log_file,
        level=settings.log_level,
    )

    changed = 0

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, system, current_path, archive_path
            FROM calls
            WHERE status = 'new'
            ORDER BY discovered_at ASC
            """
        ).fetchall()

    for row in rows:
        call_id = row["id"]
        system = row["system"]
        current_path = Path(row["current_path"]) if row["current_path"] else None
        archive_path = Path(row["archive_path"]) if row["archive_path"] else None

        resolved_path = None

        if current_path and current_path.exists() and current_path.is_file():
            resolved_path = current_path
        elif archive_path and archive_path.exists() and archive_path.is_file():
            resolved_path = archive_path
        elif system == "cx" and current_path:
            resolved_path = resolve_moved_call_path(current_path, settings.archive_cx_dir)
            if resolved_path:
                update_call_paths(
                    call_id,
                    current_path=str(resolved_path),
                    archive_path=str(resolved_path),
                )
                logger.info(
                    "recovered moved cx file id=%s old=%s new=%s",
                    call_id,
                    current_path,
                    resolved_path,
                )

        if resolved_path is None:
            logger.warning(
                "new row stranded id=%s missing current_path=%s archive_path=%s",
                call_id,
                current_path,
                archive_path,
            )
            continue

        age = time_module.time() - resolved_path.stat().st_mtime
        if age >= settings.file_stable_seconds:
            update_call_status(call_id, status="queued")
            changed += 1
            logger.info("queued id=%s path=%s", call_id, resolved_path)
        else:
            logger.info(
                "still new id=%s age=%.1fs required=%ss path=%s",
                call_id,
                age,
                settings.file_stable_seconds,
                resolved_path,
            )

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


def is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False
    
    
def is_playback_file(path: Path) -> bool:
    return path.name.lower().endswith(".playback.mp3")