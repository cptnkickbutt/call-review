from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
import fcntl
import re

from callreview.config import settings
from callreview.db import get_call_by_identity, get_conn, upsert_call_discovery, update_call_status
from callreview.utils import (
    build_archive_path,
    file_is_stable,
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


def walk_files_with_suffix(root: Path, suffixes: set[str]) -> Iterator[Path]:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in suffixes:
            yield path


def walk_top_level_files_with_suffix(root: Path, suffixes: set[str]) -> Iterator[Path]:
    if not root.exists():
        return
    for path in root.iterdir():
        if path.is_file() and path.suffix.lower() in suffixes:
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

    # CX files land unsorted at the top level of Call_Recordings.
    # Do NOT recurse, or we will rescan WIOGEN-CX/WIOGEN-TS archives forever.
    for path in walk_top_level_files_with_suffix(settings.cx_source_dir, {".mp3"}):
        if path.name.lower().endswith(".playback.mp3"):
            continue
        if not file_is_stable(path, settings.file_stable_seconds):
            continue

        stat = path.stat()
        recorded_at = datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0)

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

    for path in walk_files_with_suffix(settings.vip_source_dir, {".wav"}):
        if path.name.lower().endswith(".playback.mp3"):
            continue
        if not file_is_stable(path, settings.file_stable_seconds):
            continue

        stat = path.stat()
        filename_dt = parse_vip_filename_datetime(path.name)
        path_dt = parse_datetime_from_path_parts(path.parent)
        mtime_dt = datetime.fromtimestamp(stat.st_mtime)

        if filename_dt is not None:
            recorded_at = filename_dt.replace(microsecond=0)
        elif path_dt is not None:
            recorded_at = path_dt.replace(
                hour=mtime_dt.hour,
                minute=mtime_dt.minute,
                second=mtime_dt.second,
                microsecond=0,
            )
        else:
            recorded_at = mtime_dt.replace(microsecond=0)

        canonical_path = build_archive_path(
            archive_root=settings.archive_vip_dir,
            recorded_dt=recorded_at,
            fallback_mtime=stat.st_mtime,
            filename=path.name,
        )

        items.append(
            DiscoveredFile(
                system="vipvoice",
                path=path,
                source_path=path,
                canonical_path=canonical_path,
                recorded_at=recorded_at,
                file_size=stat.st_size,
                modified_ts=stat.st_mtime,
            )
        )

    return items


def _move_to_archive(item: DiscoveredFile) -> tuple[Path, int, float]:
    canonical_path = item.canonical_path

    if item.path.resolve() != canonical_path.resolve():
        if not settings.dry_run:
            safe_move(item.path, canonical_path)
        else:
            canonical_path.parent.mkdir(parents=True, exist_ok=True)

    stat_path = item.path if settings.dry_run else canonical_path
    stat = stat_path.stat()
    return canonical_path, stat.st_size, stat.st_mtime


def _is_unchanged(
    *,
    system: str,
    current_path: Path,
    canonical_path: Path,
    file_size: int,
    modified_ts: float,
) -> bool:
    existing = get_call_by_identity(system, canonical_path.name)
    if existing is None:
        return False

    try:
        existing_size = int(existing["file_size"])
    except (TypeError, ValueError):
        existing_size = -1

    try:
        existing_mtime = float(existing["modified_ts"])
    except (TypeError, ValueError):
        existing_mtime = -1.0

    existing_current = existing["current_path"] or ""
    existing_archive = existing["archive_path"] or ""

    return (
        existing_current == str(canonical_path)
        and existing_archive == str(canonical_path)
        and existing_size == int(file_size)
        and existing_mtime == float(modified_ts)
        and current_path.resolve() == canonical_path.resolve()
    )


def register_discoveries() -> int:
    discovered = 0

    cx_skipped = 0
    cx_seen = 0
    vip_skipped = 0
    vip_seen = 0

    for item in discover_cx_files():
        cx_seen += 1

        if _is_unchanged(
            system="cx",
            current_path=item.path,
            canonical_path=item.canonical_path,
            file_size=item.file_size,
            modified_ts=item.modified_ts,
        ):
            cx_skipped += 1
            continue

        canonical_path, file_size, modified_ts = _move_to_archive(item)
        call_time = datetime.fromtimestamp(modified_ts).replace(microsecond=0).isoformat()

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
        vip_seen += 1

        if _is_unchanged(
            system="vipvoice",
            current_path=item.path,
            canonical_path=item.canonical_path,
            file_size=item.file_size,
            modified_ts=item.modified_ts,
        ):
            vip_skipped += 1
            continue

        canonical_path, file_size, modified_ts = _move_to_archive(item)
        call_time_dt = item.recorded_at or datetime.fromtimestamp(modified_ts).replace(microsecond=0)

        _call_id, inserted = upsert_call_discovery(
            system="vipvoice",
            filename=canonical_path.name,
            source_path=str(item.source_path),
            current_path=str(canonical_path),
            archive_path=str(canonical_path),
            file_size=file_size,
            modified_ts=modified_ts,
            recorded_at=item.recorded_at.isoformat() if item.recorded_at else None,
            call_time=call_time_dt.isoformat() if call_time_dt else None,
            status="queued",
        )
        if inserted:
            discovered += 1

    print(
        f"[discovery] cx_seen={cx_seen} cx_skipped={cx_skipped} "
        f"vip_seen={vip_seen} vip_skipped={vip_skipped} inserted={discovered}"
    )

    return discovered


def register_discoveries_locked() -> int:
    lock_path = settings.db_path.with_suffix(".discover.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0

        return register_discoveries()


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