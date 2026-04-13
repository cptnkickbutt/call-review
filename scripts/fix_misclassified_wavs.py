from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3

from callreview.config import settings
from callreview.db import get_conn, utc_now_iso
from callreview.ingest import parse_vip_filename_datetime
from callreview.utils import build_archive_path, parse_datetime_from_path_parts, safe_move


DRY_RUN = True


def determine_vip_target(path: Path) -> tuple[Path, str | None]:
    stat = path.stat()
    filename_dt = parse_vip_filename_datetime(path.name)
    recorded_at = filename_dt or parse_datetime_from_path_parts(path.parent)
    target = build_archive_path(
        archive_root=settings.archive_vip_dir,
        recorded_dt=recorded_at,
        fallback_mtime=stat.st_mtime,
        filename=path.name,
    )
    return target, recorded_at.isoformat() if recorded_at else None


def main() -> None:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, filename, current_path, archive_path, playback_path
            FROM calls
            WHERE system = 'cx'
              AND LOWER(filename) LIKE '%.wav'
            ORDER BY id ASC
            """
        ).fetchall()

    print(f"Found {len(rows)} misclassified wav row(s).")

    moved = 0
    updated = 0
    missing = 0

    for row in rows:
        call_id = row["id"]
        current_path = Path(row["current_path"])

        if not current_path.exists():
            print(f"[missing] id={call_id} path={current_path}")
            missing += 1
            continue

        target_path, recorded_at = determine_vip_target(current_path)

        print(f"[fix] id={call_id}")
        print(f"      from: {current_path}")
        print(f"      to:   {target_path}")

        if not DRY_RUN:
            if current_path.resolve() != target_path.resolve():
                safe_move(current_path, target_path)
                moved += 1

            with get_conn() as conn:
                conn.execute(
                    """
                    UPDATE calls
                    SET system = 'vipvoice',
                        current_path = ?,
                        archive_path = ?,
                        recorded_at = COALESCE(?, recorded_at),
                        playback_path = NULL,
                        playback_status = 'pending',
                        playback_error = NULL,
                        status = 'queued',
                        transcript_status = 'pending',
                        error_message = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(target_path),
                        str(target_path),
                        recorded_at,
                        utc_now_iso(),
                        call_id,
                    ),
                )
            updated += 1

    print()
    print(f"Moved:   {moved}")
    print(f"Updated: {updated}")
    print(f"Missing: {missing}")
    print(f"DRY_RUN: {DRY_RUN}")


if __name__ == "__main__":
    main()