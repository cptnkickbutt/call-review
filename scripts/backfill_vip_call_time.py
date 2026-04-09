from __future__ import annotations

from datetime import datetime
from pathlib import Path

from callreview.db import get_conn, utc_now_iso
from callreview.ingest import parse_vip_filename_datetime
from callreview.utils import parse_datetime_from_path_parts


def derive_vip_call_time(path: Path) -> datetime | None:
    if not path.exists():
        return None

    stat = path.stat()
    mtime_dt = datetime.fromtimestamp(stat.st_mtime)

    filename_dt = parse_vip_filename_datetime(path.name)
    path_dt = parse_datetime_from_path_parts(path.parent)

    if filename_dt is not None:
        return filename_dt.replace(microsecond=0)

    if path_dt is not None:
        return path_dt.replace(
            hour=mtime_dt.hour,
            minute=mtime_dt.minute,
            second=mtime_dt.second,
            microsecond=0,
        )

    return mtime_dt.replace(microsecond=0)


def main(dry_run: bool = True) -> None:
    updated = 0
    skipped = 0
    missing = 0

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, filename, current_path, archive_path, call_time
            FROM calls
            WHERE system = 'vipvoice'
            """
        ).fetchall()

    for row in rows:
        current_path = Path(row["current_path"]) if row["current_path"] else None
        archive_path = Path(row["archive_path"]) if row["archive_path"] else None

        path = None
        if current_path and current_path.exists():
            path = current_path
        elif archive_path and archive_path.exists():
            path = archive_path

        if path is None:
            missing += 1
            continue

        derived = derive_vip_call_time(path)
        if derived is None:
            skipped += 1
            continue

        new_call_time = derived.isoformat()

        if row["call_time"] == new_call_time:
            skipped += 1
            continue

        print(f"ID {row['id']}: {row['call_time']} -> {new_call_time}")

        if not dry_run:
            with get_conn() as conn:
                conn.execute(
                    """
                    UPDATE calls
                    SET call_time = ?, recorded_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (new_call_time, new_call_time, utc_now_iso(), row["id"]),
                )

        updated += 1

    print(f"updated={updated} skipped={skipped} missing={missing} dry_run={dry_run}")


if __name__ == "__main__":
    main(dry_run=True)