from __future__ import annotations

from datetime import datetime
from pathlib import Path

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
    merged = 0
    missing = 0
    errors = 0

    for row in rows:
        call_id = row["id"]
        filename = row["filename"]
        current_path = Path(row["current_path"]) if row["current_path"] else None
        target_path: Path | None = None
        recorded_at: str | None = None
        existing_vip_id: int | None = None

        try:
            if current_path is None:
                print(f"[missing] id={call_id} file={filename} path=None")
                missing += 1
                continue

            if not current_path.exists():
                print(f"[missing] id={call_id} file={filename} path={current_path}")
                missing += 1
                continue

            target_path, recorded_at = determine_vip_target(current_path)

            with get_conn() as conn:
                existing_vip = conn.execute(
                    """
                    SELECT id, current_path, archive_path, playback_path, status, transcript_status
                    FROM calls
                    WHERE system = 'vipvoice'
                      AND filename = ?
                    LIMIT 1
                    """,
                    (filename,),
                ).fetchone()

            if existing_vip:
                existing_vip_id = int(existing_vip["id"])

            print(f"[fix] id={call_id}")
            print(f"      from: {current_path}")
            print(f"      to:   {target_path}")
            if existing_vip_id is not None:
                print(f"      merge into existing vipvoice row id={existing_vip_id}")

            if DRY_RUN:
                continue

            if current_path.resolve() != target_path.resolve():
                safe_move(current_path, target_path)
                moved += 1

            if existing_vip_id is not None:
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE calls
                        SET current_path = ?,
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
                            existing_vip_id,
                        ),
                    )
                    conn.execute(
                        "DELETE FROM calls WHERE id = ?",
                        (call_id,),
                    )
                merged += 1
            else:
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

        except Exception as exc:
            errors += 1
            print(f"[error] id={call_id} file={filename} err={exc}")
            if current_path is not None:
                print(f"        current_path={current_path}")
            if target_path is not None:
                print(f"        target_path={target_path}")
            if existing_vip_id is not None:
                print(f"        existing_vip_id={existing_vip_id}")

    print()
    print(f"Moved:   {moved}")
    print(f"Updated: {updated}")
    print(f"Merged:  {merged}")
    print(f"Missing: {missing}")
    print(f"Errors:  {errors}")
    print(f"DRY_RUN: {DRY_RUN}")


if __name__ == "__main__":
    main()