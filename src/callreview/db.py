from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from callreview.config import settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    ensure_parent_dir(settings.db_path)
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                system TEXT NOT NULL,
                filename TEXT NOT NULL,
                source_path TEXT NOT NULL,
                current_path TEXT NOT NULL,
                archive_path TEXT,
                playback_path TEXT,
                playback_status TEXT NOT NULL DEFAULT 'pending',
                playback_error TEXT,
                file_hash TEXT,
                file_size INTEGER NOT NULL,
                modified_ts REAL NOT NULL,
                recorded_at TEXT,
                call_time TEXT,
                discovered_at TEXT NOT NULL,
                status TEXT NOT NULL,
                transcript_status TEXT NOT NULL,
                transcript_text TEXT,
                summary_text TEXT,
                tags_csv TEXT,
                manual_tags_csv TEXT,
                priority_score INTEGER NOT NULL DEFAULT 0,
                review_status TEXT NOT NULL DEFAULT 'unreviewed',
                reviewed_by TEXT,
                notes TEXT,
                flagged INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        columns = [r["name"] for r in conn.execute("PRAGMA table_info(calls)").fetchall()]

        if "review_status" not in columns:
            conn.execute(
                "ALTER TABLE calls ADD COLUMN review_status TEXT NOT NULL DEFAULT 'unreviewed'"
            )
        if "reviewed_by" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN reviewed_by TEXT")
        if "notes" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN notes TEXT")
        if "flagged" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN flagged INTEGER NOT NULL DEFAULT 0")
        if "playback_path" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN playback_path TEXT")
        if "playback_status" not in columns:
            conn.execute(
                "ALTER TABLE calls ADD COLUMN playback_status TEXT NOT NULL DEFAULT 'pending'"
            )
        if "playback_error" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN playback_error TEXT")
        if "call_time" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN call_time TEXT")
        if "manual_tags_csv" not in columns:
            conn.execute("ALTER TABLE calls ADD COLUMN manual_tags_csv TEXT")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_system ON calls(system)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_current_path ON calls(current_path)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_discovered_at ON calls(discovered_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_call_time ON calls(call_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_review_status ON calls(review_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_flagged ON calls(flagged)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_playback_status ON calls(playback_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_system_filename_lookup ON calls(system, filename)")

        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_system_filename_unique
                ON calls(system, filename)
                """
            )
        except sqlite3.IntegrityError:
            # Legacy duplicate rows may still exist in an older database.
            # The app-level upsert logic below still keeps new inserts sane.
            pass


def get_call_by_current_path(path: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM calls WHERE current_path = ? ORDER BY id DESC LIMIT 1",
            (path,),
        ).fetchone()


def get_call_by_identity(system: str, filename: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT *
            FROM calls
            WHERE system = ? AND filename = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (system, filename),
        ).fetchone()


def get_call_by_id(call_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM calls WHERE id = ? LIMIT 1",
            (call_id,),
        ).fetchone()


def insert_call(
    *,
    system: str,
    filename: str,
    source_path: str,
    current_path: str,
    archive_path: Optional[str],
    file_size: int,
    modified_ts: float,
    recorded_at: Optional[str],
    call_time: Optional[str],
    status: str = "queued",
    transcript_status: str = "pending",
) -> int:
    now = utc_now_iso()

    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO calls (
                uuid,
                system,
                filename,
                source_path,
                current_path,
                archive_path,
                playback_path,
                playback_status,
                playback_error,
                file_hash,
                file_size,
                modified_ts,
                recorded_at,
                call_time,
                discovered_at,
                status,
                transcript_status,
                transcript_text,
                summary_text,
                tags_csv,
                manual_tags_csv,
                priority_score,
                review_status,
                reviewed_by,
                notes,
                flagged,
                error_message,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                system,
                filename,
                source_path,
                current_path,
                archive_path,
                None,
                "pending",
                None,
                None,
                file_size,
                modified_ts,
                recorded_at,
                call_time,
                now,
                status,
                transcript_status,
                None,
                None,
                None,
                None,
                0,
                "unreviewed",
                None,
                None,
                0,
                None,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)


def upsert_call_discovery(
    *,
    system: str,
    filename: str,
    source_path: str,
    current_path: str,
    archive_path: Optional[str],
    file_size: int,
    modified_ts: float,
    recorded_at: Optional[str],
    call_time: Optional[str],
    status: str = "queued",
) -> tuple[int, bool]:
    now = utc_now_iso()
    new_uuid = str(uuid.uuid4())

    with get_conn() as conn:
        before = conn.execute(
            """
            SELECT id
            FROM calls
            WHERE system = ? AND filename = ?
            LIMIT 1
            """,
            (system, filename),
        ).fetchone()

        conn.execute(
            """
            INSERT INTO calls (
                uuid,
                system,
                filename,
                source_path,
                current_path,
                archive_path,
                playback_path,
                playback_status,
                playback_error,
                file_hash,
                file_size,
                modified_ts,
                recorded_at,
                call_time,
                discovered_at,
                status,
                transcript_status,
                transcript_text,
                summary_text,
                tags_csv,
                manual_tags_csv,
                priority_score,
                review_status,
                reviewed_by,
                notes,
                flagged,
                error_message,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(system, filename) DO UPDATE SET
                source_path = excluded.source_path,
                current_path = excluded.current_path,
                archive_path = excluded.archive_path,
                file_size = excluded.file_size,
                modified_ts = excluded.modified_ts,
                recorded_at = COALESCE(excluded.recorded_at, calls.recorded_at),
                call_time = COALESCE(excluded.call_time, calls.call_time),
                status = CASE
                    WHEN calls.status IN ('processed', 'archived', 'processing') THEN calls.status
                    ELSE excluded.status
                END,
                error_message = CASE
                    WHEN calls.status IN ('processed', 'archived', 'processing') THEN calls.error_message
                    ELSE NULL
                END,
                updated_at = excluded.updated_at
            """,
            (
                new_uuid,
                system,
                filename,
                source_path,
                current_path,
                archive_path,
                None,
                "pending",
                None,
                None,
                file_size,
                modified_ts,
                recorded_at,
                call_time,
                now,
                status,
                "pending",
                None,
                None,
                None,
                None,
                0,
                "unreviewed",
                None,
                None,
                0,
                None,
                now,
                now,
            ),
        )

        row = conn.execute(
            """
            SELECT id
            FROM calls
            WHERE system = ? AND filename = ?
            LIMIT 1
            """,
            (system, filename),
        ).fetchone()

        return int(row["id"]), before is None
    
    
def update_call_paths(
    call_id: int,
    *,
    current_path: str,
    archive_path: Optional[str],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET current_path = ?, archive_path = ?, updated_at = ?
            WHERE id = ?
            """,
            (current_path, archive_path, utc_now_iso(), call_id),
        )


def update_playback_info(
    call_id: int,
    *,
    playback_path: Optional[str],
    playback_status: str,
    playback_error: Optional[str],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET playback_path = ?,
                playback_status = ?,
                playback_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                playback_path,
                playback_status,
                playback_error,
                utc_now_iso(),
                call_id,
            ),
        )


def update_call_status(
    call_id: int,
    *,
    status: Optional[str] = None,
    transcript_status: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    row = get_call_by_id(call_id)
    if row is None:
        return

    new_status = status if status is not None else row["status"]
    new_transcript_status = (
        transcript_status if transcript_status is not None else row["transcript_status"]
    )

    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET status = ?,
                transcript_status = ?,
                error_message = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (new_status, new_transcript_status, error_message, utc_now_iso(), call_id),
        )


def update_call_processing_results(
    call_id: int,
    *,
    file_hash: Optional[str],
    transcript_text: str,
    summary_text: str,
    tags_csv: str,
    priority_score: int,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET file_hash = ?,
                transcript_text = ?,
                summary_text = ?,
                tags_csv = ?,
                priority_score = ?,
                transcript_status = 'complete',
                status = 'processed',
                error_message = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (
                file_hash,
                transcript_text,
                summary_text,
                tags_csv,
                priority_score,
                utc_now_iso(),
                call_id,
            ),
        )


def update_review(
    call_id: int,
    *,
    review_status: str,
    reviewed_by: Optional[str],
    notes: Optional[str],
    flagged: int,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET review_status = ?,
                reviewed_by = ?,
                notes = ?,
                flagged = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                review_status,
                reviewed_by,
                notes,
                flagged,
                utc_now_iso(),
                call_id,
            ),
        )


def list_calls(limit: int = 200) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM calls
            ORDER BY COALESCE(call_time, discovered_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return list(rows)


def list_ready_new_calls(limit: int = 10) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM calls
            WHERE status = 'queued'
            ORDER BY COALESCE(call_time, discovered_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return list(rows)


def get_oldest_backlog_call() -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT *
            FROM calls
            WHERE status IN ('queued', 'failed')
            ORDER BY COALESCE(call_time, discovered_at) ASC, id ASC
            LIMIT 1
            """
        ).fetchone()


def search_calls(
    query: str,
    system: Optional[str] = None,
    tag: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort: str = "call_time",
    order: str = "desc",
    limit: int = 200,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM calls WHERE 1=1"
    params: list[object] = []

    if query:
        query_like = f"%{query}%"
        sql += """
        AND (
            filename LIKE ?
            OR current_path LIKE ?
            OR COALESCE(transcript_text, '') LIKE ?
            OR COALESCE(summary_text, '') LIKE ?
            OR COALESCE(tags_csv, '') LIKE ?
            OR COALESCE(manual_tags_csv, '') LIKE ?
            OR COALESCE(notes, '') LIKE ?
        )
        """
        params.extend([query_like] * 7)

    if system:
        sql += " AND system = ?"
        params.append(system)

    if tag:
        sql += """
        AND (
            COALESCE(tags_csv, '') LIKE ?
            OR COALESCE(manual_tags_csv, '') LIKE ?
        )
        """
        params.append(f"%{tag}%")
        params.append(f"%{tag}%")

    if date_from:
        sql += " AND COALESCE(call_time, discovered_at) >= ?"
        params.append(date_from)

    if date_to:
        sql += " AND COALESCE(call_time, discovered_at) < date(?, '+1 day')"
        params.append(date_to)

    sort_map = {
        "date": "COALESCE(call_time, discovered_at)",
        "priority": "priority_score",
        "id": "id",
    }
    sort_expr = sort_map.get(sort, "COALESCE(call_time, discovered_at)")
    sort_order = "ASC" if order.lower() == "asc" else "DESC"

    sql += f" ORDER BY {sort_expr} {sort_order}, id DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    return list(rows)


def top_tags(limit: int = 20) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT tags_csv
            FROM calls
            WHERE tags_csv IS NOT NULL
              AND TRIM(tags_csv) != ''
            """
        ).fetchall()

    for row in rows:
        raw = row["tags_csv"] or ""
        for tag in raw.split(","):
            cleaned = tag.strip()
            if not cleaned:
                continue
            counts[cleaned] = counts.get(cleaned, 0) + 1

    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


def _split_csv_tags(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _join_csv_tags(tags: list[str]) -> str:
    return ",".join(sorted(dict.fromkeys(tags), key=str.lower))


def get_all_distinct_tags(limit: int = 500) -> list[str]:
    counts: dict[str, int] = {}

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT tags_csv, manual_tags_csv
            FROM calls
            """
        ).fetchall()

    for row in rows:
        for raw in (row["tags_csv"], row["manual_tags_csv"]):
            for tag in _split_csv_tags(raw):
                counts[tag] = counts.get(tag, 0) + 1

    tags = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [tag for tag, _count in tags[:limit]]


def add_manual_tag(call_id: int, tag: str) -> None:
    row = get_call_by_id(call_id)
    if row is None:
        return

    clean_tag = tag.strip()
    if not clean_tag:
        return

    manual_tags = _split_csv_tags(row["manual_tags_csv"])
    if clean_tag not in manual_tags:
        manual_tags.append(clean_tag)

    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET manual_tags_csv = ?, updated_at = ?
            WHERE id = ?
            """,
            (_join_csv_tags(manual_tags), utc_now_iso(), call_id),
        )


def remove_manual_tag(call_id: int, tag: str) -> None:
    row = get_call_by_id(call_id)
    if row is None:
        return

    manual_tags = [t for t in _split_csv_tags(row["manual_tags_csv"]) if t != tag]

    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET manual_tags_csv = ?, updated_at = ?
            WHERE id = ?
            """,
            (_join_csv_tags(manual_tags), utc_now_iso(), call_id),
        )
