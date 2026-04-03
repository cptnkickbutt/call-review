from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from callreview.config import settings


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


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
                file_hash TEXT,
                file_size INTEGER NOT NULL,
                modified_ts REAL NOT NULL,
                recorded_at TEXT,
                discovered_at TEXT NOT NULL,
                status TEXT NOT NULL,
                transcript_status TEXT NOT NULL,
                transcript_text TEXT,
                summary_text TEXT,
                tags_csv TEXT,
                priority_score INTEGER NOT NULL DEFAULT 0,
                review_status TEXT NOT NULL DEFAULT 'unreviewed',
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calls_system ON calls(system)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calls_current_path ON calls(current_path)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calls_discovered_at ON calls(discovered_at)
            """
        )


def get_call_by_current_path(path: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM calls WHERE current_path = ? LIMIT 1",
            (path,),
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
    file_size: int,
    modified_ts: float,
    recorded_at: Optional[str],
) -> int:
    now = utc_now_iso()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO calls (
                uuid, system, filename, source_path, current_path,
                archive_path, file_hash, file_size, modified_ts, recorded_at,
                discovered_at, status, transcript_status, transcript_text,
                summary_text, tags_csv, priority_score, review_status,
                error_message, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                system,
                filename,
                source_path,
                current_path,
                None,
                None,
                file_size,
                modified_ts,
                recorded_at,
                now,
                "new",
                "pending",
                None,
                None,
                None,
                0,
                "unreviewed",
                None,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)


def update_call_paths(call_id: int, *, current_path: str, archive_path: Optional[str]) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE calls
            SET current_path = ?, archive_path = ?, updated_at = ?
            WHERE id = ?
            """,
            (current_path, archive_path, utc_now_iso(), call_id),
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
            SET status = ?, transcript_status = ?, error_message = ?, updated_at = ?
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


def list_calls(limit: int = 200) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM calls
            ORDER BY discovered_at DESC, id DESC
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
            ORDER BY discovered_at DESC, id DESC
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
            WHERE status IN ('new', 'queued', 'failed')
            ORDER BY discovered_at ASC, id ASC
            LIMIT 1
            """
        ).fetchone()


def search_calls(query: str, system: Optional[str] = None, limit: int = 200) -> list[sqlite3.Row]:
    query_like = f"%{query}%"
    sql = """
        SELECT *
        FROM calls
        WHERE (
            filename LIKE ?
            OR current_path LIKE ?
            OR COALESCE(transcript_text, '') LIKE ?
            OR COALESCE(summary_text, '') LIKE ?
            OR COALESCE(tags_csv, '') LIKE ?
        )
    """
    params: list[object] = [query_like, query_like, query_like, query_like, query_like]

    if system:
        sql += " AND system = ?"
        params.append(system)

    sql += " ORDER BY discovered_at DESC, id DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return list(rows)