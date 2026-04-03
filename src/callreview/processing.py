from __future__ import annotations

from datetime import datetime
from pathlib import Path

from callreview.config import settings
from callreview.db import (
    update_call_paths,
    update_call_processing_results,
    update_call_status,
)
from callreview.utils import build_archive_path, safe_move, sha256_file


def placeholder_transcribe(path: Path) -> str:
    return (
        f"Placeholder transcript for {path.name}.\n"
        f"This is where a real transcription engine will later be integrated."
    )


def placeholder_summary(transcript: str, system: str) -> str:
    return f"System={system}. Placeholder summary generated from transcript."


def placeholder_tags(transcript: str, system: str) -> list[str]:
    tags = [system, "needs-review", "placeholder"]
    lowered = transcript.lower()
    if "billing" in lowered:
        tags.append("billing")
    if "password" in lowered:
        tags.append("password")
    if "cancel" in lowered:
        tags.append("cancellation")
    return tags


def placeholder_priority(tags: list[str]) -> int:
    score = 10
    if "needs-review" in tags:
        score += 5
    if "cancellation" in tags:
        score += 15
    return score


def process_call_row(row) -> None:
    call_id = row["id"]
    system = row["system"]
    current_path = Path(row["current_path"])
    recorded_at_raw = row["recorded_at"]

    if not current_path.exists():
        update_call_status(
            call_id,
            status="failed",
            transcript_status="failed",
            error_message="File no longer exists at current_path",
        )
        return

    try:
        update_call_status(call_id, status="processing", transcript_status="running")

        file_hash = sha256_file(current_path)
        transcript = placeholder_transcribe(current_path)
        summary = placeholder_summary(transcript, system)
        tags = placeholder_tags(transcript, system)
        priority = placeholder_priority(tags)

        update_call_processing_results(
            call_id,
            file_hash=file_hash,
            transcript_text=transcript,
            summary_text=summary,
            tags_csv=",".join(sorted(set(tags))),
            priority_score=priority,
        )

        # VIPVoice may already be in the final structure.
        if system == "vipvoice":
            update_call_paths(
                call_id,
                current_path=str(current_path),
                archive_path=str(current_path),
            )
            update_call_status(call_id, status="archived")
            return

        # CX gets moved into final dated archive.
        recorded_dt = None
        if recorded_at_raw:
            try:
                recorded_dt = datetime.fromisoformat(recorded_at_raw)
            except ValueError:
                recorded_dt = None

        archive_root = settings.archive_cx_dir
        archive_path = build_archive_path(
            archive_root=archive_root,
            recorded_dt=recorded_dt,
            fallback_mtime=current_path.stat().st_mtime,
            filename=current_path.name,
        )

        if settings.dry_run:
            update_call_paths(
                call_id,
                current_path=str(current_path),
                archive_path=str(archive_path),
            )
            update_call_status(call_id, status="archived")
            return

        safe_move(current_path, archive_path)
        update_call_paths(
            call_id,
            current_path=str(archive_path),
            archive_path=str(archive_path),
        )
        update_call_status(call_id, status="archived")

    except Exception as exc:
        update_call_status(
            call_id,
            status="failed",
            transcript_status="failed",
            error_message=str(exc),
        )