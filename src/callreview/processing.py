from __future__ import annotations

import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from faster_whisper import WhisperModel

from callreview.config import settings
from callreview.db import (
    update_call_paths,
    update_call_processing_results,
    update_call_status,
    update_playback_info,
)
from callreview.utils import build_archive_path, safe_move, sha256_file


TRANSCRIPTION_MODEL = "small"
TRANSCRIPTION_DEVICE = "cpu"
TRANSCRIPTION_COMPUTE_TYPE = "int8"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_transcription_prompt() -> str:
    prompt_path = project_root() / "config" / "transcription_prompt.txt"
    if not prompt_path.exists():
        return (
            "This is a business phone call. Common names and terms include "
            "Shawn, Sean, Shaun, Colin, Crexendo, VIPVoice, WIOGEN, TelcomFS, "
            "billing, support, customer service, password reset, onboarding."
        )
    return prompt_path.read_text(encoding="utf-8").strip()


def transcribe_audio(path: Path) -> str:
    prompt = load_transcription_prompt()

    model = WhisperModel(
        TRANSCRIPTION_MODEL,
        device=TRANSCRIPTION_DEVICE,
        compute_type=TRANSCRIPTION_COMPUTE_TYPE,
    )

    segments, info = model.transcribe(
        str(path),
        beam_size=5,
        vad_filter=True,
        initial_prompt=prompt,
    )

    transcript_parts: list[str] = []

    for seg in segments:
        text = seg.text.strip()
        if text:
            transcript_parts.append(text)

    transcript_text = " ".join(transcript_parts).strip()

    if not transcript_text:
        language = getattr(info, "language", "unknown")
        return f"[No transcript text returned. Detected language: {language}]"

    return transcript_text


def placeholder_summary(transcript: str, system: str) -> str:
    first_chunk = transcript[:400].strip()
    if not first_chunk:
        return f"System={system}. No transcript text available."

    return f"System={system}. Transcript preview: {first_chunk}"


def placeholder_tags(transcript: str, system: str) -> list[str]:
    tags = [system, "needs-review"]

    lowered = transcript.lower()

    keyword_map = {
        "billing": "billing",
        "invoice": "billing",
        "payment": "billing",
        "password": "password",
        "reset": "password",
        "cancel": "cancellation",
        "disconnect": "cancellation",
        "support": "support",
        "technical": "support",
        "outage": "outage",
        "service down": "outage",
        "voicemail": "voicemail",
        "ticket": "ticket",
    }

    for needle, tag in keyword_map.items():
        if needle in lowered:
            tags.append(tag)

    return sorted(set(tags))


def placeholder_priority(tags: list[str]) -> int:
    score = 10

    if "needs-review" in tags:
        score += 5
    if "cancellation" in tags:
        score += 15
    if "outage" in tags:
        score += 20
    if "billing" in tags:
        score += 5

    return score


def playback_mp3_path_for(original_path: Path) -> Path:
    return original_path.with_suffix(".playback.mp3")


def ffmpeg_exists() -> bool:
    return shutil.which("ffmpeg") is not None


def generate_playback_file(source_path: Path) -> tuple[Path | None, str, str | None]:
    """
    Returns:
        (playback_path, playback_status, playback_error)
    """
    suffix = source_path.suffix.lower()

    if suffix == ".mp3":
        return source_path, "ready", None

    if not ffmpeg_exists():
        return None, "failed", "ffmpeg not found in PATH"

    output_path = playback_mp3_path_for(source_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(source_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "22050",
        "-b:a",
        "96k",
        str(output_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        return None, "failed", f"ffmpeg execution failed: {exc}"

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        return None, "failed", f"ffmpeg conversion failed: {stderr[:1000]}"

    if not output_path.exists() or not output_path.is_file():
        return None, "failed", "ffmpeg reported success but playback file was not created"

    return output_path, "ready", None


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
        update_playback_info(
            call_id,
            playback_path=None,
            playback_status="failed",
            playback_error="Source file missing",
        )
        return

    try:
        update_call_status(call_id, status="processing", transcript_status="running")

        file_hash = sha256_file(current_path)

        # Transcribe original source audio, not playback audio.
        transcript = transcribe_audio(current_path)
        summary = placeholder_summary(transcript, system)
        tags = placeholder_tags(transcript, system)
        priority = placeholder_priority(tags)

        update_call_processing_results(
            call_id,
            file_hash=file_hash,
            transcript_text=transcript,
            summary_text=summary,
            tags_csv=",".join(tags),
            priority_score=priority,
        )

        if system == "vipvoice":
            archived_path = current_path

            update_call_paths(
                call_id,
                current_path=str(archived_path),
                archive_path=str(archived_path),
            )

            playback_path, playback_status, playback_error = generate_playback_file(archived_path)
            update_playback_info(
                call_id,
                playback_path=str(playback_path) if playback_path else None,
                playback_status=playback_status,
                playback_error=playback_error,
            )

            update_call_status(call_id, status="archived")
            return

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

            playback_path, playback_status, playback_error = generate_playback_file(current_path)
            update_playback_info(
                call_id,
                playback_path=str(playback_path) if playback_path else None,
                playback_status=playback_status,
                playback_error=playback_error,
            )

            update_call_status(call_id, status="archived")
            return

        safe_move(current_path, archive_path)

        update_call_paths(
            call_id,
            current_path=str(archive_path),
            archive_path=str(archive_path),
        )

        playback_path, playback_status, playback_error = generate_playback_file(archive_path)
        update_playback_info(
            call_id,
            playback_path=str(playback_path) if playback_path else None,
            playback_status=playback_status,
            playback_error=playback_error,
        )

        update_call_status(call_id, status="archived")

    except Exception as exc:
        update_call_status(
            call_id,
            status="failed",
            transcript_status="failed",
            error_message=str(exc),
        )
        update_playback_info(
            call_id,
            playback_path=None,
            playback_status="failed",
            playback_error=str(exc),
        )