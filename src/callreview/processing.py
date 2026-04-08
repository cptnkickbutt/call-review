from __future__ import annotations

import re
import shutil
import subprocess
from datetime import datetime
from functools import lru_cache
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


TRANSCRIPTION_MODEL = "medium"
TRANSCRIPTION_DEVICE = "cpu"
TRANSCRIPTION_COMPUTE_TYPE = "int8"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_text_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


@lru_cache(maxsize=1)
def load_transcription_prompt() -> str:
    prompt_path = project_root() / "config" / "transcription_prompt.txt"
    if not prompt_path.exists():
        return (
            "This is a business phone call. Common names and terms include "
            "Shawn, Sean, Shaun, Colin, Crexendo, VIPVoice, WIOGEN, TelcomFS, "
            "billing, support, customer service, password reset, onboarding."
        )
    return prompt_path.read_text(encoding="utf-8").strip()


@lru_cache(maxsize=1)
def load_properties() -> list[str]:
    return load_text_lines(project_root() / "config" / "properties.txt")


def slugify_tag_value(text: str) -> str:
    value = text.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


@lru_cache(maxsize=1)
def load_staff_aliases() -> dict[str, list[str]]:
    """
    Expected format per line:
      Canonical: Alias1, Alias2, Alias3
    Also supports plain one-name-per-line:
      Marion
    """
    path = project_root() / "config" / "staff_names.txt"
    if not path.exists():
        return {}

    aliases: dict[str, list[str]] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if ":" in line:
            canonical, alias_blob = line.split(":", 1)
            canonical = canonical.strip()
            alias_list = [a.strip() for a in alias_blob.split(",") if a.strip()]
        else:
            canonical = line
            alias_list = [line]

        if not canonical:
            continue

        canonical_slug = slugify_tag_value(canonical)
        all_aliases = sorted(set([canonical] + alias_list), key=str.lower)
        aliases[canonical_slug] = all_aliases

    return aliases


@lru_cache(maxsize=1)
def get_whisper_model() -> WhisperModel:
    return WhisperModel(
        TRANSCRIPTION_MODEL,
        device=TRANSCRIPTION_DEVICE,
        compute_type=TRANSCRIPTION_COMPUTE_TYPE,
    )


def transcribe_audio(path: Path) -> str:
    prompt = load_transcription_prompt()
    model = get_whisper_model()

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


def build_summary(transcript: str) -> str:
    preview = transcript[:220].strip()
    return preview + ("..." if len(transcript) > 220 else "")


def detect_reason_tags(transcript: str) -> list[str]:
    text = normalize_for_match(transcript)

    reason_patterns: dict[str, list[str]] = {
        "payment": [
            "make a payment",
            "pay my bill",
            "pay bill",
            "make payment",
            "payment",
        ],
        "billing": [
            "billing",
            "invoice",
            "charged",
            "charge",
            "autopay",
            "auto pay",
            "bill",
            "refund",
        ],
        "internet_issue": [
            "internet issue",
            "internet down",
            "no internet",
            "internet not working",
            "wifi not working",
            "wi-fi not working",
            "can't get online",
            "cannot get online",
            "service down",
        ],
        "phone_issue": [
            "phone issue",
            "phone not working",
            "voip issue",
            "can't call out",
            "cannot call out",
            "can't receive calls",
            "cannot receive calls",
        ],
        "scheduling": [
            "schedule",
            "scheduling",
            "appointment",
            "reschedule",
            "set up a time",
        ],
        "cancellation": [
            "cancel service",
            "cancellation",
            "disconnect service",
            "disconnect",
            "terminate service",
        ],
        "outage": [
            "outage",
            "service outage",
            "widespread issue",
            "everyone is down",
            "whole property is down",
        ],
        "password_reset": [
            "password reset",
            "reset password",
            "forgot password",
        ],
        "voicemail": [
            "voicemail",
            "voice mail",
            "mailbox",
        ],
    }

    found: list[str] = []

    for reason, needles in reason_patterns.items():
        if any(needle in text for needle in needles):
            found.append(f"reason:{reason}")

    return found


def detect_property_tags(transcript: str) -> list[str]:
    text = normalize_for_match(transcript)
    found: list[str] = []

    for property_name in load_properties():
        if normalize_for_match(property_name) in text:
            found.append(f"property:{slugify_tag_value(property_name)}")

    return found


def detect_name_tags(transcript: str) -> list[str]:
    text = normalize_for_match(transcript)
    found: list[str] = []

    for canonical_slug, aliases in load_staff_aliases().items():
        for alias in aliases:
            alias_norm = normalize_for_match(alias)
            if not alias_norm:
                continue
            if re.search(rf"\b{re.escape(alias_norm)}\b", text):
                found.append(f"name:{canonical_slug}")
                break

    return found


def detect_sentiment_tags(transcript: str) -> list[str]:
    text = normalize_for_match(transcript)

    negative_hits = [
        "not what i was told",
        "you said",
        "spoke to someone already",
        "called multiple times",
        "this is ridiculous",
        "frustrated",
        "upset",
        "angry",
        "cancel service",
        "refund",
        "no internet",
        "still not working",
        "not working",
        "outage",
    ]
    positive_hits = [
        "thank you",
        "appreciate it",
        "resolved",
        "that works",
        "sounds good",
    ]

    neg = sum(1 for phrase in negative_hits if phrase in text)
    pos = sum(1 for phrase in positive_hits if phrase in text)

    if neg >= 2:
        return ["sentiment:negative"]
    if pos >= 2 and neg == 0:
        return ["sentiment:positive"]
    return ["sentiment:neutral"]


def build_tags(transcript: str) -> list[str]:
    tags: set[str] = set()

    for tag in detect_reason_tags(transcript):
        tags.add(tag)

    for tag in detect_property_tags(transcript):
        tags.add(tag)

    for tag in detect_name_tags(transcript):
        tags.add(tag)

    for tag in detect_sentiment_tags(transcript):
        tags.add(tag)

    if not any(tag.startswith("reason:") for tag in tags):
        tags.add("reason:general")

    return sorted(tags)


def score_priority(tags: list[str]) -> int:
    score = 10

    for tag in tags:
        if tag == "reason:payment":
            score += 2
        elif tag == "reason:billing":
            score += 4
        elif tag == "reason:scheduling":
            score += 3
        elif tag == "reason:password_reset":
            score += 4
        elif tag == "reason:phone_issue":
            score += 10
        elif tag == "reason:internet_issue":
            score += 15
        elif tag == "reason:outage":
            score += 20
        elif tag == "reason:cancellation":
            score += 12
        elif tag == "reason:voicemail":
            score += 1

        elif tag == "sentiment:negative":
            score += 8
        elif tag == "sentiment:positive":
            score -= 2

    return max(score, 1)


def playback_mp3_path_for(original_path: Path) -> Path:
    return original_path.with_suffix(".playback.mp3")


def ffmpeg_exists() -> bool:
    return shutil.which("ffmpeg") is not None


def generate_playback_file(source_path: Path) -> tuple[Path | None, str, str | None]:
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
        transcript = transcribe_audio(current_path)
        tags = build_tags(transcript)
        summary = build_summary(transcript)
        priority = score_priority(tags)

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