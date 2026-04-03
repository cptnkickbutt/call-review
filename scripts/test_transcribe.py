from __future__ import annotations

import json
import sys
from pathlib import Path

from faster_whisper import WhisperModel


# 🔧 EDIT THIS AS YOU LEARN YOUR DATA
DEFAULT_PROMPT = Path("config/transcription_prompt.txt").read_text()


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_transcribe.py <audio_file> [model]")
        return 1

    audio_path = Path(sys.argv[1]).resolve()
    model_name = sys.argv[2] if len(sys.argv) >= 3 else "small"

    if not audio_path.exists() or not audio_path.is_file():
        print(f"Audio file not found: {audio_path}")
        return 1

    print(f"Loading model: {model_name}")
    print(f"Audio file: {audio_path}")
    print(f"Initial prompt: {DEFAULT_PROMPT}")

    model = WhisperModel(
        model_name,
        device="cpu",
        compute_type="int8",
    )

    segments, info = model.transcribe(
        str(audio_path),
        beam_size=5,
        vad_filter=True,
        initial_prompt=DEFAULT_PROMPT,
    )

    segments_list: list[dict] = []
    transcript_parts: list[str] = []

    print()
    print("=== TRANSCRIPT ===")

    for seg in segments:
        text = seg.text.strip()
        transcript_parts.append(text)

        row = {
            "start": seg.start,
            "end": seg.end,
            "text": text,
        }
        segments_list.append(row)

        print(f"[{seg.start:8.2f} -> {seg.end:8.2f}] {text}")

    transcript_text = " ".join(part for part in transcript_parts if part).strip()

    print()
    print("=== SUMMARY INFO ===")
    print(f"Detected language: {info.language}")
    print(f"Language probability: {info.language_probability}")

    out_json = audio_path.with_suffix(audio_path.suffix + ".transcript.json")
    out_txt = audio_path.with_suffix(audio_path.suffix + ".transcript.txt")

    out_json.write_text(
        json.dumps(
            {
                "audio_file": str(audio_path),
                "model": model_name,
                "initial_prompt": DEFAULT_PROMPT,
                "language": info.language,
                "language_probability": info.language_probability,
                "segments": segments_list,
                "transcript_text": transcript_text,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    out_txt.write_text(transcript_text, encoding="utf-8")

    print()
    print(f"Wrote transcript text: {out_txt}")
    print(f"Wrote transcript json: {out_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())