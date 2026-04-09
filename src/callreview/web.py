from __future__ import annotations

import mimetypes
from pathlib import Path

from flask import Flask, abort, redirect, render_template, request, send_file, url_for

from callreview.db import (
    add_manual_tag,
    count_calls,
    get_all_distinct_tags,
    get_call_by_id,
    init_db,
    remove_manual_tag,
    search_calls,
    top_tags,
    update_review,
)


class TagInfo:
    def __init__(self, label: str, css_class: str):
        self.label = label
        self.css_class = css_class


def detect_audio_mime(path: Path) -> str:
    suffix = path.suffix.lower()

    if suffix == ".mp3":
        return "audio/mpeg"
    if suffix == ".wav":
        return "audio/wav"
    if suffix == ".ogg":
        return "audio/ogg"
    if suffix == ".m4a":
        return "audio/mp4"
    if suffix == ".mp4":
        return "audio/mp4"
    if suffix == ".gsm":
        return "audio/gsm"

    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def preferred_audio_path(row) -> Path | None:
    playback_path = row["playback_path"]
    if playback_path:
        path = Path(playback_path).resolve()
        if path.exists() and path.is_file():
            return path

    current_path = row["current_path"]
    if current_path:
        path = Path(current_path).resolve()
        if path.exists() and path.is_file():
            return path

    return None


def truncate_summary(summary: str | None, max_len: int = 120) -> str:
    if not summary:
        return ""
    summary = summary.strip()
    if len(summary) <= max_len:
        return summary
    return summary[:max_len].rstrip() + "..."


def display_filename(filename: str | None, keep_last: int = 18) -> str:
    if not filename:
        return ""
    if len(filename) <= keep_last:
        return filename
    return "..." + filename[-keep_last:]


def format_created(value: str | None) -> str:
    if not value:
        return ""
    try:
        dt = value.replace("Z", "+00:00")
        from datetime import datetime as _dt

        parsed_dt = _dt.fromisoformat(dt)
        return parsed_dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return value[:16].replace("T", " ")


def classify_tag(tag: str) -> TagInfo:
    if ":" not in tag:
        return TagInfo(tag, "")

    kind, value = tag.split(":", 1)

    if kind == "property":
        return TagInfo(value.replace("_", " ").title(), "tag-property")

    if kind == "name":
        return TagInfo(value.replace("_", " ").title(), "tag-name")

    if kind == "sentiment":
        if value == "negative":
            return TagInfo("Negative", "tag-sentiment-negative")
        if value == "positive":
            return TagInfo("Positive", "tag-sentiment-positive")
        return TagInfo("Neutral", "tag-sentiment-neutral")

    if kind == "reason":
        label = value.replace("_", " ").title()

        low = {"payment", "billing", "voicemail", "general"}
        medium = {"scheduling", "password_reset"}
        high = {"phone_issue", "internet_issue", "cancellation"}
        critical = {"outage"}

        if value in low:
            return TagInfo(label, "tag-reason-low")
        if value in medium:
            return TagInfo(label, "tag-reason-medium")
        if value in high:
            return TagInfo(label, "tag-reason-high")
        if value in critical:
            return TagInfo(label, "tag-reason-critical")

        return TagInfo(label, "tag-reason-medium")

    return TagInfo(tag, "")


def split_csv_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def combined_tags(row) -> list[str]:
    auto_tags = split_csv_tags(row["tags_csv"])
    manual_tags = split_csv_tags(row["manual_tags_csv"])
    merged: list[str] = []
    seen: set[str] = set()

    for tag in auto_tags + manual_tags:
        if tag not in seen:
            merged.append(tag)
            seen.add(tag)

    return merged


def manual_tags_only(row) -> list[str]:
    return split_csv_tags(row["manual_tags_csv"])


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    init_db()

    @app.context_processor
    def inject_helpers():
        q = request.args.get("q", "").strip()
        system = request.args.get("system", "").strip()
        tag = request.args.get("tag", "").strip()
        date_from = request.args.get("date_from", "").strip()
        date_to = request.args.get("date_to", "").strip()
        sort = request.args.get("sort", "date").strip() or "date"
        order = request.args.get("order", "desc").strip().lower() or "desc"

        def filter_url(**updates):
            params = {
                "q": q,
                "system": system,
                "tag": tag,
                "date_from": date_from,
                "date_to": date_to,
                "sort": sort,
                "order": order,
                "page": 1,
            }
            for key, value in updates.items():
                params[key] = value

            clean_params = {k: v for k, v in params.items() if v not in ("", None)}
            return url_for("index", **clean_params)

        def sort_url(field: str):
            next_order = "desc"
            if sort == field and order == "desc":
                next_order = "asc"
            elif sort == field and order == "asc":
                next_order = "desc"

            return filter_url(sort=field, order=next_order, page=1)

        def page_url(target_page: int):
            params = {
                "q": q,
                "system": system,
                "tag": tag,
                "date_from": date_from,
                "date_to": date_to,
                "sort": sort,
                "order": order,
                "page": max(target_page, 1),
            }
            clean_params = {k: v for k, v in params.items() if v not in ("", None)}
            return url_for("index", **clean_params)

        return {
            "filter_url": filter_url,
            "sort_url": sort_url,
            "page_url": page_url,
            "truncate_summary": truncate_summary,
            "display_filename": display_filename,
            "format_created": format_created,
            "classify_tag": classify_tag,
        }

    @app.route("/")
    def index():
        q = request.args.get("q", "").strip()
        system = request.args.get("system", "").strip() or None
        tag = request.args.get("tag", "").strip() or None
        date_from = request.args.get("date_from", "").strip() or None
        date_to = request.args.get("date_to", "").strip() or None
        sort = request.args.get("sort", "date").strip() or "date"
        order = request.args.get("order", "desc").strip().lower() or "desc"

        page = max(request.args.get("page", 1, type=int), 1)
        per_page = 50
        offset = (page - 1) * per_page

        total_count = count_calls(
            query=q,
            system=system,
            tag=tag,
            date_from=date_from,
            date_to=date_to,
        )

        rows = search_calls(
            query=q,
            system=system,
            tag=tag,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
            order=order,
            limit=per_page,
            offset=offset,
        )

        total_pages = max((total_count + per_page - 1) // per_page, 1)
        start_row = offset + 1 if total_count else 0
        end_row = min(offset + len(rows), total_count)

        return render_template(
            "index.html",
            rows=rows,
            q=q,
            system=system or "",
            tag=tag or "",
            date_from=date_from or "",
            date_to=date_to or "",
            sort=sort,
            order=order,
            page=page,
            per_page=per_page,
            total_count=total_count,
            total_pages=total_pages,
            start_row=start_row,
            end_row=end_row,
            top_tag_rows=top_tags(20),
            combined_tags=combined_tags,
        )

    @app.route("/call/<int:call_id>")
    def call_detail(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)

        audio_path = preferred_audio_path(row)
        playable = audio_path is not None
        audio_mime = detect_audio_mime(audio_path) if audio_path else "application/octet-stream"

        return render_template(
            "detail.html",
            row=row,
            playable=playable,
            audio_mime=audio_mime,
            all_tags=get_all_distinct_tags(),
            combined_tags=combined_tags,
            manual_tags_only=manual_tags_only,
        )

    @app.route("/audio/<int:call_id>")
    def audio(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)

        path = preferred_audio_path(row)
        if path is None:
            abort(404)

        mime = detect_audio_mime(path)

        return send_file(
            path,
            mimetype=mime,
            as_attachment=False,
            download_name=path.name,
            conditional=True,
        )

    @app.route("/call/<int:call_id>/review", methods=["POST"])
    def update_review_route(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)

        review_status = request.form.get("review_status", "unreviewed").strip().lower()
        if review_status not in {"unreviewed", "reviewed"}:
            review_status = "unreviewed"

        notes = request.form.get("notes", "").strip()
        flagged = 1 if request.form.get("flagged") == "on" else 0

        reviewed_by = "local-user"

        update_review(
            call_id,
            review_status=review_status,
            reviewed_by=reviewed_by,
            notes=notes or None,
            flagged=flagged,
        )

        return redirect(url_for("call_detail", call_id=call_id))

    @app.route("/call/<int:call_id>/tags/add", methods=["POST"])
    def add_tag_route(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)

        existing_tag = request.form.get("existing_tag", "").strip()
        new_tag_type = request.form.get("new_tag_type", "").strip().lower()
        new_tag_value = request.form.get("new_tag_value", "").strip()

        tag_to_add = ""

        if existing_tag:
            tag_to_add = existing_tag
        elif new_tag_type and new_tag_value:
            normalized_value = new_tag_value.strip().lower().replace(" ", "_")
            tag_to_add = f"{new_tag_type}:{normalized_value}"

        if tag_to_add:
            add_manual_tag(call_id, tag_to_add)

        return redirect(url_for("call_detail", call_id=call_id))

    @app.route("/call/<int:call_id>/tags/remove", methods=["POST"])
    def remove_tag_route(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)

        tag = request.form.get("tag", "").strip()
        if tag:
            remove_manual_tag(call_id, tag)

        return redirect(url_for("call_detail", call_id=call_id))

    return app