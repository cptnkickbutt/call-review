from __future__ import annotations

import mimetypes
from pathlib import Path

from flask import Flask, abort, redirect, render_template_string, request, send_file, url_for

from callreview.db import (
    add_manual_tag,
    get_all_distinct_tags,
    get_call_by_id,
    init_db,
    remove_manual_tag,
    search_calls,
    top_tags,
    update_review,
)


INDEX_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Call Recordings</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
        }
        h1 {
            margin-bottom: 8px;
        }
        h2 {
            margin-top: 24px;
            margin-bottom: 10px;
        }
        form {
            margin-bottom: 16px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ccc;
            padding: 8px;
            vertical-align: top;
        }
        th {
            background: #f5f5f5;
            text-align: left;
        }
        .tag {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px 4px 2px 0;
            border: 1px solid #bbb;
            background-color: #fafafa;
        }
        .tag a,
        .top-tag a,
        .linkish {
            text-decoration: none;
            color: inherit;
        }
        .linkish:hover,
        .tag a:hover,
        .top-tag a:hover {
            text-decoration: underline;
        }
        .muted {
            color: #666;
        }
        .summary {
            max-width: 320px;
        }
        .flag {
            font-size: 18px;
            line-height: 1;
        }
        .top-tags {
            margin: 12px 0 18px 0;
        }
        .top-tag {
            display: inline-block;
            padding: 5px 10px;
            border: 1px solid #aaa;
            border-radius: 14px;
            font-size: 13px;
            margin: 4px 6px 4px 0;
            background-color: #f8f8f8;
        }
        .top-tag-count {
            color: #666;
            margin-left: 6px;
            font-size: 12px;
        }
        .controls-row {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            align-items: center;
        }
        .sort-link {
            text-decoration: none;
            color: inherit;
        }
        .sort-link:hover {
            text-decoration: underline;
        }
        .sort-indicator {
            color: #666;
            font-size: 12px;
            margin-left: 4px;
        }
        .summary-short {
            white-space: normal;
        }

        .tag-property {
            background-color: #eaf3ff;
            border-color: #9ec5fe;
        }
        .tag-name {
            background-color: #f3e8ff;
            border-color: #c4b5fd;
        }
        .tag-sentiment-positive {
            background-color: #e8f7e8;
            border-color: #9bd39b;
        }
        .tag-sentiment-neutral {
            background-color: #f2f2f2;
            border-color: #cfcfcf;
        }
        .tag-sentiment-negative {
            background-color: #fdeaea;
            border-color: #f5a5a5;
        }
        .tag-reason-low {
            background-color: #fff7db;
            border-color: #e8cf77;
        }
        .tag-reason-medium {
            background-color: #e6f7f2;
            border-color: #8fd3c1;
        }
        .tag-reason-high {
            background-color: #ffe8cc;
            border-color: #f5b971;
        }
        .tag-reason-critical {
            background-color: #fde2e2;
            border-color: #ef9a9a;
        }
    </style>
</head>
<body>
    <h1>Telcom Call Recording Review Database</h1>
    <p class="muted">Starter UI. Work in progress. May add more features later.</p>
    <p class="warning">Disclaimer: Tags and Transcripts are auto-generated and may be inaccurate. Please verify before relying on them.</p>

    <form method="get" action="/">
        <div class="controls-row">
            <input type="text" name="q" value="{{ q }}" placeholder="Search..." size="30">

            <select name="system">
                <option value="">All systems</option>
                <option value="cx" {% if system == 'cx' %}selected{% endif %}>CX</option>
                <option value="vipvoice" {% if system == 'vipvoice' %}selected{% endif %}>VIPVoice</option>
            </select>

            <input type="text" name="tag" value="{{ tag }}" placeholder="Tag">

            <label>From:</label>
            <input type="date" name="date_from" value="{{ date_from }}">

            <label>To:</label>
            <input type="date" name="date_to" value="{{ date_to }}">

            <input type="hidden" name="sort" value="{{ sort }}">
            <input type="hidden" name="order" value="{{ order }}">

            <button type="submit">Filter</button>
            <button type="button" onclick="window.location = window.location.href">Refresh</button>

            <label>
                <input type="checkbox" id="auto-refresh-toggle">
                Auto-Refresh
            </label>

            {% if q or system or tag or date_from or date_to %}
                <button type="button" onclick="window.location='/'">Clear</button>
            {% endif %}
        </div>
    </form>

    {% if top_tag_rows %}
        <h2>Top Tags</h2>
        <div class="top-tags">
            {% for tag_name, tag_count in top_tag_rows %}
                {% set tag_info = classify_tag(tag_name) %}
                <span class="top-tag {{ tag_info.css_class }}">
                    <a href="/?tag={{ tag_name }}">{{ tag_info.label }}</a>
                    <span class="top-tag-count">{{ tag_count }}</span>
                </span>
            {% endfor %}
        </div>
    {% endif %}

    <table>
        <thead>
            <tr>
                <th>
                    <a class="sort-link" href="{{ sort_url('id') }}">
                        ID
                        {% if sort == 'id' %}
                            <span class="sort-indicator">{{ '↑' if order == 'asc' else '↓' }}</span>
                        {% endif %}
                    </a>
                </th>
                <th>
                    <a class="sort-link" href="{{ sort_url('date') }}">
                        Created
                        {% if sort == 'date' %}
                            <span class="sort-indicator">{{ '↑' if order == 'asc' else '↓' }}</span>
                        {% endif %}
                    </a>
                </th>
                <th>Flag</th>
                <th>System</th>
                <th>File</th>
                <th>Status</th>
                <th>Transcript</th>
                <th>Playback</th>
                <th>Review</th>
                <th>
                    <a class="sort-link" href="{{ sort_url('priority') }}">
                        Priority
                        {% if sort == 'priority' %}
                            <span class="sort-indicator">{{ '↑' if order == 'asc' else '↓' }}</span>
                        {% endif %}
                    </a>
                </th>
                <th>Tags</th>
                <th>Summary</th>
            </tr>
        </thead>
        <tbody>
            {% for row in rows %}
            <tr>
                <td><a class="linkish" href="/call/{{ row['id'] }}">{{ row['id'] }}</a></td>
                <td>{{ format_created(row['call_time'] or row['discovered_at']) }}</td>
                <td class="flag">{% if row['flagged'] %}🚩{% endif %}</td>
                <td>
                    <a class="linkish" href="{{ filter_url(system=row['system']) }}">{{ row['system'] }}</a>
                </td>
                <td>
                    <a class="linkish" href="/call/{{ row['id'] }}">{{ display_filename(row['filename']) }}</a>
                </td>
                <td>{{ row['status'] }}</td>
                <td>{{ row['transcript_status'] }}</td>
                <td>{{ row['playback_status'] }}</td>
                <td>{{ row['review_status'] }}</td>
                <td>{{ row['priority_score'] }}</td>
                <td>
                    {% for one_tag in combined_tags(row) %}
                        {% set tag_info = classify_tag(one_tag) %}
                        <span class="tag {{ tag_info.css_class }}">
                            <a href="{{ filter_url(tag=one_tag) }}">{{ tag_info.label }}</a>
                        </span>
                    {% endfor %}
                </td>
                <td class="summary">
                    <div class="summary-short">
                        {{ truncate_summary(row['summary_text']) }}
                    </div>
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>

    <script>
        (function () {
            const key = "callreview_auto_refresh";
            const checkbox = document.getElementById("auto-refresh-toggle");
            if (!checkbox) return;

            const saved = window.localStorage.getItem(key);
            if (saved === "true") {
                checkbox.checked = true;
            }

            let intervalId = null;

            function startAutoRefresh() {
                if (intervalId) return;
                intervalId = window.setInterval(function () {
                    window.location.reload();
                }, 30000);
            }

            function stopAutoRefresh() {
                if (!intervalId) return;
                window.clearInterval(intervalId);
                intervalId = null;
            }

            checkbox.addEventListener("change", function () {
                window.localStorage.setItem(key, checkbox.checked ? "true" : "false");
                if (checkbox.checked) {
                    startAutoRefresh();
                } else {
                    stopAutoRefresh();
                }
            });

            if (checkbox.checked) {
                startAutoRefresh();
            }
        })();
    </script>
</body>
</html>
"""

DETAIL_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Call {{ row['id'] }}</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            max-width: 1200px;
        }
        pre {
            white-space: pre-wrap;
            background: #f7f7f7;
            padding: 12px;
            border: 1px solid #ddd;
        }
        .meta {
            margin-bottom: 20px;
        }
        .label {
            font-weight: bold;
        }
        .section {
            margin-top: 20px;
        }
        textarea {
            width: 100%;
            min-height: 120px;
            box-sizing: border-box;
        }
        .flag {
            font-size: 20px;
        }
        .muted {
            color: #666;
        }
        .warn {
            color: #a15c00;
        }
        .tag {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px 4px 2px 0;
            border: 1px solid #bbb;
            background-color: #fafafa;
        }
        .linkish {
            text-decoration: none;
            color: inherit;
        }
        .linkish:hover {
            text-decoration: underline;
        }

        .tag-property {
            background-color: #eaf3ff;
            border-color: #9ec5fe;
        }
        .tag-name {
            background-color: #f3e8ff;
            border-color: #c4b5fd;
        }
        .tag-sentiment-positive {
            background-color: #e8f7e8;
            border-color: #9bd39b;
        }
        .tag-sentiment-neutral {
            background-color: #f2f2f2;
            border-color: #cfcfcf;
        }
        .tag-sentiment-negative {
            background-color: #fdeaea;
            border-color: #f5a5a5;
        }
        .tag-reason-low {
            background-color: #fff7db;
            border-color: #e8cf77;
        }
        .tag-reason-medium {
            background-color: #e6f7f2;
            border-color: #8fd3c1;
        }
        .tag-reason-high {
            background-color: #ffe8cc;
            border-color: #f5b971;
        }
        .tag-reason-critical {
            background-color: #fde2e2;
            border-color: #ef9a9a;
        }
    </style>
</head>
<body>
    <p><a href="/">← Back</a></p>
    <h1>
        Call {{ row['id'] }}
        {% if row['flagged'] %}
            <span class="flag">🚩</span>
        {% endif %}
    </h1>

    <div class="meta">
        <p>
            <span class="label">System:</span>
            <a class="linkish" href="/?system={{ row['system'] }}">{{ row['system'] }}</a>
        </p>
        <p><span class="label">Filename:</span> {{ row['filename'] }}</p>
        <p><span class="label">Created:</span> {{ format_created(row['call_time'] or row['discovered_at']) }}</p>
        <p><span class="label">Current Path:</span> {{ row['current_path'] }}</p>
        <p><span class="label">Archive Path:</span> {{ row['archive_path'] or '' }}</p>
        <p><span class="label">Playback Path:</span> {{ row['playback_path'] or '' }}</p>
        <p><span class="label">Playback Status:</span> {{ row['playback_status'] }}</p>
        <p><span class="label">Playback Error:</span> {{ row['playback_error'] or '' }}</p>
        <p><span class="label">Status:</span> {{ row['status'] }}</p>
        <p><span class="label">Transcript Status:</span> {{ row['transcript_status'] }}</p>
        <p><span class="label">Priority:</span> {{ row['priority_score'] }}</p>
        <p>
            <span class="label">Tags:</span>
            {% if row['tags_csv'] %}
                {% for one_tag in row['tags_csv'].split(',') %}
                    {% set clean_tag = one_tag.strip() %}
                    {% if clean_tag %}
                        {% set tag_info = classify_tag(clean_tag) %}
                        <span class="tag {{ tag_info.css_class }}">{{ tag_info.label }}</span>
                    {% endif %}
                {% endfor %}
            {% endif %}
        </p>
        <div class="section">
            <h2>Manual Tags</h2>

            <form method="post" action="/call/{{ row['id'] }}/tags/add" style="margin-bottom: 12px;">
                <label for="existing_tag"><span class="label">Add Existing Tag:</span></label>
                <select id="existing_tag" name="existing_tag">
                    <option value="">-- Select a tag --</option>
                    {% for tag_name in all_tags %}
                        <option value="{{ tag_name }}">{{ classify_tag(tag_name).label }}</option>
                    {% endfor %}
                </select>
                <button type="submit">Add</button>
            </form>

            <form method="post" action="/call/{{ row['id'] }}/tags/add">
                <label for="new_tag_type"><span class="label">Create New Tag:</span></label>
                <select id="new_tag_type" name="new_tag_type">
                    <option value="reason">reason</option>
                    <option value="property">property</option>
                    <option value="name">name</option>
                    <option value="sentiment">sentiment</option>
                    <option value="manual">manual</option>
                </select>

                <input type="text" name="new_tag_value" placeholder="tag value">
                <button type="submit">Add New Tag</button>
            </form>

            {% set manual_tags = manual_tags_only(row) %}
            {% if manual_tags %}
                <p style="margin-top: 12px;">
                    <span class="label">Manual Tags:</span>
                    {% for one_tag in manual_tags %}
                        {% set tag_info = classify_tag(one_tag) %}
                        <span class="tag {{ tag_info.css_class }}">
                            {{ tag_info.label }}
                            <form method="post" action="/call/{{ row['id'] }}/tags/remove"
                                style="display:inline; margin-left: 4px;">
                                <input type="hidden" name="tag" value="{{ one_tag }}">
                                <button type="submit" style="font-size: 11px;">x</button>
                            </form>
                        </span>
                    {% endfor %}
                </p>
            {% endif %}
        </div>
        <p><span class="label">Review Status:</span> {{ row['review_status'] }}</p>
        <p><span class="label">Reviewed By:</span> {{ row['reviewed_by'] or '' }}</p>
        <p><span class="label">Error:</span> {{ row['error_message'] or '' }}</p>
    </div>

    <div class="section">
        <h2>Audio</h2>
        {% if playable %}
            <audio controls preload="metadata" style="width: 100%;">
                <source src="/audio/{{ row['id'] }}" type="{{ audio_mime }}">
                Your browser does not support the audio element.
            </audio>
            <p>
                <a href="/audio/{{ row['id'] }}" target="_blank">Open audio directly</a>
            </p>
            <p class="muted">Detected MIME: {{ audio_mime }}</p>
        {% else %}
            <p class="warn">No playable audio file is currently available.</p>
            {% if row['playback_error'] %}
                <p class="warn">{{ row['playback_error'] }}</p>
            {% endif %}
        {% endif %}
    </div>

    <div class="section">
        <h2>Review</h2>
        <form method="post" action="/call/{{ row['id'] }}/review">
            <label for="review_status"><span class="label">Review Status:</span></label>
            <select id="review_status" name="review_status">
                <option value="unreviewed" {% if row['review_status'] == 'unreviewed' %}selected{% endif %}>Unreviewed</option>
                <option value="reviewed" {% if row['review_status'] == 'reviewed' %}selected{% endif %}>Reviewed</option>
            </select>

            <p>
                <label>
                    <input type="checkbox" name="flagged" {% if row['flagged'] %}checked{% endif %}>
                    Flag as important
                </label>
            </p>

            <p>
                <label for="notes"><span class="label">Notes:</span></label><br>
                <textarea id="notes" name="notes">{{ row['notes'] or '' }}</textarea>
            </p>

            <button type="submit">Save Review</button>
        </form>
    </div>

    <div class="section">
        <h2>Summary</h2>
        <pre>{{ row['summary_text'] or '' }}</pre>
    </div>

    <div class="section">
        <h2>Transcript</h2>
        <pre>{{ row['transcript_text'] or '' }}</pre>
    </div>
</body>
</html>
"""


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
        parsed = Path  # dummy to satisfy static tools if needed
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
    app = Flask(__name__)
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

            return filter_url(sort=field, order=next_order)

        return {
            "filter_url": filter_url,
            "sort_url": sort_url,
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

        rows = search_calls(
            query=q,
            system=system,
            tag=tag,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
            order=order,
        )

        return render_template_string(
            INDEX_TEMPLATE,
            rows=rows,
            q=q,
            system=system or "",
            tag=tag or "",
            date_from=date_from or "",
            date_to=date_to or "",
            sort=sort,
            order=order,
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

        return render_template_string(
            DETAIL_TEMPLATE,
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
            normalized_value = new_tag_value.strip().lower()
            normalized_value = normalized_value.replace(" ", "_")
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