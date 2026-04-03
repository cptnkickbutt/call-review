from __future__ import annotations

import mimetypes
from pathlib import Path

from flask import Flask, abort, redirect, render_template_string, request, send_file, url_for

from callreview.db import (
    get_call_by_id,
    init_db,
    list_calls,
    search_calls,
    top_tags,
    update_review,
)


INDEX_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Call Review</title>
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
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border: 1px solid #888;
            border-radius: 12px;
            font-size: 12px;
            margin-right: 4px;
            background-color: #f5f5f5;
        }
        .linkish {
            color: inherit;
            text-decoration: none;
        }

        .linkish:hover {
            text-decoration: underline;
        }
        .tag {
            display: inline-block;
            padding: 3px 8px;
            border: 1px solid #bbb;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px 4px 2px 0;
            background-color: #fafafa;
        }
        .tag a,
        .top-tag a {
            text-decoration: none;
            color: inherit;
        }
        .muted {
            color: #666;
        }
        .summary {
            max-width: 500px;
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
    </style>
</head>
<body>
    <h1>Call Review</h1>
    <p class="muted">Starter UI. LDAP/Apache auth can be added later in front of this app.</p>

    <form method="get" action="/">
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

        <button type="submit">Filter</button>

        {% if q or system or tag or date_from or date_to %}
            <button type="button" onclick="window.location='/'">Clear</button>
        {% endif %}
    </form>

    {% if top_tag_rows %}
        <h2>Top Tags</h2>
        <div class="top-tags">
            {% for tag_name, tag_count in top_tag_rows %}
                <span class="top-tag">
                    <a href="/?tag={{ tag_name }}">{{ tag_name }}</a>
                    <span class="top-tag-count">{{ tag_count }}</span>
                </span>
            {% endfor %}
        </div>
    {% endif %}

    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Date</th>
                <th>Flag</th>
                <th>System</th>
                <th>File</th>
                <th>Status</th>
                <th>Transcript</th>
                <th>Playback</th>
                <th>Review</th>
                <th>Priority</th>
                <th>Tags</th>
                <th>Summary</th>
            </tr>
        </thead>
        <tbody>
            {% for row in rows %}
            <tr>
                <td><a class="linkish" href="/call/{{ row['id'] }}">{{ row['id'] }}</a></td>
                <td>{{ (row['call_time'] or row['discovered_at'] or '')[:10] }}</td>
                <td class="flag">{% if row['flagged'] %}🚩{% endif %}</td>
                <td><a class="linkish" href="/?system={{ row['system'] }}"><span class="badge">{{ row['system'] }}</span></a></td>
                <td><a class="linkish" href="/call/{{ row['id'] }}">{{ row['filename'] }}</a></td>
                <td>{{ row['status'] }}</td>
                <td>{{ row['transcript_status'] }}</td>
                <td>{{ row['playback_status'] }}</td>
                <td>{{ row['review_status'] }}</td>
                <td>{{ row['priority_score'] }}</td>
                <td>
                    {% if row['tags_csv'] %}
                        {% for one_tag in row['tags_csv'].split(',') %}
                            {% set clean_tag = one_tag.strip() %}
                            {% if clean_tag %}
                                <span class="tag">
                                    <a href="/?tag={{ clean_tag }}">{{ clean_tag }}</a>
                                </span>
                            {% endif %}
                        {% endfor %}
                    {% endif %}
                </td>
                <td class="summary">{{ row['summary_text'] or '' }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
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
            border: 1px solid #bbb;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px 4px 2px 0;
            background-color: #fafafa;
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
        <p><span class="label">System:</span> {{ row['system'] }}</p>
        <p><span class="label">Filename:</span> {{ row['filename'] }}</p>
        <p><span class="label">Call Time:</span> {{ row['call_time'] or '' }}</p>
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
                        <span class="tag">{{ clean_tag }}</span>
                    {% endif %}
                {% endfor %}
            {% endif %}
        </p>
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


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()

    @app.route("/")
    def index():
        q = request.args.get("q", "").strip()
        system = request.args.get("system", "").strip() or None
        tag = request.args.get("tag", "").strip() or None
        date_from = request.args.get("date_from", "").strip() or None
        date_to = request.args.get("date_to", "").strip() or None

        rows = search_calls(
            query=q,
            system=system,
            tag=tag,
            date_from=date_from,
            date_to=date_to,
        )

        return render_template_string(
            INDEX_TEMPLATE,
            rows=rows,
            q=q,
            system=system or "",
            tag=tag or "",
            date_from=date_from or "",
            date_to=date_to or "",
            top_tag_rows=top_tags(20),
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

    return app