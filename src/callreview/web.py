from __future__ import annotations

from flask import Flask, abort, render_template_string, request

from callreview.db import get_call_by_id, init_db, list_calls, search_calls


INDEX_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Call Review</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 24px; }
        h1 { margin-bottom: 8px; }
        form { margin-bottom: 16px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ccc; padding: 8px; vertical-align: top; }
        th { background: #f5f5f5; text-align: left; }
        .badge { display: inline-block; padding: 2px 8px; border: 1px solid #999; border-radius: 10px; font-size: 12px; }
        .muted { color: #666; }
        .summary { max-width: 500px; }
    </style>
</head>
<body>
    <h1>Call Review</h1>
    <p class="muted">Starter UI. LDAP/Apache auth can be added later in front of this app.</p>

    <form method="get" action="/">
        <input type="text" name="q" value="{{ q }}" placeholder="Search filename, transcript, tags..." size="50">
        <select name="system">
            <option value="">All systems</option>
            <option value="cx" {% if system == 'cx' %}selected{% endif %}>CX</option>
            <option value="vipvoice" {% if system == 'vipvoice' %}selected{% endif %}>VIPVoice</option>
        </select>
        <button type="submit">Search</button>
    </form>

    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>System</th>
                <th>File</th>
                <th>Status</th>
                <th>Transcript</th>
                <th>Priority</th>
                <th>Tags</th>
                <th>Summary</th>
            </tr>
        </thead>
        <tbody>
            {% for row in rows %}
            <tr>
                <td><a href="/call/{{ row['id'] }}">{{ row['id'] }}</a></td>
                <td><span class="badge">{{ row['system'] }}</span></td>
                <td>{{ row['filename'] }}</td>
                <td>{{ row['status'] }}</td>
                <td>{{ row['transcript_status'] }}</td>
                <td>{{ row['priority_score'] }}</td>
                <td>{{ row['tags_csv'] or '' }}</td>
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
        body { font-family: Arial, sans-serif; margin: 24px; max-width: 1200px; }
        pre { white-space: pre-wrap; background: #f7f7f7; padding: 12px; border: 1px solid #ddd; }
        .meta { margin-bottom: 20px; }
        .label { font-weight: bold; }
        .section { margin-top: 20px; }
    </style>
</head>
<body>
    <p><a href="/">← Back</a></p>
    <h1>Call {{ row['id'] }}</h1>

    <div class="meta">
        <p><span class="label">System:</span> {{ row['system'] }}</p>
        <p><span class="label">Filename:</span> {{ row['filename'] }}</p>
        <p><span class="label">Current Path:</span> {{ row['current_path'] }}</p>
        <p><span class="label">Archive Path:</span> {{ row['archive_path'] or '' }}</p>
        <p><span class="label">Status:</span> {{ row['status'] }}</p>
        <p><span class="label">Transcript Status:</span> {{ row['transcript_status'] }}</p>
        <p><span class="label">Priority:</span> {{ row['priority_score'] }}</p>
        <p><span class="label">Tags:</span> {{ row['tags_csv'] or '' }}</p>
        <p><span class="label">Review Status:</span> {{ row['review_status'] }}</p>
        <p><span class="label">Error:</span> {{ row['error_message'] or '' }}</p>
    </div>

    <div class="section">
        <h2>Audio</h2>
        <p>This starter does not yet expose streamed audio through Flask.</p>
        <p>Later options:</p>
        <ul>
            <li>Serve audio via Flask route</li>
            <li>Serve audio directly through Apache</li>
            <li>Protect audio behind LDAP-authenticated reverse proxy</li>
        </ul>
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


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()

    @app.route("/")
    def index():
        q = request.args.get("q", "").strip()
        system = request.args.get("system", "").strip() or None

        if q:
            rows = search_calls(q, system=system)
        else:
            rows = list_calls()

        return render_template_string(INDEX_TEMPLATE, rows=rows, q=q, system=system or "")

    @app.route("/call/<int:call_id>")
    def call_detail(call_id: int):
        row = get_call_by_id(call_id)
        if row is None:
            abort(404)
        return render_template_string(DETAIL_TEMPLATE, row=row)

    return app