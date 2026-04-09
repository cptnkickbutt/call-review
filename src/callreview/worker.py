from __future__ import annotations

import time

from callreview.config import settings
from callreview.db import claim_next_call, init_db, reset_stale_processing_calls
from callreview.ingest import queue_stable_new_calls, register_discoveries
from callreview.processing import process_call_row


def run_worker() -> None:
    init_db()

    recovered = reset_stale_processing_calls(settings.worker_stale_processing_minutes)

    cycle = 0

    print("Worker started.")
    print(f"Scan interval: {settings.worker_scan_interval}s")
    print(f"Dry run: {settings.dry_run}")
    print(f"Transcription model: {settings.transcription_model}")
    print(f"Recovered stale processing calls: {recovered}")

    while True:
        cycle += 1

        inserted = register_discoveries()
        queued = queue_stable_new_calls()

        if inserted:
            print(f"[cycle {cycle}] registered {inserted} new file(s)")
        if queued:
            print(f"[cycle {cycle}] queued {queued} stable file(s)")

        prefer_backlog = (
            cycle % max(settings.worker_backlog_every, 1) == 0
        )

        row = claim_next_call(prefer_backlog=prefer_backlog)

        if row is not None:
            print(
                f"[cycle {cycle}] processing id={row['id']} "
                f"system={row['system']} file={row['filename']}"
            )
            process_call_row(row)
        else:
            print(f"[cycle {cycle}] nothing ready")

        time.sleep(settings.worker_scan_interval)