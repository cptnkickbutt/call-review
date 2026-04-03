from __future__ import annotations

import time

from callreview.config import settings
from callreview.db import get_oldest_backlog_call, init_db, list_ready_new_calls
from callreview.ingest import queue_stable_new_calls, register_discoveries
from callreview.processing import process_call_row


def pick_next_call(cycle_number: int):
    ready = list_ready_new_calls(limit=10)

    if ready:
        if cycle_number % max(settings.worker_backlog_every, 1) == 0:
            backlog = get_oldest_backlog_call()
            if backlog is not None:
                return backlog
        return ready[0]

    return get_oldest_backlog_call()


def run_worker() -> None:
    init_db()
    cycle = 0

    print("Worker started.")
    print(f"Scan interval: {settings.worker_scan_interval}s")
    print(f"Dry run: {settings.dry_run}")

    while True:
        cycle += 1

        inserted = register_discoveries()
        queued = queue_stable_new_calls()

        if inserted:
            print(f"[cycle {cycle}] registered {inserted} new file(s)")
        if queued:
            print(f"[cycle {cycle}] queued {queued} stable file(s)")

        row = pick_next_call(cycle)
        if row is not None:
            print(
                f"[cycle {cycle}] processing id={row['id']} "
                f"system={row['system']} file={row['filename']}"
            )
            process_call_row(row)
        else:
            print(f"[cycle {cycle}] nothing ready")

        time.sleep(settings.worker_scan_interval)