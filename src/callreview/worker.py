from __future__ import annotations

import time
import traceback

from callreview.config import settings
from callreview.db import claim_next_call, init_db
from callreview.ingest import queue_stable_new_calls, register_discoveries_locked
from callreview.processing import process_call_row


def _prefix() -> str:
    return f"[worker {settings.worker_instance}]"


def run_worker() -> None:
    init_db()
    cycle = 0

    print(f"{_prefix()} started")
    print(f"{_prefix()} scan interval: {settings.worker_scan_interval}s")
    print(f"{_prefix()} dry run: {settings.dry_run}")
    print(f"{_prefix()} discovery enabled: {settings.worker_discovery_enabled}")

    while True:
        cycle += 1

        try:
            inserted = 0
            compat_queued = 0

            if settings.worker_discovery_enabled:
                inserted = register_discoveries_locked()
                compat_queued = queue_stable_new_calls()

            if inserted:
                print(f"{_prefix()} [cycle {cycle}] registered {inserted} canonical file(s)")
            if compat_queued:
                print(f"{_prefix()} [cycle {cycle}] re-queued {compat_queued} legacy row(s)")

            row = claim_next_call(include_failed=True)

            if row is not None:
                print(
                    f"{_prefix()} [cycle {cycle}] processing "
                    f"id={row['id']} system={row['system']} file={row['filename']}"
                )
                process_call_row(row)
            else:
                print(f"{_prefix()} [cycle {cycle}] nothing ready")

        except Exception as exc:
            print(f"{_prefix()} [cycle {cycle}] unhandled worker error: {exc}")
            print(traceback.format_exc())

        time.sleep(settings.worker_scan_interval)


if __name__ == "__main__":
    run_worker()