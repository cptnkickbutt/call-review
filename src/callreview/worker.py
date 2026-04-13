from __future__ import annotations

import time

from callreview.config import settings
from callreview.db import (
    claim_newest_queued_call,
    claim_oldest_backlog_call,
    init_db,
    reset_interrupted_processing_calls,
)
from callreview.ingest import queue_stable_new_calls, register_discoveries_locked
from callreview.logging_utils import setup_logging
from callreview.processing import process_call_row

logger = setup_logging("callreview.worker")


def _prefix() -> str:
    return f"[worker {settings.worker_instance}]"


def pick_next_call(cycle_number: int):
    backlog_every = max(settings.worker_backlog_every, 1)

    if cycle_number % backlog_every == 0:
        row = claim_oldest_backlog_call()
        if row is not None:
            return row

    return claim_newest_queued_call()


def run_worker() -> None:
    init_db()
    cycle = 0

    reset_count = 0
    if settings.worker_instance == "1":
        reset_count = reset_interrupted_processing_calls(
            settings.worker_stale_processing_minutes
        )

    logger.info("%s started", _prefix())
    logger.info("%s scan interval: %ss", _prefix(), settings.worker_scan_interval)
    logger.info("%s dry run: %s", _prefix(), settings.dry_run)
    logger.info("%s discovery enabled: %s", _prefix(), settings.worker_discovery_enabled)
    
    logger.info(
        "%s stale processing threshold: %sm",
        _prefix(),
        settings.worker_stale_processing_minutes,
    )
    if reset_count:
        logger.info("%s reset %s interrupted processing call(s)", _prefix(), reset_count)

    while True:
        cycle += 1

        try:
            inserted = 0
            compat_queued = 0

            if settings.worker_discovery_enabled:
                inserted = register_discoveries_locked()
                compat_queued = queue_stable_new_calls()

            if inserted:
                logger.info(
                    "%s [cycle %s] registered %s canonical file(s)",
                    _prefix(),
                    cycle,
                    inserted,
                )
            if compat_queued:
                logger.info(
                    "%s [cycle %s] re-queued %s legacy row(s)",
                    _prefix(),
                    cycle,
                    compat_queued,
                )

            row = pick_next_call(cycle)

            if row is not None:
                logger.info(
                    "%s [cycle %s] processing id=%s system=%s file=%s",
                    _prefix(),
                    cycle,
                    row["id"],
                    row["system"],
                    row["filename"],
                )
                process_call_row(row)
            else:
                logger.info("%s [cycle %s] nothing ready", _prefix(), cycle)

        except Exception:
            logger.exception("%s [cycle %s] unhandled worker error", _prefix(), cycle)

        time.sleep(settings.worker_scan_interval)


if __name__ == "__main__":
    run_worker()