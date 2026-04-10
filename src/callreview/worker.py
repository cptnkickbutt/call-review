from __future__ import annotations

import time

from callreview.config import settings
from callreview.db import claim_next_call, init_db, reset_stale_processing_calls
from callreview.ingest import queue_stable_new_calls, register_discoveries
from callreview.logging_utils import setup_logging
from callreview.processing import process_call_row

logger = setup_logging(
    name="callreview.worker",
    log_dir=settings.log_dir,
    log_filename=settings.log_file,
    level=settings.log_level,
)


def run_worker() -> None:
    init_db()

    recovered = reset_stale_processing_calls(settings.worker_stale_processing_minutes)

    cycle = 0

    logger.info("Worker started.")
    logger.info("Scan interval: %ss", settings.worker_scan_interval)
    logger.info("Dry run: %s", settings.dry_run)
    logger.info("Transcription model: %s", settings.transcription_model)
    logger.info("Recovered stale processing calls: %s", recovered)

    while True:
        cycle += 1

        try:
            inserted = register_discoveries()
            queued = queue_stable_new_calls()

            if inserted:
                logger.info("[cycle %s] registered %s new file(s)", cycle, inserted)
            if queued:
                logger.info("[cycle %s] queued %s stable file(s)", cycle, queued)

            prefer_backlog = (
                cycle % max(settings.worker_backlog_every, 1) == 0
            )

            row = claim_next_call(prefer_backlog=prefer_backlog)

            if row is not None:
                logger.info(
                    "[cycle %s] processing id=%s system=%s file=%s",
                    cycle,
                    row["id"],
                    row["system"],
                    row["filename"],
                )
                process_call_row(row)
            else:
                logger.info("[cycle %s] nothing ready", cycle)

        except Exception:
            logger.exception("[cycle %s] unhandled worker error", cycle)

        time.sleep(settings.worker_scan_interval)