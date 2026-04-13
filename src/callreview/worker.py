from __future__ import annotations

import time
import traceback

from callreview.config import settings
from callreview.db import claim_next_call, init_db
from callreview.ingest import queue_stable_new_calls, register_discoveries_locked
from callreview.logging_utils import setup_logging
from callreview.processing import process_call_row

logger = setup_logging("callreview.worker")


def _prefix() -> str:
    return f"[worker {settings.worker_instance}]"


def run_worker() -> None:
    init_db()
    cycle = 0

    logger.info("%s started", _prefix())
    logger.info("%s scan interval: %ss", _prefix(), settings.worker_scan_interval)
    logger.info("%s dry run: %s", _prefix(), settings.dry_run)
    logger.info("%s discovery enabled: %s", _prefix(), settings.worker_discovery_enabled)

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

            row = claim_next_call(include_failed=True)

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

        except Exception as exc:
            logger.exception(
                "%s [cycle %s] unhandled worker error: %s",
                _prefix(),
                cycle,
                exc,
            )

        time.sleep(settings.worker_scan_interval)


if __name__ == "__main__":
    run_worker()