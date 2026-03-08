from __future__ import annotations

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from app.etl.config import Settings
from app.etl.logging_config import setup_logging
from app.etl.run import run_once

logger = logging.getLogger("etl.scheduler")


def _run_job() -> None:
    exit_code = run_once(configure_logging=False)
    if exit_code != 0:
        logger.warning("Scheduled ETL run exited with code=%s", exit_code)


def main() -> None:
    settings = Settings()
    setup_logging(settings.log_level)

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        _run_job,
        "interval",
        minutes=settings.schedule_every_minutes,
        id="etl_run_once",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    logger.info("Starting scheduler: every %s minutes", settings.schedule_every_minutes)
    scheduler.start()

    # Run once on startup (nice for demo).
    _run_job()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Stopping scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
