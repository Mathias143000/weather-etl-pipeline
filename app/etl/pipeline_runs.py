from __future__ import annotations

import datetime as dt

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import PipelineRun


def start_pipeline_run(
    session: Session,
    *,
    source: str,
    started_at: dt.datetime,
    target_dates: list[dt.date],
) -> PipelineRun:
    run = PipelineRun(
        source=source,
        started_at=started_at,
        status=PipelineRun.Status.FAILED,
        target_window_start=target_dates[0] if target_dates else None,
        target_window_end=target_dates[-1] if target_dates else None,
        details={},
    )
    session.add(run)
    session.flush()
    return run


def finish_pipeline_run(
    session: Session,
    *,
    run: PipelineRun,
    finished_at: dt.datetime,
    status: str,
    raw_inserted: int,
    raw_duplicates: int,
    raw_failed: int,
    mart_rows_upserted: int,
    mart_failed: int,
    dq_warnings: int,
    details: dict[str, object],
    last_error: str | None = None,
) -> PipelineRun:
    run.finished_at = finished_at
    run.status = status
    run.raw_inserted = raw_inserted
    run.raw_duplicates = raw_duplicates
    run.raw_failed = raw_failed
    run.mart_rows_upserted = mart_rows_upserted
    run.mart_failed = mart_failed
    run.dq_warnings = dq_warnings
    run.details = details
    run.last_error = last_error
    session.flush()
    return run


def get_recent_pipeline_runs(session: Session, *, limit: int = 5) -> list[PipelineRun]:
    stmt = (
        select(PipelineRun)
        .order_by(PipelineRun.started_at.desc(), PipelineRun.id.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt).all())
