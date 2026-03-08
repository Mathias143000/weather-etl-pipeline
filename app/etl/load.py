from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import WeatherRaw
from app.etl.sql_utils import get_dialect_name, insert_for_model

logger = logging.getLogger("etl.load")


def load_raw_event(
    session: Session,
    *,
    source: str,
    city_id: int,
    fetched_at: dt.datetime,
    payload: dict[str, Any],
) -> bool:
    """Insert RAW event with dedup.

    Returns True if inserted, False if skipped as duplicate.
    Dedup key: (source, city_id, fetched_date).
    """
    fetched_date = fetched_at.date()
    dialect_name = get_dialect_name(session)

    stmt = insert_for_model(dialect_name, WeatherRaw).values(
        source=source,
        city_id=city_id,
        fetched_at=fetched_at,
        fetched_date=fetched_date,
        payload=payload,
    )

    # ON CONFLICT DO NOTHING (portable for sqlite/postgres).
    stmt = stmt.on_conflict_do_nothing(index_elements=["source", "city_id", "fetched_date"])

    res = session.execute(stmt)
    inserted = (res.rowcount or 0) > 0
    return inserted


def get_latest_raw_for_date(
    session: Session,
    *,
    source: str,
    city_id: int,
    date: dt.date,
) -> WeatherRaw | None:
    query = (
        select(WeatherRaw)
        .where(
            WeatherRaw.source == source,
            WeatherRaw.city_id == city_id,
            WeatherRaw.fetched_date == date,
        )
        .order_by(WeatherRaw.fetched_at.desc())
        .limit(1)
    )
    return session.execute(query).scalars().first()


def get_latest_raw_snapshot(
    session: Session,
    *,
    source: str,
    city_id: int,
) -> WeatherRaw | None:
    query = (
        select(WeatherRaw)
        .where(
            WeatherRaw.source == source,
            WeatherRaw.city_id == city_id,
        )
        .order_by(WeatherRaw.fetched_at.desc(), WeatherRaw.id.desc())
        .limit(1)
    )
    return session.execute(query).scalars().first()
