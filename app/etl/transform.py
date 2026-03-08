from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from sqlalchemy.orm import Session

from app.db.models import DailyCityMetrics
from app.etl.sql_utils import get_dialect_name, insert_for_model

logger = logging.getLogger("etl.transform")


@dataclass(frozen=True)
class DailyMetric:
    date: dt.date
    observation_count: int
    avg_temp: float
    min_temp: float
    max_temp: float
    sum_precip: float


def _parse_iso_datetime(value: str) -> dt.datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid hourly time value: {value!r}") from exc


def parse_hourly_to_daily(payload: dict[str, Any]) -> list[DailyMetric]:
    """Convert Open-Meteo 'hourly' arrays into daily aggregates."""
    hourly = payload.get("hourly") or {}
    times: list[str] = hourly.get("time") or []
    temps: list[float] = hourly.get("temperature_2m") or []
    precip: list[float] = hourly.get("precipitation") or []

    if not (len(times) == len(temps) == len(precip)):
        raise ValueError("hourly arrays length mismatch")

    buckets: dict[dt.date, dict[str, list[float]]] = {}
    for raw_time, raw_temp, raw_precip in zip(times, temps, precip, strict=True):
        day = _parse_iso_datetime(raw_time).date()
        try:
            temp = float(raw_temp)
            rain = float(raw_precip)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid hourly numeric values at time={raw_time!r}: "
                f"temperature={raw_temp!r}, precipitation={raw_precip!r}"
            ) from exc

        bucket = buckets.setdefault(day, {"temps": [], "precip": []})
        bucket["temps"].append(temp)
        bucket["precip"].append(rain)

    metrics: list[DailyMetric] = []
    for day, bucket in sorted(buckets.items(), key=lambda item: item[0]):
        day_temps = bucket["temps"]
        day_precip = bucket["precip"]
        if not day_temps:
            continue
        metrics.append(
            DailyMetric(
                date=day,
                observation_count=len(day_temps),
                avg_temp=sum(day_temps) / len(day_temps),
                min_temp=min(day_temps),
                max_temp=max(day_temps),
                sum_precip=sum(day_precip),
            )
        )
    return metrics


def upsert_daily_metrics(
    session: Session,
    *,
    city_id: int,
    metrics: Iterable[DailyMetric],
) -> int:
    """Upsert metrics into mart. Returns number of rows affected (best-effort)."""
    dialect_name = get_dialect_name(session)
    affected = 0

    for metric in metrics:
        stmt = insert_for_model(dialect_name, DailyCityMetrics).values(
            city_id=city_id,
            date=metric.date,
            avg_temp=metric.avg_temp,
            min_temp=metric.min_temp,
            max_temp=metric.max_temp,
            sum_precip=metric.sum_precip,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["city_id", "date"],
            set_={
                "avg_temp": metric.avg_temp,
                "min_temp": metric.min_temp,
                "max_temp": metric.max_temp,
                "sum_precip": metric.sum_precip,
            },
        )
        result = session.execute(stmt)
        affected += result.rowcount or 0

    return affected
