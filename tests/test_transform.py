import datetime as dt

import pytest
from sqlalchemy import select

from app.db.models import DailyCityMetrics
from app.etl.transform import DailyMetric, parse_hourly_to_daily, upsert_daily_metrics


def test_parse_hourly_to_daily_aggregates():
    payload = {
        "hourly": {
            "time": [
                "2026-02-25T00:00",
                "2026-02-25T01:00",
                "2026-02-25T02:00",
                "2026-02-26T00:00",
            ],
            "temperature_2m": [0, 2, 4, 10],
            "precipitation": [0.1, 0.0, 0.2, 1.0],
        }
    }

    metrics = parse_hourly_to_daily(payload)
    assert len(metrics) == 2

    day1 = metrics[0]
    assert day1.date == dt.date(2026, 2, 25)
    assert day1.observation_count == 3
    assert day1.min_temp == 0.0
    assert day1.max_temp == 4.0
    assert day1.avg_temp == pytest.approx((0 + 2 + 4) / 3)
    assert day1.sum_precip == pytest.approx(0.3)


def test_parse_hourly_to_daily_supports_z_suffix():
    payload = {
        "hourly": {
            "time": ["2026-02-25T00:00Z", "2026-02-25T01:00Z"],
            "temperature_2m": [1.0, 3.0],
            "precipitation": [0.0, 0.1],
        }
    }

    metrics = parse_hourly_to_daily(payload)
    assert metrics[0].date == dt.date(2026, 2, 25)
    assert metrics[0].observation_count == 2
    assert metrics[0].avg_temp == pytest.approx(2.0)


def test_parse_hourly_to_daily_raises_on_mismatch():
    payload = {
        "hourly": {
            "time": ["2026-02-25T00:00"],
            "temperature_2m": [1.0, 3.0],
            "precipitation": [0.0],
        }
    }

    with pytest.raises(ValueError, match="length mismatch"):
        parse_hourly_to_daily(payload)


def test_upsert_daily_metrics_updates_existing_row(session):
    day = dt.date(2026, 2, 25)
    first_metric = DailyMetric(
        date=day,
        observation_count=24,
        avg_temp=1.5,
        min_temp=1.0,
        max_temp=2.0,
        sum_precip=0.2,
    )
    second_metric = DailyMetric(
        date=day,
        observation_count=24,
        avg_temp=3.0,
        min_temp=2.0,
        max_temp=4.0,
        sum_precip=1.5,
    )

    affected_1 = upsert_daily_metrics(session, city_id=1, metrics=[first_metric])
    affected_2 = upsert_daily_metrics(session, city_id=1, metrics=[second_metric])
    session.commit()

    assert affected_1 == 1
    assert affected_2 == 1

    rows = session.execute(select(DailyCityMetrics)).scalars().all()
    assert len(rows) == 1
    row = rows[0]
    assert row.avg_temp == pytest.approx(3.0)
    assert row.min_temp == pytest.approx(2.0)
    assert row.max_temp == pytest.approx(4.0)
    assert row.sum_precip == pytest.approx(1.5)
