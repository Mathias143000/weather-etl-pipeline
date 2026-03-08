import datetime as dt

from app.etl.extract import CityDTO
from app.etl.quality import evaluate_daily_metrics
from app.etl.transform import DailyMetric


def test_evaluate_daily_metrics_warns_on_missing_day():
    city = CityDTO(id=1, name="TestCity", latitude=10.0, longitude=20.0, timezone="UTC")
    target_date = dt.date(2026, 2, 25)

    warnings = evaluate_daily_metrics(
        city=city,
        metrics=[],
        target_dates=[target_date],
        min_hours_per_complete_day=24,
    )

    assert len(warnings) == 1
    assert warnings[0].code == "missing_day"
    assert warnings[0].date == target_date


def test_evaluate_daily_metrics_warns_on_incomplete_day():
    city = CityDTO(id=1, name="TestCity", latitude=10.0, longitude=20.0, timezone="UTC")
    target_date = dt.date(2026, 2, 25)
    metrics = [
        DailyMetric(
            date=target_date,
            observation_count=12,
            avg_temp=2.0,
            min_temp=1.0,
            max_temp=4.0,
            sum_precip=0.3,
        )
    ]

    warnings = evaluate_daily_metrics(
        city=city,
        metrics=metrics,
        target_dates=[target_date],
        min_hours_per_complete_day=24,
    )

    assert len(warnings) == 1
    assert warnings[0].code == "incomplete_day"
    assert warnings[0].date == target_date
