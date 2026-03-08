import datetime as dt

from app.etl.transform import parse_hourly_to_daily


def test_parsing_returns_normalized_fields():
    payload = {
        "hourly": {
            "time": ["2026-02-25T00:00", "2026-02-25T01:00"],
            "temperature_2m": [1.5, 2.5],
            "precipitation": [0.0, 0.2],
        }
    }

    metrics = parse_hourly_to_daily(payload)
    assert metrics[0].date == dt.date(2026, 2, 25)
    assert metrics[0].observation_count == 2
    assert isinstance(metrics[0].avg_temp, float)
    assert isinstance(metrics[0].sum_precip, float)
