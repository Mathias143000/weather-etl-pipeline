import datetime as dt

from sqlalchemy import select

from app.db.models import WeatherRaw
from app.etl.load import load_raw_event


def test_dedup_insert_same_day(session):
    now = dt.datetime(2026, 2, 25, 12, 0, tzinfo=dt.timezone.utc)
    payload = {"hourly": {"time": [], "temperature_2m": [], "precipitation": []}}

    inserted1 = load_raw_event(
        session,
        source="open_meteo",
        city_id=1,
        fetched_at=now,
        payload=payload,
    )
    inserted2 = load_raw_event(
        session,
        source="open_meteo",
        city_id=1,
        fetched_at=now,
        payload=payload,
    )
    session.commit()

    assert inserted1 is True
    assert inserted2 is False

    rows = session.execute(select(WeatherRaw)).scalars().all()
    assert len(rows) == 1
