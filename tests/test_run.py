import datetime as dt

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.models import City, DailyCityMetrics, PipelineRun, WeatherRaw
from app.etl.run import run_once


def _setup_db(db_path):
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            City(
                id=1,
                name="Tallinn",
                latitude=59.4370,
                longitude=24.7536,
                timezone="UTC",
            )
        )
        session.commit()
    return engine


def _build_payload(*, day: dt.date, hours_for_day: int, include_today_preview: bool = True) -> dict:
    times = [f"{day.isoformat()}T{hour:02d}:00" for hour in range(hours_for_day)]
    temps = [float(hour) for hour in range(hours_for_day)]
    precip = [0.1 for _ in range(hours_for_day)]

    if include_today_preview:
        today = day + dt.timedelta(days=1)
        for hour in range(3):
            times.append(f"{today.isoformat()}T{hour:02d}:00")
            temps.append(10.0 + hour)
            precip.append(0.0)

    return {
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "precipitation": precip,
        }
    }


def test_run_once_persists_successful_pipeline_run(monkeypatch, tmp_path):
    db_path = tmp_path / "etl_success.db"
    engine = _setup_db(db_path)
    yesterday = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)

    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("SOURCE_NAME", "open_meteo")
    monkeypatch.setenv("OPEN_METEO_BASE_URL", "https://example.test")
    monkeypatch.setenv("TRANSFORM_CLOSED_DAYS_BACK", "1")
    monkeypatch.setenv("MIN_HOURS_PER_COMPLETE_DAY", "24")
    monkeypatch.setattr(
        "app.etl.run.fetch_weather_with_window",
        lambda *args, **kwargs: _build_payload(day=yesterday, hours_for_day=24),
    )

    assert run_once(configure_logging=False) == 0
    assert run_once(configure_logging=False) == 0

    with Session(engine) as session:
        raw_rows = session.execute(select(WeatherRaw)).scalars().all()
        mart_rows = session.execute(select(DailyCityMetrics)).scalars().all()
        runs = session.execute(select(PipelineRun).order_by(PipelineRun.id)).scalars().all()

    assert len(raw_rows) == 1
    assert len(mart_rows) == 1
    assert mart_rows[0].date == yesterday

    assert len(runs) == 2
    assert runs[0].status == PipelineRun.Status.SUCCESS
    assert runs[0].raw_inserted == 1
    assert runs[0].raw_duplicates == 0
    assert runs[0].mart_rows_upserted == 1
    assert runs[0].dq_warnings == 0
    assert runs[0].target_window_start == yesterday
    assert runs[0].target_window_end == yesterday

    assert runs[1].status == PipelineRun.Status.SUCCESS
    assert runs[1].raw_inserted == 0
    assert runs[1].raw_duplicates == 1
    assert runs[1].mart_rows_upserted == 1


def test_run_once_marks_partial_success_when_day_is_incomplete(monkeypatch, tmp_path):
    db_path = tmp_path / "etl_incomplete.db"
    engine = _setup_db(db_path)
    yesterday = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)

    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("SOURCE_NAME", "open_meteo")
    monkeypatch.setenv("OPEN_METEO_BASE_URL", "https://example.test")
    monkeypatch.setenv("TRANSFORM_CLOSED_DAYS_BACK", "1")
    monkeypatch.setenv("MIN_HOURS_PER_COMPLETE_DAY", "24")
    monkeypatch.setattr(
        "app.etl.run.fetch_weather_with_window",
        lambda *args, **kwargs: _build_payload(day=yesterday, hours_for_day=12),
    )

    assert run_once(configure_logging=False) == 1

    with Session(engine) as session:
        mart_rows = session.execute(select(DailyCityMetrics)).scalars().all()
        run = session.execute(select(PipelineRun)).scalar_one()

    assert mart_rows == []
    assert run.status == PipelineRun.Status.PARTIAL_SUCCESS
    assert run.dq_warnings == 1
    assert run.raw_inserted == 1
    assert run.mart_rows_upserted == 0
