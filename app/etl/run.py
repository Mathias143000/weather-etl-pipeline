from __future__ import annotations

import datetime as dt
import logging
import sys
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import City, PipelineRun
from app.db.session import create_db_engine
from app.etl.config import Settings, load_cities_from_yaml
from app.etl.extract import CityDTO, fetch_weather_with_window
from app.etl.load import get_latest_raw_snapshot, load_raw_event
from app.etl.logging_config import setup_logging
from app.etl.pipeline_runs import finish_pipeline_run, start_pipeline_run
from app.etl.quality import QualityWarning, evaluate_daily_metrics
from app.etl.transform import DailyMetric, parse_hourly_to_daily, upsert_daily_metrics

logger = logging.getLogger("etl.run")


@dataclass(frozen=True)
class RawStageResult:
    inserted: int = 0
    duplicates: int = 0
    failed: int = 0
    city_results: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class TransformStageResult:
    rows_upserted: int = 0
    failed: int = 0
    dq_warning_count: int = 0
    city_results: list[dict[str, object]] = field(default_factory=list)
    dq_warnings: list[dict[str, object]] = field(default_factory=list)


def _city_to_dto(city: City) -> CityDTO:
    return CityDTO(
        id=city.id,
        name=city.name,
        latitude=city.latitude,
        longitude=city.longitude,
        timezone=city.timezone,
    )


def _get_cities(session: Session) -> list[CityDTO]:
    cities = session.scalars(select(City).order_by(City.id)).all()
    return [_city_to_dto(city) for city in cities]


def _build_target_dates(now_utc: dt.datetime, *, closed_days_back: int) -> list[dt.date]:
    window_end = now_utc.date() - dt.timedelta(days=1)
    window_start = window_end - dt.timedelta(days=closed_days_back - 1)
    return [window_start + dt.timedelta(days=offset) for offset in range(closed_days_back)]


def _sync_cities_from_yaml(session: Session, yaml_path: str) -> None:
    yaml_cities = load_cities_from_yaml(yaml_path)
    if not yaml_cities:
        logger.warning("CITIES_YAML is set but contains no cities: path=%s", yaml_path)
        return

    existing = {city.name: city for city in session.scalars(select(City)).all()}
    created = 0
    updated = 0

    for item in yaml_cities:
        city = existing.get(item.name)
        if city is None:
            session.add(
                City(
                    name=item.name,
                    latitude=item.latitude,
                    longitude=item.longitude,
                    timezone=item.timezone,
                )
            )
            created += 1
            continue

        is_changed = False
        if city.latitude != item.latitude:
            city.latitude = item.latitude
            is_changed = True
        if city.longitude != item.longitude:
            city.longitude = item.longitude
            is_changed = True
        if city.timezone != item.timezone:
            city.timezone = item.timezone
            is_changed = True
        if is_changed:
            updated += 1

    session.flush()
    logger.info(
        "Cities synchronized from YAML: created=%s updated=%s path=%s",
        created,
        updated,
        yaml_path,
    )


def _extract_to_raw(
    session: Session,
    *,
    settings: Settings,
    now_utc: dt.datetime,
    cities: list[CityDTO],
) -> RawStageResult:
    inserted_count = 0
    duplicate_count = 0
    failed_count = 0
    city_results: list[dict[str, object]] = []
    past_days = max(settings.open_meteo_past_days, settings.transform_closed_days_back)

    for city in cities:
        try:
            logger.info("Extracting weather for city=%s", city.name)
            payload = fetch_weather_with_window(
                settings.open_meteo_base_url,
                city,
                past_days=past_days,
                forecast_days=settings.open_meteo_forecast_days,
            )
            inserted = load_raw_event(
                session,
                source=settings.source_name,
                city_id=city.id,
                fetched_at=now_utc,
                payload=payload,
            )
            if inserted:
                inserted_count += 1
                city_results.append({"city": city.name, "status": "inserted"})
            else:
                duplicate_count += 1
                city_results.append({"city": city.name, "status": "duplicate"})
        except Exception as exc:
            failed_count += 1
            city_results.append(
                {
                    "city": city.name,
                    "status": "failed",
                    "error": str(exc),
                }
            )
            logger.exception("RAW stage failed for city=%s", city.name)

    return RawStageResult(
        inserted=inserted_count,
        duplicates=duplicate_count,
        failed=failed_count,
        city_results=city_results,
    )


def _filter_quality_passed_metrics(
    metrics: list[DailyMetric],
    warnings: list[QualityWarning],
) -> list[DailyMetric]:
    rejected_dates = {warning.date for warning in warnings}
    return [metric for metric in metrics if metric.date not in rejected_dates]


def _transform_to_mart(
    session: Session,
    *,
    settings: Settings,
    target_dates: list[dt.date],
    cities: list[CityDTO],
) -> TransformStageResult:
    affected_count = 0
    failed_count = 0
    dq_warnings: list[dict[str, object]] = []
    city_results: list[dict[str, object]] = []

    for city in cities:
        try:
            raw = get_latest_raw_snapshot(
                session,
                source=settings.source_name,
                city_id=city.id,
            )
            if not raw:
                failed_count += 1
                city_results.append(
                    {
                        "city": city.name,
                        "status": "missing_raw",
                    }
                )
                logger.warning("No RAW snapshot for city=%s", city.name)
                continue

            metrics = parse_hourly_to_daily(raw.payload)
            target_metrics = [metric for metric in metrics if metric.date in set(target_dates)]
            warnings = evaluate_daily_metrics(
                city=city,
                metrics=target_metrics,
                target_dates=target_dates,
                min_hours_per_complete_day=settings.min_hours_per_complete_day,
            )
            quality_passed_metrics = _filter_quality_passed_metrics(target_metrics, warnings)
            if quality_passed_metrics:
                affected_count += upsert_daily_metrics(
                    session,
                    city_id=city.id,
                    metrics=quality_passed_metrics,
                )

            dq_warnings.extend([warning.to_dict() for warning in warnings])
            city_results.append(
                {
                    "city": city.name,
                    "status": "upserted" if quality_passed_metrics else "skipped",
                    "rows_upserted": len(quality_passed_metrics),
                    "warning_count": len(warnings),
                    "target_dates": [date.isoformat() for date in target_dates],
                }
            )

            for warning in warnings:
                logger.warning(warning.message)
        except Exception as exc:
            failed_count += 1
            city_results.append(
                {
                    "city": city.name,
                    "status": "failed",
                    "error": str(exc),
                }
            )
            logger.exception("Mart stage failed for city=%s", city.name)

    return TransformStageResult(
        rows_upserted=affected_count,
        failed=failed_count,
        dq_warning_count=len(dq_warnings),
        city_results=city_results,
        dq_warnings=dq_warnings,
    )


def _persist_run_summary(
    *,
    engine,
    run_id: int,
    finished_at: dt.datetime,
    status: str,
    raw_result: RawStageResult,
    transform_result: TransformStageResult,
    target_dates: list[dt.date],
    last_error: str | None = None,
) -> None:
    with Session(engine) as session:
        run = session.get(PipelineRun, run_id)
        if run is None:
            logger.error("Pipeline run disappeared before summary persistence: run_id=%s", run_id)
            return

        details = {
            "target_dates": [date.isoformat() for date in target_dates],
            "raw": raw_result.city_results,
            "mart": transform_result.city_results,
            "dq_warnings": transform_result.dq_warnings,
        }
        finish_pipeline_run(
            session,
            run=run,
            finished_at=finished_at,
            status=status,
            raw_inserted=raw_result.inserted,
            raw_duplicates=raw_result.duplicates,
            raw_failed=raw_result.failed,
            mart_rows_upserted=transform_result.rows_upserted,
            mart_failed=transform_result.failed,
            dq_warnings=transform_result.dq_warning_count,
            details=details,
            last_error=last_error,
        )
        session.commit()


def run_once(*, configure_logging: bool = True) -> int:
    settings = Settings()
    if configure_logging:
        setup_logging(settings.log_level)

    engine = create_db_engine(settings.database_url)
    now_utc = dt.datetime.now(dt.timezone.utc)
    target_dates = _build_target_dates(
        now_utc,
        closed_days_back=settings.transform_closed_days_back,
    )

    raw_result = RawStageResult()
    transform_result = TransformStageResult()
    run_id: int | None = None

    try:
        with Session(engine) as session:
            run = start_pipeline_run(
                session,
                source=settings.source_name,
                started_at=now_utc,
                target_dates=target_dates,
            )
            session.commit()
            run_id = run.id

        with Session(engine) as session:
            if settings.cities_yaml_path:
                _sync_cities_from_yaml(session, settings.cities_yaml_path)
                session.commit()

            cities = _get_cities(session)
            if not cities:
                raise RuntimeError("No cities found in DB. Add rows into cities table.")

            raw_result = _extract_to_raw(
                session,
                settings=settings,
                now_utc=now_utc,
                cities=cities,
            )
            session.commit()

        with Session(engine) as session:
            cities = _get_cities(session)
            transform_result = _transform_to_mart(
                session,
                settings=settings,
                target_dates=target_dates,
                cities=cities,
            )
            session.commit()

        status = PipelineRun.Status.SUCCESS
        if raw_result.failed or transform_result.failed or transform_result.dq_warning_count:
            status = PipelineRun.Status.PARTIAL_SUCCESS

        if run_id is not None:
            _persist_run_summary(
                engine=engine,
                run_id=run_id,
                finished_at=dt.datetime.now(dt.timezone.utc),
                status=status,
                raw_result=raw_result,
                transform_result=transform_result,
                target_dates=target_dates,
            )

        if status == PipelineRun.Status.PARTIAL_SUCCESS:
            logger.warning(
                "ETL finished with warnings: raw_inserted=%s raw_duplicates=%s raw_failed=%s "
                "mart_rows_upserted=%s mart_failed=%s dq_warnings=%s target_dates=%s",
                raw_result.inserted,
                raw_result.duplicates,
                raw_result.failed,
                transform_result.rows_upserted,
                transform_result.failed,
                transform_result.dq_warning_count,
                [date.isoformat() for date in target_dates],
            )
            return 1

        logger.info(
            "ETL done. raw_inserted=%s raw_duplicates=%s mart_rows_upserted=%s target_dates=%s",
            raw_result.inserted,
            raw_result.duplicates,
            transform_result.rows_upserted,
            [date.isoformat() for date in target_dates],
        )
        return 0
    except Exception as exc:
        logger.exception("ETL failed")
        if run_id is not None:
            _persist_run_summary(
                engine=engine,
                run_id=run_id,
                finished_at=dt.datetime.now(dt.timezone.utc),
                status=PipelineRun.Status.FAILED,
                raw_result=raw_result,
                transform_result=transform_result,
                target_dates=target_dates,
                last_error=str(exc),
            )
        return 2


if __name__ == "__main__":
    sys.exit(run_once())
