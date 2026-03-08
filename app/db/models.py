from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .base import Base


class City(Base):
    __tablename__ = "cities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    timezone: Mapped[str] = mapped_column(String, nullable=False, default="UTC")


class WeatherRaw(Base):
    __tablename__ = "weather_raw"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "city_id",
            "fetched_date",
            name="uq_weather_raw_day",
        ),
    )

    # SQLite requires INTEGER PRIMARY KEY for autoincrement.
    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    )
    source: Mapped[str] = mapped_column(String, nullable=False)
    city_id: Mapped[int] = mapped_column(
        ForeignKey("cities.id", ondelete="CASCADE"),
        nullable=False,
    )
    fetched_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    fetched_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    payload: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )


class DailyCityMetrics(Base):
    __tablename__ = "daily_city_metrics"
    __table_args__ = (
        UniqueConstraint("city_id", "date", name="uq_daily_city_metrics"),
    )

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    )
    city_id: Mapped[int] = mapped_column(
        ForeignKey("cities.id", ondelete="CASCADE"),
        nullable=False,
    )
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)

    avg_temp: Mapped[float] = mapped_column(Float, nullable=False)
    min_temp: Mapped[float] = mapped_column(Float, nullable=False)
    max_temp: Mapped[float] = mapped_column(Float, nullable=False)
    sum_precip: Mapped[float] = mapped_column(Float, nullable=False)
    computed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    class Status:
        SUCCESS = "success"
        PARTIAL_SUCCESS = "partial_success"
        FAILED = "failed"

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    )
    source: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    target_window_start: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    target_window_end: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    raw_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    raw_duplicates: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    raw_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mart_rows_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mart_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dq_warnings: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
        default=dict,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
