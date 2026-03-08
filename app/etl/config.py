from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CityConfig(BaseModel):
    name: str
    latitude: float
    longitude: float
    timezone: str = "UTC"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(alias="DATABASE_URL")
    open_meteo_base_url: str = Field(
        default="https://api.open-meteo.com/v1/forecast",
        alias="OPEN_METEO_BASE_URL",
    )
    source_name: str = Field(default="open_meteo", alias="SOURCE_NAME")
    open_meteo_past_days: int = Field(default=2, alias="OPEN_METEO_PAST_DAYS")
    open_meteo_forecast_days: int = Field(default=1, alias="OPEN_METEO_FORECAST_DAYS")

    schedule_every_minutes: int = Field(default=60, alias="SCHEDULE_EVERY_MINUTES")
    transform_closed_days_back: int = Field(default=1, alias="TRANSFORM_CLOSED_DAYS_BACK")
    min_hours_per_complete_day: int = Field(default=24, alias="MIN_HOURS_PER_COMPLETE_DAY")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Optional: override cities from YAML instead of DB seed
    cities_yaml_path: Optional[str] = Field(default=None, alias="CITIES_YAML")

    @field_validator(
        "open_meteo_past_days",
        "open_meteo_forecast_days",
        "schedule_every_minutes",
        "transform_closed_days_back",
        "min_hours_per_complete_day",
    )
    @classmethod
    def validate_positive_integers(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Configuration values must be greater than or equal to 1.")
        return value


def load_cities_from_yaml(path: str | Path) -> list[CityConfig]:
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Cities YAML not found: {yaml_path}")

    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("Cities YAML root must be a mapping with 'cities' key.")

    items: Any = data.get("cities", [])
    if not isinstance(items, list):
        raise ValueError("Cities YAML 'cities' field must be a list.")

    return [CityConfig.model_validate(item) for item in items]
