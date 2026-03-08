from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger("etl.extract")
REQUIRED_HOURLY_FIELDS = ("time", "temperature_2m", "precipitation")


@dataclass(frozen=True)
class CityDTO:
    id: int
    name: str
    latitude: float
    longitude: float
    timezone: str


def _validate_weather_payload(payload: dict[str, Any], city: CityDTO) -> None:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError(f"Open-Meteo payload is missing 'hourly' block for city={city.name}")

    missing = [field for field in REQUIRED_HOURLY_FIELDS if field not in hourly]
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(
            f"Open-Meteo payload is missing required hourly fields for city={city.name}: "
            f"{missing_csv}"
        )


def fetch_weather(base_url: str, city: CityDTO) -> dict[str, Any]:
    """Backward-compatible wrapper for the default fetch window."""
    return fetch_weather_with_window(
        base_url,
        city,
        past_days=1,
        forecast_days=1,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(
        (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError)
    ),
    reraise=True,
)
def fetch_weather_with_window(
    base_url: str,
    city: CityDTO,
    *,
    past_days: int,
    forecast_days: int,
) -> dict[str, Any]:
    """Fetch hourly weather for a city from Open-Meteo (no auth)."""
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": "temperature_2m,precipitation",
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "UTC",
    }

    timeout = httpx.Timeout(10.0, connect=5.0)
    with httpx.Client(timeout=timeout) as client:
        r = client.get(base_url, params=params)
        r.raise_for_status()
        data = r.json()

    _validate_weather_payload(data, city)
    return data
