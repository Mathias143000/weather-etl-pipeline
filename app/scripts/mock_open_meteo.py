from __future__ import annotations

import datetime as dt
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def _as_int(value: str | None, fallback: int) -> int:
    try:
        return int(value or "")
    except (TypeError, ValueError):
        return fallback


def _as_float(value: str | None, fallback: float) -> float:
    try:
        return float(value or "")
    except (TypeError, ValueError):
        return fallback


def build_payload(query: dict[str, list[str]]) -> dict[str, object]:
    latitude = _as_float((query.get("latitude") or [None])[0], 0.0)
    past_days = max(_as_int((query.get("past_days") or [None])[0], 2), 1)
    forecast_days = max(_as_int((query.get("forecast_days") or [None])[0], 1), 1)
    total_days = past_days + forecast_days
    start_date = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=past_days)

    times: list[str] = []
    temperatures: list[float] = []
    precipitation: list[float] = []

    city_bias = round((latitude % 7) * 0.4, 2)

    for day_offset in range(total_days):
        current_day = start_date + dt.timedelta(days=day_offset)
        for hour in range(24):
            times.append(f"{current_day.isoformat()}T{hour:02d}:00")
            temperatures.append(round(8.0 + city_bias + day_offset + (hour / 10.0), 2))
            precipitation.append(round((hour % 4) * 0.1, 2))

    return {
        "latitude": latitude,
        "longitude": _as_float((query.get("longitude") or [None])[0], 0.0),
        "generationtime_ms": 1.23,
        "utc_offset_seconds": 0,
        "timezone": "UTC",
        "timezone_abbreviation": "UTC",
        "elevation": 0,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "degC",
            "precipitation": "mm",
        },
        "hourly": {
            "time": times,
            "temperature_2m": temperatures,
            "precipitation": precipitation,
        },
    }


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, object], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json({"ok": True, "service": "open-meteo-mock"})
            return

        if parsed.path == "/v1/forecast":
            self._send_json(build_payload(parse_qs(parsed.query)))
            return

        self._send_json({"ok": False, "error": "not found"}, status=404)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    port = _as_int(os.getenv("MOCK_OPEN_METEO_PORT"), 8081)
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(json.dumps({"service": "open-meteo-mock", "port": port}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
