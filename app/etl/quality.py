from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass

from app.etl.extract import CityDTO
from app.etl.transform import DailyMetric


@dataclass(frozen=True)
class QualityWarning:
    city_id: int
    city_name: str
    date: dt.date
    code: str
    message: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["date"] = self.date.isoformat()
        return payload


def evaluate_daily_metrics(
    *,
    city: CityDTO,
    metrics: list[DailyMetric],
    target_dates: list[dt.date],
    min_hours_per_complete_day: int,
) -> list[QualityWarning]:
    metrics_by_date = {metric.date: metric for metric in metrics}
    warnings: list[QualityWarning] = []

    for target_date in target_dates:
        metric = metrics_by_date.get(target_date)
        if metric is None:
            warnings.append(
                QualityWarning(
                    city_id=city.id,
                    city_name=city.name,
                    date=target_date,
                    code="missing_day",
                    message=(
                        "Missing daily aggregate for "
                        f"city={city.name} date={target_date.isoformat()}"
                    ),
                )
            )
            continue

        if metric.observation_count < min_hours_per_complete_day:
            warnings.append(
                QualityWarning(
                    city_id=city.id,
                    city_name=city.name,
                    date=target_date,
                    code="incomplete_day",
                    message=(
                        f"Incomplete day for city={city.name} date={target_date.isoformat()}: "
                        f"hours={metric.observation_count} expected>={min_hours_per_complete_day}"
                    ),
                )
            )

    return warnings
