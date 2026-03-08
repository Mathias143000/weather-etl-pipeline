from __future__ import annotations

import argparse
import json

from sqlalchemy.orm import Session

from app.db.session import create_db_engine
from app.etl.config import Settings
from app.etl.pipeline_runs import get_recent_pipeline_runs


def build_report(limit: int) -> str:
    settings = Settings()
    engine = create_db_engine(settings.database_url)

    with Session(engine) as session:
        runs = get_recent_pipeline_runs(session, limit=limit)

    if not runs:
        return "No pipeline runs found."

    lines = []
    for run in runs:
        lines.append(
            json.dumps(
                {
                    "id": run.id,
                    "status": run.status,
                    "started_at": run.started_at.isoformat(),
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                    "source": run.source,
                    "target_window_start": (
                        run.target_window_start.isoformat() if run.target_window_start else None
                    ),
                    "target_window_end": (
                        run.target_window_end.isoformat() if run.target_window_end else None
                    ),
                    "raw_inserted": run.raw_inserted,
                    "raw_duplicates": run.raw_duplicates,
                    "raw_failed": run.raw_failed,
                    "mart_rows_upserted": run.mart_rows_upserted,
                    "mart_failed": run.mart_failed,
                    "dq_warnings": run.dq_warnings,
                },
                ensure_ascii=True,
            )
        )

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print recent ETL pipeline runs.")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    print(build_report(limit=args.limit))


if __name__ == "__main__":
    main()
