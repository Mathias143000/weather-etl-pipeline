from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_compose(*args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def query_scalar(sql: str) -> str:
    return run_compose("exec", "-T", "db", "psql", "-U", "etl", "-d", "etl", "-At", "-c", sql)


def wait_for_pipeline(timeout_seconds: int = 90) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    latest_status = ""
    while time.time() < deadline:
        latest_status = query_scalar(
            "SELECT COALESCE((SELECT status FROM pipeline_runs ORDER BY id DESC LIMIT 1), '');"
        )
        raw_count = int(query_scalar("SELECT COUNT(*) FROM weather_raw;") or "0")
        mart_count = int(query_scalar("SELECT COUNT(*) FROM daily_city_metrics;") or "0")
        run_count = int(query_scalar("SELECT COUNT(*) FROM pipeline_runs;") or "0")
        if run_count >= 1 and raw_count >= 1 and mart_count >= 1:
            latest_json = query_scalar(
                """
                SELECT row_to_json(t)::text
                FROM (
                  SELECT id, status, raw_inserted, raw_duplicates, raw_failed,
                         mart_rows_upserted, mart_failed, dq_warnings,
                         target_window_start, target_window_end
                  FROM pipeline_runs
                  ORDER BY id DESC
                  LIMIT 1
                ) t;
                """
            )
            return {
                "run_count": run_count,
                "raw_count": raw_count,
                "mart_count": mart_count,
                "latest_run": json.loads(latest_json),
            }
        time.sleep(2)

    raise RuntimeError(
        f"Timed out waiting for ETL output. Latest known pipeline status='{latest_status}'."
    )


def main() -> None:
    db_ready = query_scalar("SELECT 1;")
    if db_ready.strip() != "1":
        raise RuntimeError("PostgreSQL is not ready")

    summary = wait_for_pipeline()
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=True))


if __name__ == "__main__":
    main()
