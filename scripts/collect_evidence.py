from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "artifacts" / "evidence"


def run(*args: str) -> str:
    completed = subprocess.run(
        list(args),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def write_file(name: str, content: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / name).write_text(content, encoding="utf-8")


PIPELINE_RUNS_SQL = """
SELECT id, status, raw_inserted, raw_duplicates, mart_rows_upserted, dq_warnings
FROM pipeline_runs
ORDER BY id DESC
LIMIT 10;
""".strip()

DAILY_METRICS_SQL = """
SELECT city_id, date, avg_temp, min_temp, max_temp, sum_precip
FROM daily_city_metrics
ORDER BY date DESC, city_id;
""".strip()


def main() -> None:
    write_file("compose-ps.txt", run("docker", "compose", "ps", "-a"))
    write_file("compose-config.txt", run("docker", "compose", "config"))
    write_file("compose-logs.txt", run("docker", "compose", "logs", "--no-color"))
    write_file(
        "pipeline-runs.txt",
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            "etl",
            "-d",
            "etl",
            "-c",
            PIPELINE_RUNS_SQL,
        ),
    )
    write_file(
        "daily-city-metrics.txt",
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            "etl",
            "-d",
            "etl",
            "-c",
            DAILY_METRICS_SQL,
        ),
    )
    write_file(
        "report.txt",
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "etl",
            "python",
            "-m",
            "app.etl.report",
            "--limit",
            "5",
        ),
    )
    print(str(OUTPUT_DIR))


if __name__ == "__main__":
    main()
