# Weather ETL Pipeline Demo Runbook

## Purpose

This runbook is the shortest path to showing the project as a portfolio-ready DataOps stand.

## 1. Prepare Environment

```bash
python -m pip install -r requirements-dev.txt
python scripts/bootstrap_env.py
```

## 2. Validate The Code

```bash
python -m ruff check .
python -m pytest -q
```

## 3. Start The Stack

```bash
docker compose up -d --build
docker compose ps -a
```

Expected services:

- `db`
- `etl`
- `open-meteo-mock`
- `adminer`

## 4. Run The Compose Smoke

```bash
python scripts/compose_smoke.py
```

Expected outcome:

- database is reachable
- at least one pipeline run exists
- RAW rows exist
- mart rows exist
- latest run status is visible

## 5. Inspect Recent Runs

```bash
docker compose exec -T etl python -m app.etl.report --limit 5
```

You can also inspect the mart in Adminer at `http://localhost:8080`.

## 6. Trigger Another ETL Cycle

```bash
python scripts/run_backfill.py
```

This is useful to demonstrate:

- repeated scheduled-like execution
- duplicate-safe RAW ingest
- stable mart refresh behavior

## 7. Collect Evidence

```bash
python scripts/collect_evidence.py
```

Artifacts are written to:

- `artifacts/evidence/compose-ps.txt`
- `artifacts/evidence/compose-config.txt`
- `artifacts/evidence/compose-logs.txt`
- `artifacts/evidence/pipeline-runs.txt`
- `artifacts/evidence/daily-city-metrics.txt`
- `artifacts/evidence/report.txt`

## 8. Stop The Stack

```bash
docker compose down -v --remove-orphans
```

## Troubleshooting

### Smoke fails because there are no mart rows

Check:

- `docker compose logs etl --no-color`
- `docker compose exec -T etl python -m app.etl.report --limit 5`

This usually means the quality gate blocked publication or the ETL never reached transform.

### Database is up but scheduler did not run

Check:

- `docker compose logs etl --no-color`

The scheduler should execute one ETL cycle on startup before entering its interval loop.

### Need to inspect SQL state directly

Example:

```bash
docker compose exec -T db psql -U etl -d etl -c "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 5;"
```
