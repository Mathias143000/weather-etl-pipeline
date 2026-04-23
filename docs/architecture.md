# Weather ETL Pipeline Architecture

## Intent

This repository is the portfolio's compact DataOps platform lab.
The main goal is to show mature ETL behavior with a small, explainable stack.

## Core Flow

1. The scheduler triggers `run_once`.
2. The extract stage fetches hourly weather payloads per city.
3. RAW snapshots are loaded idempotently into `weather_raw`.
4. The transform stage converts hourly payloads into daily metrics.
5. Quality checks decide which closed-day rows are safe to publish.
6. Published rows land in `daily_city_metrics`.
7. The full run outcome is recorded in `pipeline_runs`.

## Why There Is A Mock API In Compose

The compose lab uses `open-meteo-mock` instead of the public API so that:

- CI stays deterministic
- local smoke runs do not depend on internet stability
- the ETL still exercises the full extract/load/transform path

The local non-compose path can still use the real Open-Meteo endpoint from `.env`.

## Storage Model

### `weather_raw`

Stores source payloads per city and fetch date.
This is the raw accountability layer for downstream debugging.

### `daily_city_metrics`

Stores daily aggregates after quality gating.
This is the reporting-facing mart layer.

### `pipeline_runs`

Stores operational metadata for each ETL run:

- status
- counters
- target window
- warnings
- failure details

## Operational Trade-Offs

### Why no Airflow yet

The pipeline already has a clear schedule, a deterministic entrypoint, quality gating, and an audit trail.
Airflow would be a valid future upgrade, but it is not required for this repo to tell a strong DataOps story today.

### Why closed-day semantics matter

Publishing partially collected days makes a reporting mart unstable.
This repo intentionally favors trustworthy daily aggregates over maximum immediacy.

## Current Compose Topology

- `db`
- `etl`
- `open-meteo-mock`
- `adminer`

This is enough to show:

- scheduled batch behavior
- idempotent load
- deterministic runtime
- inspectable storage and metadata
