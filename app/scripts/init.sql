-- Basic schema for ETL mini-pipeline: RAW (staging) + mart
-- Runs automatically on first Postgres init via docker-entrypoint-initdb.d

CREATE TABLE IF NOT EXISTS cities (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  latitude    DOUBLE PRECISION NOT NULL,
  longitude   DOUBLE PRECISION NOT NULL,
  timezone    TEXT NOT NULL DEFAULT 'UTC'
);

-- Seed a couple of cities for demo
INSERT INTO cities (name, latitude, longitude, timezone) VALUES
  ('Tallinn', 59.4370, 24.7536, 'Europe/Tallinn'),
  ('Moscow', 55.7558, 37.6173, 'Europe/Moscow')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS weather_raw (
  id           BIGSERIAL PRIMARY KEY,
  source       TEXT NOT NULL,
  city_id      INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  fetched_date DATE NOT NULL,
  payload      JSONB NOT NULL,

  -- One RAW record per (source, city, day-of-fetch)
  CONSTRAINT uq_weather_raw_day UNIQUE (source, city_id, fetched_date)
);

CREATE TABLE IF NOT EXISTS daily_city_metrics (
  id            BIGSERIAL PRIMARY KEY,
  city_id       INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
  date          DATE NOT NULL,
  avg_temp      DOUBLE PRECISION NOT NULL,
  min_temp      DOUBLE PRECISION NOT NULL,
  max_temp      DOUBLE PRECISION NOT NULL,
  sum_precip    DOUBLE PRECISION NOT NULL,
  computed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uq_daily_city_metrics UNIQUE (city_id, date)
);

CREATE INDEX IF NOT EXISTS idx_weather_raw_city_fetched ON weather_raw (city_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_city_date ON daily_city_metrics (city_id, date DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id                  BIGSERIAL PRIMARY KEY,
  source              TEXT NOT NULL,
  started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at         TIMESTAMPTZ NULL,
  status              TEXT NOT NULL,
  target_window_start DATE NULL,
  target_window_end   DATE NULL,
  raw_inserted        INTEGER NOT NULL DEFAULT 0,
  raw_duplicates      INTEGER NOT NULL DEFAULT 0,
  raw_failed          INTEGER NOT NULL DEFAULT 0,
  mart_rows_upserted  INTEGER NOT NULL DEFAULT 0,
  mart_failed         INTEGER NOT NULL DEFAULT 0,
  dq_warnings         INTEGER NOT NULL DEFAULT 0,
  details             JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_error          TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);
