# Weather ETL Pipeline

Production-style weather ETL pipeline на данных Open-Meteo.

Проект забирает почасовые погодные данные из внешнего API, складывает сырой ответ в RAW-слой, строит дневную витрину и сохраняет историю batch-run. В репозитории собраны extract, load, transform, quality checks и audit trail запусков.

## В чем идея проекта

Многие учебные ETL-репозитории заканчиваются на “скачал JSON и записал в таблицу”. Здесь сделан следующий шаг:

- есть разделение RAW и mart
- загрузка идемпотентна
- витрина строится только по закрытым дням
- data quality checks влияют на публикацию данных
- каждый запуск пайплайна оставляет после себя audit trail

За счет этого проект выглядит не как скрипт, а как небольшой управляемый pipeline.

## Что показывает репозиторий

- Проектирование ETL-потока от внешнего API до аналитической витрины
- Идемпотентную загрузку через `ON CONFLICT`
- Разделение raw snapshots и derived metrics
- Обработку частичных ошибок по городам без остановки всего batch-run
- Сохранение статусов и счетчиков запусков для operational debugging
- Базовые data quality checks до публикации строк в витрину

## Архитектура

```text
Open-Meteo API
      |
      v
[Extract]
hourly payload per city
      |
      v
[Load RAW]
table: weather_raw
key: (source, city_id, fetched_date)
      |
      v
[Transform Closed Days]
latest RAW snapshot -> daily aggregates
quality checks -> в витрину попадают только полные дни
      |
      v
[Mart]
table: daily_city_metrics
key: (city_id, date)
      |
      v
[Audit]
table: pipeline_runs
status run-а, counters, warnings, details JSON
```

## Модель данных

### `cities`

Справочник городов с координатами и timezone.

### `weather_raw`

Хранит сырые snapshots ответа API.

- `source`
- `city_id`
- `fetched_at`
- `fetched_date`
- `payload`
- уникальный ключ: `(source, city_id, fetched_date)`

### `daily_city_metrics`

Дневная аналитическая витрина.

- `city_id`
- `date`
- `avg_temp`
- `min_temp`
- `max_temp`
- `sum_precip`
- `computed_at`
- уникальный ключ: `(city_id, date)`

### `pipeline_runs`

Аудит запусков пайплайна.

- `started_at`
- `finished_at`
- `status`
- `target_window_start`
- `target_window_end`
- `raw_inserted`
- `raw_duplicates`
- `raw_failed`
- `mart_rows_upserted`
- `mart_failed`
- `dq_warnings`
- `details`
- `last_error`

## Почему важны closed-day semantics

Hourly APIs почти всегда неудобны для прямой аналитики: в ответе легко получить полный вчерашний день, половину сегодняшнего и немного прогноза сверху. Если строить витрину “как пришло”, метрики будут прыгать и выглядеть ненадежно.

Поэтому этот pipeline по умолчанию публикует только полностью закрытые дни. Это небольшое решение, но именно оно делает витрину больше похожей на реальный reporting layer.

## Data Quality Checks

Перед записью в витрину пайплайн проверяет:

- что нужные target dates вообще есть в raw snapshot
- что для каждого закрытого дня набралось минимум `MIN_HOURS_PER_COMPLETE_DAY` наблюдений

Если день отсутствует или неполный, пайплайн:

- не пишет эту строку в витрину
- фиксирует warning в `pipeline_runs.details`
- помечает запуск как `partial_success`

Это важная часть пайплайна: данные не считаются “хорошими по умолчанию” и не публикуются без базовой проверки полноты.

## Структура проекта

```text
app/
  db/
    base.py
    models.py
    session.py
  etl/
    config.py
    extract.py
    load.py
    logging_config.py
    pipeline_runs.py
    quality.py
    report.py
    run.py
    scheduler.py
    sql_utils.py
    transform.py
  scripts/
    init.sql
tests/
docker-compose.yml
Dockerfile
requirements.txt
requirements-dev.txt
```

## Быстрый старт

### Docker

```bash
cp .env.example .env
docker compose up --build
```

После старта доступны:

- ETL scheduler container
- PostgreSQL
- Adminer на `http://localhost:8080`

Стандартные учетные данные базы:

- server: `db`
- database: `etl`
- user: `etl`
- password: `etl`

Если нужна полная переинициализация:

```bash
docker compose down -v
docker compose up --build
```

### Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
# Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
python -m app.etl.run
```

Запуск scheduler:

```bash
python -m app.etl.scheduler
```

Просмотр последних запусков:

```bash
python -m app.etl.report --limit 5
```

## Переменные окружения

См. `.env.example`.

- `DATABASE_URL` - SQLAlchemy database URL
- `LOG_LEVEL` - уровень логирования
- `SCHEDULE_EVERY_MINUTES` - интервал scheduler
- `OPEN_METEO_BASE_URL` - endpoint Open-Meteo
- `OPEN_METEO_PAST_DAYS` - сколько прошлых дней запрашивать у API
- `OPEN_METEO_FORECAST_DAYS` - сколько будущих дней запрашивать у API
- `SOURCE_NAME` - label источника в RAW-слое
- `TRANSFORM_CLOSED_DAYS_BACK` - сколько закрытых дней публиковать в витрину
- `MIN_HOURS_PER_COMPLETE_DAY` - минимальное число наблюдений для полного дня
- `CITIES_YAML` - опциональный YAML override для таблицы `cities`

Пример YAML:

```yaml
cities:
  - name: Berlin
    latitude: 52.52
    longitude: 13.405
    timezone: Europe/Berlin
```

## Примеры SQL-запросов

Последние строки витрины:

```sql
SELECT
  c.name,
  m.date,
  m.avg_temp,
  m.min_temp,
  m.max_temp,
  m.sum_precip
FROM daily_city_metrics m
JOIN cities c ON c.id = m.city_id
ORDER BY m.date DESC, c.name;
```

Последние pipeline runs:

```sql
SELECT
  started_at,
  finished_at,
  status,
  raw_inserted,
  raw_duplicates,
  raw_failed,
  mart_rows_upserted,
  mart_failed,
  dq_warnings
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

## Проверка качества

```bash
python -m ruff check .
python -m pytest -q
```

CI запускает:

- `ruff check`
- `pytest --cov=app`

## Заметки

- Pipeline специально собран без внешнего оркестратора, чтобы фокус оставался на data modeling, quality checks и идемпотентной загрузке.
- Витрина публикуется только по закрытым дням. Это снижает “свежесть” данных, но делает reporting слой стабильнее.
- Частичные ошибки по городам не прерывают весь запуск. Вместо этого run получает `partial_success`, а детали сохраняются в `pipeline_runs`.
