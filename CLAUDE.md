# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**sync-service** is a Dagster-orchestrated data pipeline that extracts sales, clients, and product data from multiple sources (MySQL AWS RDS, Firebird legacy, HTTP APIs) into a PostgreSQL warehouse, then transforms it through dbt (staging → intermediate → marts). It replaces a legacy DuckDB-based Flask sidecar (dbt-sync).

## Development Commands

```bash
# Start local dev stack (Postgres + Redis + Dagster UI at :3000)
docker-compose -f docker-compose.local.yml up

# Run Dagster dev server directly (requires Postgres + Redis running)
dagster dev -h 0.0.0.0 -p 3000

# dbt commands (run from repo root, dbt/ contains the project)
dbt parse --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt

# Install dependencies
pip install -r requirements.txt
```

## Architecture

### Data Flow
```
Sources (MySQL RDS × 4, Firebird, client-service API, product-service API)
  → Raw extraction (Dagster assets → raw.* tables via COPY bulk load)
    → dbt staging (views, light normalization)
      → dbt intermediate (views, business logic: account resolution, GM detection, price history, discounts)
        → dbt marts (fct_sales table, monthly partitioned)
```

### Key Patterns

- **Monthly partitioning**: All sales assets use `MonthlyPartitionsDefinition(start_date='2023-01-01')`. Partitions are calendar months (YYYY-MM-01).
- **Multi-source consolidation**: 4 MySQL databases (DIMDS, DIMPPAL, DISDS, DISPPAL) write to single `raw.raw_sales` using source-aware delete+insert for idempotency.
- **Sensor-triggered dbt**: `raw_sales_dbt_sensor` detects raw_sales materialization and fires `dbt_sales_build` with the same partition key.
- **Atomic bulk loads**: All raw assets use psycopg2 COPY (not INSERT) within transactions. Either TRUNCATE+COPY or DELETE+COPY for atomicity.
- **Resource concurrency**: Firebird limited to 1 concurrent run, MySQL to 3 (configured in `dagster.yaml`).
- **dbt layering**: Staging/intermediate are views; only marts materialized as tables. dbt receives `min_month`/`max_month` vars for incremental runs.

### Entry Points

- `dagster_sync/definitions.py` — All jobs, schedules, sensors, and resource bindings
- `dagster_sync/assets/raw/` — Raw extraction assets (one file per source)
- `dagster_sync/assets/dbt.py` — dbt asset definitions with custom `SyncDbtTranslator`
- `dagster_sync/resources/` — Database and API connectors (warehouse, distri_rds, prices_db, client_service, product_service)
- `dagster_sync/types/` — TypedDict schemas for all row types

### Jobs & Schedules

| Job | Schedule | Description |
|-----|----------|-------------|
| `sales_sync` | Hourly (`0 * * * *`) | Extracts 4 MySQL sources → raw_sales |
| `dimensions_sync` | Daily 3am | Updates master data (clients, sellers, articles) |
| `price_history_sync` | 4am & 4pm | Firebird price history |
| `dbt_sales_build` | Sensor-triggered | Incremental dbt for monthly partitions |
| `dbt_sales_full_refresh` | Manual only | Full rebuild + test suite |
| `dbt_dimensions_build` | After dimensions_sync | Rebuilds dimension views |

### Database Schemas

- `raw` — Extracted source data (raw_sales, raw_clients, raw_sellers, raw_articulos, etc.)
- `staging` — dbt staging views (stg_*)
- `intermediate` — dbt business logic views (int_sales_*)
- `analytics` — dbt marts (fct_sales)

## Conventions

- Python code uses TypedDict for row schemas (see `dagster_sync/types/`)
- Raw assets return `MaterializeResult` with metadata (rows_written, source_db, etc.)
- Retry policy on all raw assets: max_retries=3, delay=60s
- Environment variables defined in `.env.local.example`
- Comments in code must be in English
