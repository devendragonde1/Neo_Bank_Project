# 01_Metada_setup — Metadata Layer Setup

One-time setup for the Neo_Bank metadata-driven pipeline. This folder creates the `banking` catalog and the `banking.metadata` control tables, the blob landing volume, and registers every table the pipeline ingests — from both the Neon Postgres source and blob storage — together with its load parameters and initial watermarks.



## Notebooks

- `01_setup_metadata` — one-time DDL + seed inserts (6 cells, run top to bottom)
- `02_check_metada` — validation queries to verify the setup

## What `01_setup_metadata` does

1. **Create catalog & metadata schema** — `CREATE CATALOG IF NOT EXISTS banking`, `CREATE SCHEMA IF NOT EXISTS banking.metadata`, then four Delta control tables:
   - `metadata.tables` — static registry of logical tables (`table_id`, `table_name`, `source_system`, `source_schema`, `source_table`, `source_path`, `target_layer`, `bronze_schema`, `silver_schema`, `gold_schema`, `active_flag`, `load_order`, `created_at`)
   - `metadata.table_parameters` — per-table processing config (`table_id`, `parameter_name`, `parameter_value`, `created_at`; names: `load_type`, `primary_key`, `watermark_column`)
   - `metadata.table_watermarks` — last successful watermark per table (`table_id`, `last_watermark_value`, `last_updated_at`, `last_run_id`), partitioned by `table_id`
   - `metadata.pipeline_runs` — execution audit (`run_id`, `table_id`, `layer` Bronze/Silver/Gold, `start_time`, `end_time`, `status` SUCCESS/FAILED, `number_of_records`, `error_message`), partitioned by `table_id`
2. **Create blob landing zone** — schema `banking.source` + volume `banking.source.volume`, where source CSV files for blob ingestion land (Auto Loader checkpoints/schema live under `/Volumes/banking/source/volume/_checkpoints` and `_schema`)
3. **Register silver-layer source tables (1–6)** — postgres tables from schema `banking`, and blob tables with `source_path` (`/Volumes/banking/source/volume/credit_bureau_reports/`, `/Volumes/banking/source/volume/payment_gateway_logs/`)
4. **Register load parameters** — `load_type`, `primary_key`, `watermark_column` per table (see table below)
5. **Seed initial watermarks** — `1900-01-01 00:00:00` for the five incrementally loaded tables (branches is FULL load, so no watermark)
6. **Register gold-layer target tables (7–11)** — `source_system='silver'`, `target_layer='gold'`, driving the `03_Silver_to_Gold` chain

## Registered tables & load parameters

| table_id | table_name | source_system | load_type | primary_key | watermark_column |
| --- | --- | --- | --- | --- | --- |
| 1 | customers | postgres | MERGE | customer_id | updated_at |
| 2 | accounts | postgres | MERGE | account_id | updated_at |
| 3 | transactions | postgres | APPEND | txn_id | txn_timestamp |
| 4 | branches | postgres | FULL | branch_code | — |
| 5 | credit_bureau_reports | blob | MERGE | customer_id | bureau_pull_date |
| 6 | payment_gateway_logs | blob | APPEND | txn_id | processed_timestamp |
| 7 | customer_360 | silver | — | — | — |
| 8 | branch_performance | silver | — | — | — |
| 9 | transaction_channel_summary | silver | — | — | — |
| 10 | daily_bank_kpi | silver | — | — | — |
| 11 | risk_customer_summary | silver | — | — | — |

## What `02_check_metada` does

Validation queries to inspect the metadata after setup:

- Full contents of `metadata.tables`, `metadata.table_parameters`, and `metadata.table_watermarks`
- Recent runs from `metadata.pipeline_runs` (ordered by `start_time DESC`) — useful for auditing Silver/Gold loads
- A `tables ⨝ table_parameters` join showing each table's `load_type`

## Usage notes

- Run `01_setup_metadata` once, top to bottom; then run `02_check_metada` to verify.
- The `CREATE ... IF NOT EXISTS` statements are idempotent, but the `INSERT` statements are **not** — re-running the whole notebook will duplicate registrations. Use `DELETE FROM` / `TRUNCATE` on the metadata tables first if you need to re-seed.
- The downstream pipeline (`02_source_to_silver`, `03_Silver_to_Gold`) reads everything it needs from these tables; no hardcoded table lists exist in the pipeline notebooks.
