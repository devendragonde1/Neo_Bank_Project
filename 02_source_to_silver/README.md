# 02_source_to_silver — Source to Silver Ingestion

The metadata-driven ingestion chain that loads data from both sources — Neon Postgres (JDBC) and blob storage (Auto Loader) — into the bronze layer, then transforms bronze into silver with `MERGE` / `APPEND` / `FULL` load logic. Everything the chain needs (which tables, how to load them, where to stop) is read from the `banking.metadata` control tables set up in [01_Metada_setup](../01_Metada_setup/README.md); no table lists are hardcoded.

## Notebooks (in chain order)

| # | Notebook | Role | Input | Output |
| --- | --- | --- | --- | --- |
| 00 | `00_secret_scope` | one-time setup of the Postgres connection secret | connection variables | secret `banking-scope` / `postgres-connection-json` |
| 01 | `01_read_table_list` | read active tables for a source system | widget `source_system` | task value `tables_metadata` |
| 02 | `02_read_table_parameters` | read load parameters for one table | widget `table_metadata` | task value `table_parameters` |
| 03 | `03_source_to_bronze` | load source → bronze | widgets `table_metadata`, `table_parameters`, `run_id` | rows in `banking.bronze.{table}` |
| 04 | `04_bronze_to_silver` | bronze → silver + watermark/audit updates | same widgets | rows in `banking.silver.{table}` |

## 00_secret_scope (one-time prerequisite)

1. Defines the Neon Postgres connection variables at the top of the notebook — replace the placeholders with **your own credentials**:

   ```python
   postgres_host     = "<your-postgres-host>"      # e.g. your Neon endpoint
   postgres_port     = "<your-postgres-port>"      # e.g. 5432
   postgres_database = "<your-database-name>"
   postgres_user     = "<your-database-user>"
   postgres_password = "<your-database-password>"

   secret_scope_name = "banking-scope"
   secret_key_name   = "postgres-connection-json"
   ```

2. Builds a JSON connection config (including `org.postgresql.Driver`) and stores it as a **single secret** in scope `banking-scope` under key `postgres-connection-json`, using the Secrets API 2.0 (`/api/2.0/secrets/scopes/create`, `/api/2.0/secrets/put`) with the session token from the notebook context.
3. Verifies retrieval with `dbutils.secrets.get`.

`03_source_to_bronze` reads this secret at load time — it never touches the variables directly.

> **Security note:** use your own credentials and keep them only in the Databricks secret — never commit or share real secrets in notebook source. The last cell of the notebook also stores a `gmail_api_key` secret; replace it with your own key or delete that cell if you don't need it.

## Job orchestration

Two jobs drive the chain (started by `NeoBank_Master` with `run_id` + `source_system`):

```
Neo_Bank_source_to_silver (job 301262740631864)
  └─ Get_list_of_tables        → 01_read_table_list
  └─ For Each (concurrency 4)  → over each dict in tables_metadata
       └─ run_job: Neo_bank_source_to_silver_inner (job 597428290683523)
            ├─ Get_list_of_parameters   → 02_read_table_parameters (table_metadata={{input}})
            ├─ Source_to_Bronze_ingestion → 03_source_to_bronze (table_parameters from task values)
            └─ Bronze_to_Silver          → 04_bronze_to_silver (table_parameters from task values)
```

- `run_id` (the job run id) is passed down as a job parameter and used for the `pipeline_runs` audit rows.
- Widgets are single-quoted dict strings parsed with `str.replace("'", '"')` + `json.loads` (or `ast.literal_eval` in 02).
- For **interactive testing**, you can paste the whole `tables_metadata` list into the `table_metadata` widget — `02_read_table_parameters` takes the first element.
- Notebooks 03 and 04 are designed to be called by the parent job; running them standalone without parameters raises errors by design.

## 01_read_table_list

- Reads `banking.metadata.tables`, filters `active_flag = true` and `source_system` matching the widget (valid values: `postgres`, `blob`, `silver`), ordered by `load_order`.
- Converts every row to a dict of strings (all 13 columns) and sets task value `tables_metadata` for the For Each loop.

## 02_read_table_parameters

- Parses the `table_metadata` widget (via `ast.literal_eval`, accepts a dict or a list of dicts), extracts `table_id`.
- Reads `banking.metadata.table_parameters` for that `table_id` and flattens it to a dict (`load_type`, `primary_key`, `watermark_column`), set as task value `table_parameters`.

## 03_source_to_bronze

- Writes an `INPROGRESS` audit row to `banking.metadata.pipeline_runs` (run_id, table_id, layer `Silver`, start time) — updated to `FAILED` with the error message if the load throws.
- Reads the last watermark from `banking.metadata.table_watermarks` (APPEND/MERGE only).
- **Postgres**: reads the connection JSON from the `banking-scope` secret, builds a JDBC URL (`sslmode=require`), `spark.read.jdbc` with an incremental `WHERE watermark_column > last_watermark` query when applicable; batch append into `banking.bronze.{table}` via `saveAsTable`.
- **Blob**: Auto Loader (`cloudFiles`, CSV, `header=true`) from `source_path`, schema at `/Volumes/banking/source/volume/_schema/{table_name}`, streaming Delta append to bronze with checkpoint at `/Volumes/banking/source/volume/_checkpoints/{table_name}` and `trigger(availableNow=True)`.
- Adds `insert_timestamp` to every row; bronze is append-only. Watermark is NOT advanced here (only after silver succeeds).

## 04_bronze_to_silver

- Reads `banking.{bronze_schema}.{table}` and filters rows above the last watermark for APPEND/MERGE.
- Adds audit columns `insert_timestamp` + `update_timestamp`.
- Creates `banking.silver` schema if needed; creates the silver table on first run (overwrite), then applies the load type:

| load_type | Behavior |
| --- | --- |
| `FULL` | Overwrite the silver table (with `overwriteSchema=true`) — full snapshot reload |
| `APPEND` | Append all (filtered) bronze rows |
| `MERGE` | Delta `MERGE` on `t.{primary_key} = s.{primary_key}` — `whenMatchedUpdateAll` + `whenNotMatchedInsertAll` (upsert) |

- After a successful load, advances the watermark: computes `max(watermark_column)` from the processed bronze data and `MERGE`s it into `banking.metadata.table_watermarks`.
- The `finally` block updates the `pipeline_runs` audit row (end time, status `SUCCESS`/`FAILED`, records processed, error message).

## Usage notes

- End-to-end, run the master job: `NeoBank_Master` → `Neo_Bank_source_to_silver` (for `source_system = blob` and `postgres`) → `Neo_Bank_silver_to_gold`.
- The parent job's `source_system` parameter decides which tables this chain processes (e.g. `blob` runs tables 5–6, `postgres` runs 1–4).
- To add a new source table: register it in `banking.metadata.tables` + `table_parameters` (see the metadata setup README) — no code change needed here.
