# Neo_Bank Project

An end-to-end banking analytics pipeline on Databricks that ingests data from two sources — a Neon Postgres source database ("neondbe", schema "banking") and cloud blob storage — into a medallion architecture (bronze → silver → gold) using a metadata-driven approach, and serves the results through the NeoBank_Dashboard and a Genie agent for natural-language analytics.

## Architecture

```
Sources:
  - Neon Postgres (banking schema: customers, accounts, transactions, branches)
  - Blob storage (credit_bureau_reports, payment_gateway_logs)
        ↓
  Bronze layer  →  Silver layer  →  Gold layer (banking.gold)  →  NeoBank_Dashboard + Genie Agent
```

## Project Structure

### 01_Metada_setup/
Sets up and validates the metadata tables that drive ingestion. See [01_Metada_setup/README.md](#file-3807176542113240) for the full setup guide (control-table DDL, blob landing volume, registered tables and load parameters).

- `01_setup_metadata` — creates/populates the metadata tables (renamed from `)1_setup_metadata`; no jobs referenced it by path)
- `02_check_metada` — validates the metadata setup

### 02_source_to_silver/
Source-to-silver ingestion chain. See [02_source_to_silver/README.md](#file-3807176542113241) for the full chain guide (secret setup, job orchestration, per-notebook details, and load-type behavior).

- `00_secret_scope` — defines Postgres/Neon connection variables (host, port, database, user, password) and stores them as a JSON secret (scope `banking-scope`, key `postgres-connection-json`)
- `01_read_table_list` — reads active tables from `banking.metadata.tables` (filtered by `active_flag` + `source_system`, where `source_system` is one of `postgres`, `blob`, `silver`) and sets the `tables_metadata` task value
- `02_read_table_parameters` — reads per-table parameters from `banking.metadata.table_parameters` (`load_type`, `primary_key`, `watermark_column`) and sets the `table_parameters` task value
- `03_source_to_bronze` — loads source tables into the bronze layer (JDBC batch reads for Postgres, Auto Loader streaming for blob sources — see [Source → Bronze](#source--bronze-03_source_to_bronze))
- `04_bronze_to_silver` — applies load logic (MERGE/APPEND/FULL) to upsert bronze data into silver

### 03_Silver_to_Gold/
Silver-to-gold transformation chain. See [03_Silver_to_Gold/README.md](#file-3807176542113242) for the full guide (driver mechanics, the child-notebook contract, and all five gold tables).

- `01_Silver_to_Gold_Driver` — driver notebook (receives `run_id` and `table_metadata`; meant to be called by a parent job For Each — errors by design if run without parameters; renamed from `01_Silver_to_Golde_Driver`)
- `gold_transformations/` — per-table transformation notebooks:
  - `customer_360`
  - `branch_performance`
  - `transaction_channel_summary`
  - `daily_bank_kpi`
  - `risk_customer_summary`

### NeoBank_Dashboard.lvdash.json
AI/BI dashboard with pages: `Executive_Dashboard`, `Customer_insights`, `Branch_Performance`, `Gateway_and_Device`.

### Banking Performance and Risk Analytics (Genie Agent)
A Genie agent — [Banking Performance and Risk Analytics](#genie-01f1d1202f351646a7c90bc79d1982c3) — for natural-language Q&A over the banking gold tables. You can ask it about:

- Branch-level performance — customer volume, account count, deposits, transaction totals
- Customer profiles — segments, account activity, balances, credit scores, risk grades
- Daily bank-wide KPIs — total balance, transaction trends, average credit score, high-risk customer counts
- Customer risk — risk grades, average credit scores, loan exposure, overdue amounts
- Transaction activity by gateway and device — success rates and processing times

Not covered: individual transaction details, loan-level breakdowns, predictive/forecast analytics, or cross-bank benchmarks.

**Tables it uses** (all in `banking.gold`):

| Table | Domain |
| --- | --- |
| `banking.gold.branch_performance` | Branch-level performance metrics (customers, accounts, deposits, transaction volumes) |
| `banking.gold.customer_360` | Comprehensive customer view (balances, activity, credit scores, risk grades, segments) |
| `banking.gold.daily_bank_kpi` | Daily bank-wide KPIs (customers, accounts, balances, transactions, risk) |
| `banking.gold.risk_customer_summary` | Customers by risk grade (credit scores, external loans, overdue amounts) |
| `banking.gold.transaction_channel_summary` | Transactions by date, gateway, and device (success/failure rates, processing times) |

#### Recreating this Genie space

Genie spaces are workspace objects — they cannot be exported as files or cloned from this repo. To recreate this space in another workspace (after running the pipeline so the gold tables exist):

1. In the target workspace, go to **Genie** (sidebar) → **Create Genie Space**, and name it `Banking Performance and Risk Analytics`.
2. Add the five `banking.gold` tables listed above.
3. Paste this text into the space's description/instructions field:

   ```text
   This Agent provides comprehensive insights into banking performance, customer risk, and transaction activity by analyzing branch, customer, daily bank, risk, and channel metrics.

   Capabilities:
   - Evaluate branch-level performance including customer volume, account count, deposits, and transaction totals.
   - Profile customers by segment, account activity, balances, credit scores, and risk grades.
   - Track daily bank-wide KPIs such as total balance, transaction trends, credit score averages, and high-risk customer counts.
   - Analyze customer risk by aggregating risk grades, average credit scores, loan exposure, and overdue amounts.
   - Review transaction activity by gateway and device, including success rates and processing times.

   Limitations:
   - Does not provide individual transaction details or loan-level breakdowns.
   - Lacks predictive analytics or forecast models for future performance.
   - No cross-bank or external benchmark comparisons; data is limited to internal metrics.
   ```

4. Ask a test question (e.g. "Which branch has the highest deposits?") to verify the space answers correctly.
5. Share it: users need at least **Can Run** on the Genie space and `SELECT` on the gold tables.

## How It Works (metadata-driven)

### `banking.metadata.tables`

| Column | Description |
| --- | --- |
| `table_id` | Unique table identifier |
| `table_name` | Target table name |
| `source_system` | Source system identifier |
| `source_schema` / `source_table` | Source location in Postgres |
| `target_layer` | Target layer in the medallion architecture |
| Bronze / silver / gold schemas | Target schema names per layer |
| `active_flag` | Whether the table is actively ingested |
| `load_order` | Order in which tables are loaded |

### `banking.metadata.table_parameters`

| Column | Description |
| --- | --- |
| `table_id` | Foreign key to `metadata.tables` |
| `parameter_name` | Parameter key (e.g., `load_type`, `primary_key`, `watermark_column`) |
| `parameter_value` | Parameter value |

Supported `load_type` values: `MERGE`, `APPEND`, `FULL`.

### Registered tables

- Active Postgres source tables: `customers` (1), `accounts` (2), `transactions` (3), `branches` (4)
- Blob-sourced tables: `credit_bureau_reports` (5), `payment_gateway_logs` (6)
- Gold target tables: `customer_360` (7), `branch_performance` (8), `transaction_channel_summary` (9), `daily_bank_kpi` (10), `risk_customer_summary` (11)

### Source → Bronze (`03_source_to_bronze`)

Receives `table_metadata`, `table_parameters`, and `run_id` as widgets, and branches on `source_system`:

- **Postgres (JDBC batch)** — reads the connection JSON from the `banking-scope` secret scope (`postgres-connection-json`), builds a JDBC URL (`sslmode=require`), and reads via `spark.read.jdbc`. For `APPEND`/`MERGE` loads with a `watermark_column`, only rows newer than the last watermark (from `banking.metadata.table_watermarks`) are pulled; written to bronze with a batch append (`saveAsTable`).
- **Blob (Auto Loader streaming)** — reads CSV files from the `source_path` in the table metadata using Auto Loader (`cloudFiles.format=csv`, `header=true`), with inferred schema stored at `/Volumes/banking/source/volume/_schema/{table_name}`. Written to bronze as a streaming Delta append with checkpointing at `/Volumes/banking/source/volume/_checkpoints/{table_name}` and `trigger(availableNow=True)` for a bounded per-run batch.

Additional behavior:

- Every load is audited in `banking.metadata.pipeline_runs` (run_id, table_id, layer, start/end time, status `INPROGRESS` → `FAILED` on error, record count, error message).
- All rows get an `insert_timestamp` column; bronze tables (`banking.bronze.{table_name}`) are append-only.
- Watermarks are advanced only after the silver load completes.

### Job flow

1. `01_read_table_list` reads the active table list from metadata.
2. A For Each loop passes each table's metadata dict to `02_read_table_parameters`.
3. `03_source_to_bronze` and `04_bronze_to_silver` perform the load.
4. The `03_Silver_to_Gold` chain follows the same driver pattern.

Notebooks communicate via widgets and task values (`tables_metadata`, `table_parameters`); for interactive testing, the whole list can be passed into the widget.

## Dashboard

NeoBank_Dashboard provides executive KPIs (total customers, deposits, transactions, high-risk customers), monthly transaction trends, success vs. failure share, and per-page views for customer insights, branch performance, and gateway/device analysis.

For conversational access to the same metrics, use the Genie agent [Banking Performance and Risk Analytics](#genie-01f1d1202f351646a7c90bc79d1982c3).
