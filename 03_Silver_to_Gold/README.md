# 03_Silver_to_Gold — Silver to Gold Transformations

The metadata-driven chain that builds the gold analytics layer in `banking.gold` from the silver tables. It reuses the same pattern as [02_source_to_silver](../02_source_to_silver/README.md): a job reads the active table list from `banking.metadata.tables` (with `source_system = 'silver'`) and a For Each loop calls the driver once per gold table.

## Structure

- `01_Silver_to_Gold_Driver` — the driver (see [How the driver works](#how-the-driver-works))
- `gold_transformations/` — one notebook per gold table, named exactly after the `table_name` in metadata:
  - `customer_360` (table_id 7)
  - `branch_performance` (8)
  - `transaction_channel_summary` (9)
  - `daily_bank_kpi` (10)
  - `risk_customer_summary` (11)

> Note: this driver was originally named `01_Silver_to_Golde_Driver` (typo); it was renamed to `01_Silver_to_Gold_Driver`, and the `Neo_Bank_silver_to_gold` job (920737228204538) task path was updated to the new name.

## Job orchestration

```
Neo_Bank_silver_to_gold (job 920737228204538, source_system = silver)
  └─ Get_list_of_tables        → 02_source_to_silver/01_read_table_list (reads tables 7–11)
  └─ For Each (concurrency 4)  → over each dict in tables_metadata
       └─ 01_Silver_to_Gold_Driver (run_id + table_metadata = {{input}})
            └─ dbutils.notebook.run → gold_transformations/{table_name}
```

Started by `NeoBank_Master` after both source-to-silver runs (blob + postgres) finish.

## How the driver works

1. **Widgets** — receives `run_id` and `table_metadata` (single-quoted dict string parsed with `replace("'", '"')` + `json.loads`). Both are required — running the driver standalone without parameters raises `ValueError` by design.
2. **Audit** — inserts or updates a `banking.metadata.pipeline_runs` row for (`run_id`, `table_id`) with `layer = 'Gold'`, status `INPROGRESS`.
3. **Dispatch** — derives the child notebook path as `gold_transformations/{table_name}`: the `table_name` in the metadata registry IS the notebook name. Adding a gold table therefore needs no driver change.
4. **Run child** — `dbutils.notebook.run(path, timeout_seconds=0)`; the child returns its row count via `dbutils.notebook.exit()`, which the driver captures.
5. **Audit update** — end time, status (`SUCCESS`/`FAILED`), record count, and error message are written back to `pipeline_runs`; if the child failed, the driver re-raises so the job task fails.

## Contract for gold_transformations notebooks

Every child notebook follows the same two-cell contract:

1. Rebuilds its table from silver with `CREATE OR REPLACE TABLE banking.gold.{name} AS ...` (gold is fully rebuilt every run — no incremental logic).
2. Ends with `dbutils.notebook.exit(str(count))` returning the row count for the audit table.

To add a new gold table: create the notebook in `gold_transformations/`, register the table in `banking.metadata.tables` (`source_system='silver'`, `target_layer='gold'`, `table_name` = notebook name, `gold_schema='gold'`). No code changes needed in the driver or job.

## The five gold tables

| table | Grain | Silver sources | What it produces |
| --- | --- | --- | --- |
| `customer_360` | one row per customer | customers, accounts, transactions, branches, credit_bureau_reports | Customer name + branch, account aggregates (`total_accounts`, `total_balance`), transaction aggregates, latest credit report per customer (latest `bureau_pull_date`: credit score, risk grade, external loans/overdue), and `customer_segment` by balance (HIGH ≥ 500k, MEDIUM ≥ 100k, else LOW) |
| `branch_performance` | one row per branch | branches, customers, accounts, transactions | Branch-level totals: customers, accounts, deposits (`total_balance`), transaction count and amount |
| `transaction_channel_summary` | one row per date × gateway × device | transactions, payment_gateway_logs (joined on `txn_id`) | Total / successful / failed transactions and average `processing_time_ms` per channel and device |
| `daily_bank_kpi` | one row per transaction date | transactions, customers, accounts, credit_bureau_reports | Daily transaction totals cross-joined with bank-wide metrics (total customers, accounts, deposits, avg credit score, high-risk customer count) — the bank-wide values repeat on every date |
| `risk_customer_summary` | one row per risk grade | credit_bureau_reports | Customers, avg credit score, total external loans, and total overdue amount per `risk_grade` |

These tables feed the NeoBank_Dashboard pages (Executive_Dashboard, Customer_insights, Branch_Performance, Gateway_and_Device).

## Usage notes

- Run end-to-end via `NeoBank_Master`; the driver is not meant to be run interactively (it fails without widgets by design).
- Gold tables are rebuilt from scratch on every run (`CREATE OR REPLACE`), so they always mirror the current silver state.
- Watermarks don't apply here — incremental logic lives only in the source-to-silver chain.
