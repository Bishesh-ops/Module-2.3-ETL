# Assignment 2.3 – Retail daily ETL (Airflow → Snowflake Gold)

Four daily files → Bronze → Silver → Gold, loaded by one DAG, `retail_daily`.

```
init_tables → wait_for_files → ingest_file (one task per file, 4) → silver (clean, dedupe, check, quarantine)
            → gold (dimensions, then facts) → reconcile
```

## The four files (`data/incoming/`)
| File | Feeds | Load type |
|---|---|---|
| `item_master_YYYYMMDD.csv` | DIM_ITEM, DIM_SUBCLASS | ITEM = SCD2, SUBCLASS = SCD1 |
| `store_master_YYYYMMDD.csv` | DIM_LOCN, DIM_DISTRICT | LOCN = SCD2, DISTRICT = SCD1 |
| `pos_sales_YYYYMMDD.csv` | FCT_SALES | delete + insert per day |
| `inventory_snapshot_YYYYMMDD.csv` | FCT_INVENTORY | positional (rows closed, never deleted) |

`python tools/make_data.py` regenerates the 20 files (5 days × 4). Planted events: SCD1 rename 09-14, store 1004 changes district 09-15,
five broken sales rows in the 09-15 file, item I007 changes subclass 09-16, two inventory positions vanish 09-16.

## Layers
- **Bronze** – the file copied untouched to `BRONZE_ROOT/<file>/ds=<date>/`, then loaded as text into `stg_*` (replaced per run date).
- **Silver** – `silver_*`: typed rows of the run date, each with `reject_cd` (NULL = good) and, for dims/inventory, an `action_cd` (NEW / CHANGE / CORRECT / NOCHANGE / STALE). Rejected rows are copied to `rej_row` with the reason.
- **Gold** – the Assignment 2.1 tables. Parents the files don't provide (chain, area, region, department, category, brand, supplier, location type) hold one `UNKNOWN`/`STORE` seed row. The calendar tables get the run date's year/month/week/day.

## Quarantine reasons
`NULL_ITEM`, `NULL_KEY`, `NEGATIVE_QTY`, `BAD_QTY`, `BAD_AMOUNT`, `DUPLICATE`, `ORPHAN_ITEM`, `ORPHAN_STORE`, `NULL_ATTRIBUTE`, `BAD_ROW_SHAPE`.
Bad rows never stop the run; they are counted in the `reconcile` log and in `rej_row`.

## Reconciliation (last task, fails the DAG)
file rows = staged + bad-shape rows · sales lines in file = Gold + quarantined · **net sales in file = Gold + quarantined** · inventory positions match Gold · open inventory rows = positions in the snapshot.

## How to run
1. Compose mounts: `./dags`, `./data`, `./include:/opt/airflow/include`, `./tests:/opt/airflow/tests`; env `PYTHONPATH: /opt/airflow/include`; `_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-snowflake pytest`.
2. Airflow connection `snowflake_default` holds credentials, warehouse, database and schema. Nothing else does.
3. Put the files in `./data/incoming/`. Settings can be changed with env `ETL_<NAME>` or Airflow Variable `etl_<name>` (`include/config.py`).
4. Unpause `retail_daily`. `init_tables` creates every table that is missing.

## How to backfill (five days)
`start_date` 2026-09-13, `end_date` 2026-09-17, `catchup=True`, `max_active_runs=1`: unpausing runs 09-13 … 09-17 one after another in one go.
Redo a range: `airflow backfill create --dag-id retail_daily --from-date 2026-09-13 --to-date 2026-09-17`.
One day by hand: trigger with config `{"run_date": "2026-09-15"}`.

## Safe to run twice
- staging and silver are replaced per run date; `fct_sales` is deleted and re-inserted per day
- SCD2: an unchanged row does nothing; a change closes the open version and opens one; an older day re-run is `STALE` and ignored
- SCD1: overwritten only when the incoming day is not older than the value already applied
- inventory: compared with the row covering the date; closed, never deleted
- tests re-run the whole backfill and one earlier day and assert every count and total is unchanged.

## Tests
`python -m pytest tests` (logic on SQLite, uses the same SQL files). In the container: `pytest /opt/airflow/tests` also checks the DAG parses, has no cycles, retries are set, and nothing is hard-coded.

## Known limitations
- Tested on SQLite; **not run on a real Snowflake account here** – expect to fix small dialect issues on the first run.
- All four files are expected every day (the store master is "weekly" in the slides; we deliver it daily).
- No customer feed: `dim_customer` is empty and `fct_sales.customer_key` is null. Unknown parents use seed rows.
- The calendar only holds the days that have been run. Reject rows for a day are rebuilt when that day is re-run.
- An inventory position whose row is quarantined keeps its previous stock row until a good row arrives.

## Evidence to attach
Grid view after the backfill (all green) and the `reconcile` task log of each day.
<img width="1911" height="890" alt="completed_dag_backfills" src="https://github.com/user-attachments/assets/22967ca1-ed0d-4e41-9ea1-eb22f4ed7d92" />
<img width="1066" height="718" alt="retail_daily-graph" src="https://github.com/user-attachments/assets/23e443d4-e34e-4707-ab20-93f95a7a04c8" />


