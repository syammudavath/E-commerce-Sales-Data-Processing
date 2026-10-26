# Architecture

## Data flow

```
 Source files (data/)    Raw layer            Enriched layer                  Aggregate         Reports (SQL)
 --------------------    ---------            --------------                  ---------         -------------
 Orders.json    ------>  raw_orders     --->  enriched_orders  ------------>  agg_profit  --->  Profit by Year
 Customer.xlsx  ------>  raw_customers  --->  enriched_customers --^                            Profit by Year + Category
 Products.csv   ------>  raw_products   --->  enriched_products  --^                            Profit by Customer
                                              quarantine_orders (rejected rows)                 Profit by Customer + Year
```

All tables are Delta tables in `<catalog>.<schema>` (default `dev_test.ecommerce_sales`). Every table is rebuilt in overwrite mode, so re-runs are idempotent.

## Code layout

| Module | Responsibility |
| --- | --- |
| `scripts/transformations.py` | Pure PySpark functions: readers (explicit schemas), cleaning, de-duplication, validation, enrichment joins, aggregation. DataFrame in, DataFrame out. |
| `scripts/pipeline.py` | `PipelineConfig`, table writes, stage orchestration (`load_raw`, `build_enriched`, `build_aggregate`), `reconcile` checks, `run_pipeline` entry point. |
| `tests/conftest.py` | `spark` fixture: reuses the Databricks session, or creates a `local[2]` session locally. |
| `tests/test_transformations.py` | pytest unit tests for every transformation, on in-memory data. |
| `notebooks/01_ecommerce_sales_pipeline` | Widgets, runs the tests, runs the pipeline, previews tables, SQL reports. |
| `notebooks/02_run_unit_tests` | Runs the pytest suite on Databricks compute (standalone or via `%run`). |

Business rules live in `transformations` and orchestration lives in `pipeline`. Keeping them apart means the rules can be tested without touching tables, and the same code runs from a notebook, a Lakeflow Job, or local pytest.

## Stages

| Stage | Function | Output |
| --- | --- | --- |
| 1. Raw | `load_raw` | `raw_orders`, `raw_customers`, `raw_products`: source as-is, snake_case columns, plus `_source_file` and `_ingested_at` |
| 2. Enriched masters | `build_enriched` | `enriched_customers`, `enriched_products`: cleaned, typed, one row per key |
| 3. Enriched orders | `build_enriched` | `enriched_orders` (master table) and `quarantine_orders` |
| 4. Aggregate | `build_aggregate` | `agg_profit`: year x category x sub-category x customer |
| 5. Reconciliation | `reconcile` | Raises `DataQualityError` if any check fails |
| 6. Reports | SQL cells | Four roll-ups of `agg_profit` |

## Performance and scalability

* Customer and product tables are small, so they are broadcast for the joins and the orders table is never shuffled.
* Only built-in Spark expressions are used (no Python UDFs), so all logic runs in the Spark engine.
* De-duplication uses window functions; nothing is collected to the driver.
* Explicit schemas skip a schema-inference pass over the source files.
* `agg_profit` is pre-computed at the finest reporting grain, so the SQL reports are cheap roll-ups.
* Profit is stored as `DECIMAL(18,2)`, so sums are exact.
* For larger volumes: ingest incrementally with Auto Loader from a Unity Catalog volume, and use liquid clustering on `enriched_orders` by `order_year` and `customer_id`.

## Error handling

* `require_columns` fails fast if a source file is missing an expected column.
* The notebook checks that the three source files exist before running.
* Malformed JSON is captured in PERMISSIVE mode and quarantined. Bad dates use `try_to_timestamp` and bad prices use `try_cast`, so neither stops the run.
* Row-level problems go to `quarantine_orders`, with the reasons in `_dq_errors`.
* Reads and writes are logged. Failures are logged with a stack trace and re-raised.
* The pipeline notebook runs the unit tests first and stops if any of them fail.

## Spark Connect compatibility

The code uses only the DataFrame API: no RDDs, no `sparkContext`, no JVM access. It runs on serverless and on standard (shared) compute as well as classic clusters.
