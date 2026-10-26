# E-commerce Sales Data Processing

A PySpark pipeline on Databricks that processes e-commerce sales data (orders, customers, products). It:

1. Loads the raw source data.
2. Cleans and enriches it.
3. Builds a master orders table and a profit aggregate.
4. Produces SQL reports for business stakeholders.

All transformations are written in PySpark and covered by pytest unit tests. SQL is used only for the final reports.

**Test status:** 29 of 29 unit tests passed, and all reconciliation checks passed. Total profit is 278,417.03. See [docs/test_results.md](docs/test_results.md) for the full pytest output, the pipeline summary and the SQL report results.

## Repository structure

```
E-commerce-Sales-Data-Processing/
|-- data/                          Source datasets (Orders.json, Customer.xlsx, Products.csv)
|   `-- README.md
|-- docs/
|   |-- architecture.md            Data flow, code layout, performance, error handling
|   |-- data_dictionary.md         Every table and column
|   |-- data_quality.md            Data issues found, cleaning rules, assumptions, checks
|   `-- test_results.md            Unit test output, reconciliation checks, SQL report results
|-- notebooks/
|   |-- 01_ecommerce_sales_pipeline   Main entry point: tests -> pipeline -> SQL reports
|   `-- 02_run_unit_tests             Runs the pytest suite on Databricks
|-- scripts/
|   |-- __init__.py
|   |-- transformations.py         Pure PySpark transformation functions
|   `-- pipeline.py                Orchestration, table writes, reconciliation
|-- tests/
|   |-- conftest.py                Spark fixture (Databricks or local)
|   `-- test_transformations.py    Unit tests
|-- .gitignore
|-- README.md
`-- requirements.txt               Local dev / CI dependencies
```

## Tasks and where they are implemented

| Task | Implementation | Output table |
| --- | --- | --- |
| Raw table for each source | `pipeline.load_raw`, `transformations.read_*` | `raw_orders`, `raw_customers`, `raw_products` |
| Enriched customers and products | `transformations.enrich_customers`, `enrich_products` | `enriched_customers`, `enriched_products` |
| Enriched orders: order info, profit (2 dp), customer name and country, product category and sub-category | `transformations.clean_orders`, `build_enriched_orders` | `enriched_orders` (+ `quarantine_orders`) |
| Aggregate: profit by year, category, sub-category, customer | `transformations.build_profit_aggregate` | `agg_profit` |
| SQL: profit by year; year + category; customer; customer + year | SQL cells in `notebooks/01_ecommerce_sales_pipeline` | - |
| Unit tests | `tests/test_transformations.py` | - |

## Running on Databricks

1. Put the repo in a Databricks Git folder (see [Pushing to Git](#pushing-to-git)) or in a workspace folder.
2. Copy `Orders.json`, `Customer.xlsx` and `Products.csv` into `data/`, or into a Unity Catalog volume.
3. Open `notebooks/01_ecommerce_sales_pipeline` and set the widgets:

   | Widget | Default | Meaning |
   | --- | --- | --- |
   | `catalog` | `dev_test` | Target catalog |
   | `schema` | `ecommerce_sales` | Target schema (created if missing) |
   | `data_path` | `<repo>/data` | Folder with the three source files (a workspace path or `/Volumes/...`) |

4. Click **Run all**. The notebook runs the unit tests first (via `%run ./02_run_unit_tests`) and stops if any fail. It then builds every table, runs the reconciliation checks and outputs the four SQL reports.

It works on serverless, standard and classic compute. The code uses only the DataFrame API, so it is Spark Connect compatible. Reading `Customer.xlsx` uses the Databricks native Excel reader.

To run only the tests, open `notebooks/02_run_unit_tests` and click **Run all**.

### Scheduling

Create a Lakeflow Job with a notebook task pointing to `notebooks/01_ecommerce_sales_pipeline`, and pass `catalog`, `schema` and `data_path` as job parameters. If you use a Git folder as the job source, each run uses the committed code.

## Running tests locally

You need Python 3.10+ and Java 17 (for local Spark).

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest tests -v
```

The `spark` fixture in `tests/conftest.py` creates a `local[2]` SparkSession. The tests use in-memory data only, so no source files or Databricks connection are needed.

Do not `pip install -r requirements.txt` on Databricks: PySpark is already part of the runtime.

## Pushing to Git

Option A: create a Git folder in Databricks (recommended).

1. Create an empty repository in GitHub, GitLab or Azure DevOps.
2. In Databricks, go to **Settings > Linked accounts** and add your Git credentials (a personal access token).
3. In **Workspace**, choose **Create > Git folder** and paste the repository URL.
4. Copy or move the contents of this folder into the Git folder.
5. Open the Git dialog (the branch name button), review the changes, write a commit message, then **Commit & Push**.

Option B: push from your machine.

1. Download the folder from the workspace with **Export > Source File** or **DBC/ZIP**, or with `databricks workspace export-dir`.
2. Push it from your machine:

```bash
cd ecommerce-sales-pipeline
git init -b main
git add .
git commit -m "E-commerce sales data processing pipeline"
git remote add origin https://github.com/<your-user>/ecommerce-sales-pipeline.git
git push -u origin main
```

### Committing notebook outputs

By default Databricks commits notebooks without their cell outputs. To keep the test results and query results in Git:

1. Store the notebooks in **`.ipynb` (Jupyter) format**. Use **File > Change notebook format**, or set the default format to IPYNB in **Settings > Developer**.
2. Allow output commits by adding a `.databricks/commit_outputs` file to the repo root that lists the notebooks whose outputs may be committed. For example:

```
notebooks/01_ecommerce_sales_pipeline.ipynb
notebooks/02_run_unit_tests.ipynb
```

3. When committing, select **Export cell outputs** in the commit dialog. This option only appears for `.ipynb` notebooks covered by `commit_outputs`.

Outputs can contain data. If the data is sensitive, do not commit outputs, and do not commit `data/`.

## Data quality summary

* Products have duplicate IDs (1,851 rows, 1,818 IDs). They are de-duplicated before the join so profit is not counted twice.
* 204 orders (44 product IDs) are not in the product master. They are kept with category `Unknown`, so total profit is preserved.
* 70 customer names contain noise. They are cleaned: for example `Bi1l Eplett` becomes Bill Eplett, `Helen Wa55erman` becomes Helen Wasserman, `Rick Wi 4567@#$lson` becomes Rick Wilson, and Unicode names such as Anna Häberlin are kept.
* Invalid orders (null keys or profit, bad dates, corrupt JSON) go to `quarantine_orders` with a reason.
* Reconciliation checks after every load: no rows lost, no join fan-out, profit totals equal across tables, unique keys.

See [docs/data_quality.md](docs/data_quality.md) for the full rules and limitations.

## Testing

`tests/test_transformations.py` has 29 test cases (parametrised) covering:

* Column standardisation.
* Required-column validation and blank-to-NULL trimming.
* 14 customer-name cleaning cases: noise, look-alike characters, split words, Unicode, hyphens and apostrophes, all-noise names, NULL names.
* Missing country, duplicate and NULL customer IDs, and that metadata columns are dropped.
* Product de-duplication and invalid prices.
* Date parsing and quarantine reasons, including multiple errors on one row and corrupt records.
* Rounding to 2 decimals for positive and negative profit, unknown customers and products, no join fan-out, the decimal type.
* Aggregate sums, distinct order count and total preservation.
* Empty input.

## Results

These results come from the validated run on 2026-10-26, on table `dev_test.ecommerce_sales`:

| Metric | Value |
| --- | --- |
| Orders loaded / enriched | 9,994 / 9,994 |
| Quarantined orders | 0 |
| Orders with unknown product | 204 (44 product IDs) |
| Customers / products (after de-duplication) | 793 / 1,818 |
| `agg_profit` rows | 8,129 |
| Total profit | 278,417.03 |

Profit by year:

| Year | Profit |
| --- | --- |
| 2014 | 39,185.75 |
| 2015 | 63,073.09 |
| 2016 | 65,073.23 |
| 2017 | 111,084.96 |

Profit by year and category:

| Year | Furniture | Office Supplies | Technology | Unknown |
| --- | --- | --- | --- | --- |
| 2014 | -5,331.04 | 22,500.32 | 21,493.35 | 523.12 |
| 2015 | 3,027.18 | 24,519.39 | 34,943.36 | 583.16 |
| 2016 | 6,889.50 | 34,555.53 | 23,223.76 | 404.44 |
| 2017 | 3,041.54 | 44,273.12 | 63,281.79 | 488.51 |

The most profitable customers are:

* Frank Hawley: 13,400.32
* Tamara Chand: 8,981.32
* Raymond Buch: 6,976.36

The largest losses are:

* Susan MacKendrick: -16,510.34
* Ed Jacobs: -6,964.03
* Cindy Stewart: -6,626.24

Re-run `notebooks/01_ecommerce_sales_pipeline` to refresh these numbers. The notebook outputs then hold the latest values.

## Documentation

* [docs/architecture.md](docs/architecture.md): data flow, code layout, performance and error handling.
* [docs/data_dictionary.md](docs/data_dictionary.md): every table and column.
* [docs/data_quality.md](docs/data_quality.md): data issues, cleaning rules, assumptions and reconciliation checks.
* [docs/test_results.md](docs/test_results.md): unit test output, pipeline run summary and SQL report results.
