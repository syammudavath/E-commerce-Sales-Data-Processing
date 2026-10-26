# Test and run results

These results come from running `notebooks/02_run_unit_tests` and `notebooks/01_ecommerce_sales_pipeline` on Databricks serverless compute (Python 3.12, pytest 8.4.2) on 2026-10-26. The target schema was `dev_test.ecommerce_sales`.

## 1. Unit tests: 29 passed, 0 failed

Command: `pytest tests -v --no-header -p no:cacheprovider`

```
============================= test session starts ==============================
collecting ... collected 29 items

../tests/test_transformations.py::test_to_snake_case[Sub-Category-sub_category] PASSED [  3%]
../tests/test_transformations.py::test_to_snake_case[Customer ID-customer_id] PASSED [  6%]
../tests/test_transformations.py::test_to_snake_case[ Price per product -price_per_product] PASSED [ 10%]
../tests/test_transformations.py::test_to_snake_case[_corrupt_record-_corrupt_record] PASSED [ 13%]
../tests/test_transformations.py::test_require_columns_raises PASSED     [ 17%]
../tests/test_transformations.py::test_trim_strings_blank_to_null PASSED [ 20%]
../tests/test_transformations.py::test_customer_name_cleaning[Gary567 Hansen-Gary Hansen] PASSED [ 24%]
../tests/test_transformations.py::test_customer_name_cleaning[Tam&^*ara Willing___)ham-Tamara Willingham] PASSED [ 27%]
../tests/test_transformations.py::test_customer_name_cleaning[Ad.       ..am Hart-Adam Hart] PASSED [ 31%]
../tests/test_transformations.py::test_customer_name_cleaning[  O'Neil Smith-Jones -O'Neil Smith-Jones] PASSED [ 34%]
../tests/test_transformations.py::test_customer_name_cleaning[Bi1l Eplett-Bill Eplett] PASSED [ 37%]
../tests/test_transformations.py::test_customer_name_cleaning[Helen Wa55erman-Helen Wasserman] PASSED [ 41%]
../tests/test_transformations.py::test_customer_name_cleaning[Rick Wi 4567@#$lson-Rick Wilson] PASSED [ 44%]
../tests/test_transformations.py::test_customer_name_cleaning[Paul  4567Van Hugh-Paul Van Hugh] PASSED [ 48%]
../tests/test_transformations.py::test_customer_name_cleaning[Anna H\xe4berlin-Anna H\xe4berlin] PASSED [ 51%]
../tests/test_transformations.py::test_customer_name_cleaning[Maribeth 5chnelling-Maribeth Schnelling] PASSED [ 55%]
../tests/test_transformations.py::test_customer_name_cleaning[Ji11 Stevenson-Jill Stevenson] PASSED [ 58%]
../tests/test_transformations.py::test_customer_name_cleaning[Jason Fortune--Jason Fortune] PASSED [ 62%]
../tests/test_transformations.py::test_customer_name_cleaning[@#$123-Unknown] PASSED [ 65%]
../tests/test_transformations.py::test_customer_name_cleaning[None-Unknown] PASSED [ 68%]
../tests/test_transformations.py::test_customer_missing_country PASSED   [ 72%]
../tests/test_transformations.py::test_customer_dedup_and_null_id PASSED [ 75%]
../tests/test_transformations.py::test_enriched_customers_drop_metadata PASSED [ 79%]
../tests/test_transformations.py::test_product_dedup_and_price_cast PASSED [ 82%]
../tests/test_transformations.py::test_order_date_parsing_and_quarantine PASSED [ 86%]
../tests/test_transformations.py::test_corrupt_record_quarantined PASSED [ 89%]
../tests/test_transformations.py::test_enriched_orders PASSED            [ 93%]
../tests/test_transformations.py::test_profit_aggregate PASSED           [ 96%]
../tests/test_transformations.py::test_empty_input PASSED                [100%]

============================== 29 passed in 29.26s ==============================
All unit tests passed
```

### What each test verifies

| Test | Cases | Verifies |
| --- | --- | --- |
| `test_to_snake_case` | 4 | Column names become snake_case; the `_` metadata prefix is kept |
| `test_require_columns_raises` | 1 | A missing required column raises `ValueError` |
| `test_trim_strings_blank_to_null` | 1 | Strings are trimmed; blank strings become NULL |
| `test_customer_name_cleaning` | 14 | Noise symbols, digits, look-alike characters, split words, Unicode letters, apostrophes and hyphens, all-noise names, NULL names |
| `test_customer_missing_country` | 1 | A missing country becomes `Unknown` |
| `test_customer_dedup_and_null_id` | 1 | Duplicate IDs collapse case-insensitively; NULL or blank IDs are dropped; a row with a known name is preferred |
| `test_enriched_customers_drop_metadata` | 1 | Lineage columns are not carried into the enriched table |
| `test_product_dedup_and_price_cast` | 1 | One row per product (deterministic pick); an invalid price becomes NULL; a missing category becomes `Unknown` |
| `test_order_date_parsing_and_quarantine` | 1 | `d/M/yyyy` dates parse; bad dates, NULL keys and NULL profit are quarantined with reasons; duplicate rows are removed |
| `test_corrupt_record_quarantined` | 1 | Malformed JSON is quarantined as `corrupt_record` |
| `test_enriched_orders` | 1 | Profit is rounded to 2 dp (positive and negative); customer and product attributes are joined; unknown customers and products are kept; no join fan-out; `DECIMAL(18,2)` type |
| `test_profit_aggregate` | 1 | Group sums, distinct order count, and the total is preserved |
| `test_empty_input` | 1 | Empty input gives empty output with no error |

## 2. Pipeline run and reconciliation checks: all passed

Output of `run_pipeline()`:

```
raw_orders                     9994
enriched_orders                9994
quarantined_orders             0
orders_unknown_product         204
orders_unknown_customer_name   137
total_profit                   278417.03
```

| Check | Result |
| --- | --- |
| No rows lost (distinct raw <= enriched + quarantined <= raw) | Passed: 9,994 = 9,994 + 0 |
| No join fan-out (`row_id` unique in `enriched_orders`) | Passed |
| Profit preserved (`enriched_orders` total = `agg_profit` total) | Passed: 278,417.03 |
| `product_id` unique in `enriched_products` | Passed: 1,818 products (from 1,851 raw rows) |
| `customer_id` unique in `enriched_customers` | Passed: 793 customers |

An independent cross-check, the sum of `ROUND(profit, 2)` taken directly from `raw_orders`, also gives 278,417.03.

## 3. SQL report results

### Profit by Year

| order_year | profit |
| --- | --- |
| 2014 | 39,185.75 |
| 2015 | 63,073.09 |
| 2016 | 65,073.23 |
| 2017 | 111,084.96 |

### Profit by Year + Product Category

| order_year | Furniture | Office Supplies | Technology | Unknown |
| --- | --- | --- | --- | --- |
| 2014 | -5,331.04 | 22,500.32 | 21,493.35 | 523.12 |
| 2015 | 3,027.18 | 24,519.39 | 34,943.36 | 583.16 |
| 2016 | 6,889.50 | 34,555.53 | 23,223.76 | 404.44 |
| 2017 | 3,041.54 | 44,273.12 | 63,281.79 | 488.51 |

### Profit by Customer

793 rows in total. The top 10 are:

| customer_id | customer_name | profit |
| --- | --- | --- |
| FH-14275 | Frank Hawley | 13,400.32 |
| TC-20980 | Tamara Chand | 8,981.32 |
| RB-19360 | Raymond Buch | 6,976.36 |
| SC-20095 | Sanjit Chand | 5,757.30 |
| HL-15040 | Hunter Lopez | 5,622.43 |
| PR-18880 | Patrick Ryan | 5,596.20 |
| AB-10105 | Adrian Barton | 5,444.97 |
| TA-21385 | Tom Ashbrook | 4,703.72 |
| CM-12385 | Christopher Martinez | 3,900.03 |
| PS-19045 | Penelope Sewall | 3,183.77 |

The largest losses are:

* Susan MacKendrick: -16,510.34
* Ed Jacobs: -6,964.03
* Cindy Stewart: -6,626.24

### Profit by Customer + Year

2,499 rows in total. The first rows are:

| customer_id | customer_name | order_year | profit |
| --- | --- | --- | --- |
| AA-10315 | Alex Avila | 2014 | 280.68 |
| AA-10315 | Alex Avila | 2015 | 7.00 |
| AA-10315 | Alex Avila | 2016 | -747.09 |
| AA-10315 | Alex Avila | 2017 | 96.50 |
| AA-10375 | Allen Armold | 2014 | 16.72 |
| AA-10375 | Allen Armold | 2015 | 69.03 |
| AA-10375 | Allen Armold | 2016 | 122.73 |
| AA-10375 | Allen Armold | 2017 | 68.92 |

## Reproducing

* On Databricks, run `notebooks/02_run_unit_tests` for the tests, or `notebooks/01_ecommerce_sales_pipeline` for everything.
* Optionally, in an external CI system, run `pip install -r requirements.txt`, then `pytest tests -v`. Do not pip install on Databricks; PySpark is already part of the runtime.
