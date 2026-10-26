# Data dictionary

All tables live in `<catalog>.<schema>` (default `dev_test.ecommerce_sales`). The row counts are from the validated run on 2026-10-26.

## Raw layer

The source data is loaded as-is. Column names are converted to snake_case, because Delta does not allow spaces, and two lineage columns are added.

| Table | Source | Rows |
| --- | --- | --- |
| `raw_orders` | `Orders.json` | 9,994 |
| `raw_customers` | `Customer.xlsx` (sheet `Worksheet`) | 793 |
| `raw_products` | `Products.csv` | 1,851 |

Columns in all raw tables:

| Column | Description |
| --- | --- |
| `_source_file` | Path of the source file |
| `_ingested_at` | Load timestamp |

`raw_orders` also has `_corrupt_record`, which holds the raw text of any JSON record that could not be parsed.

## `enriched_customers`

One row per `customer_id` (793 rows).

| Column | Type | Description |
| --- | --- | --- |
| `customer_id` | STRING | Customer key, trimmed and upper-cased |
| `customer_name` | STRING | Cleaned name (see `data_quality.md`); `Unknown` if missing |
| `email` | STRING | Email |
| `phone` | STRING | Phone (kept as text; formats vary) |
| `address` | STRING | Postal address (may contain line breaks) |
| `segment` | STRING | Consumer / Corporate / Home Office |
| `country` | STRING | Country; `Unknown` if missing |
| `city`, `state`, `region` | STRING | Location |
| `postal_code` | STRING | Kept as text to preserve leading zeros |

## `enriched_products`

One row per `product_id` (1,818 rows).

| Column | Type | Description |
| --- | --- | --- |
| `product_id` | STRING | Product key, trimmed and upper-cased |
| `category` | STRING | Furniture / Office Supplies / Technology; `Unknown` if missing |
| `sub_category` | STRING | e.g. Chairs, Phones, Binders; `Unknown` if missing |
| `product_name` | STRING | Product name |
| `state` | STRING | State from the source |
| `price_per_product` | DOUBLE | Unit price; NULL if not numeric |

## `enriched_orders` (master table)

One row per order line (9,994 rows).

| Column | Type | Description |
| --- | --- | --- |
| `row_id` | BIGINT | Unique order line ID |
| `order_id` | STRING | Order ID (an order can have several lines) |
| `order_date` | DATE | Parsed from `d/M/yyyy` |
| `order_year` | INT | Calendar year of `order_date` |
| `ship_date` | DATE | Parsed from `d/M/yyyy` |
| `ship_mode` | STRING | Standard Class / Second Class / First Class / Same Day |
| `customer_id` | STRING | Customer key |
| `customer_name` | STRING | From `enriched_customers`; `Unknown` if not found or blank |
| `country` | STRING | From `enriched_customers` |
| `product_id` | STRING | Product key |
| `category` | STRING | From `enriched_products`; `Unknown` if the product is not in the master |
| `sub_category` | STRING | From `enriched_products`; `Unknown` if the product is not in the master |
| `quantity` | INT | Units |
| `price` | DOUBLE | Line price |
| `discount` | DOUBLE | Discount rate (0 to 1) |
| `profit` | DECIMAL(18,2) | Source profit rounded to 2 decimal places |

## `agg_profit`

The grain is `order_year` x `category` x `sub_category` x `customer_id` (8,129 rows).

| Column | Type | Description |
| --- | --- | --- |
| `order_year` | INT | Year |
| `category` | STRING | Product category |
| `sub_category` | STRING | Product sub-category |
| `customer_id` | STRING | Customer key |
| `customer_name` | STRING | Customer name |
| `total_profit` | DECIMAL(18,2) | Sum of `profit` |
| `order_count` | BIGINT | Distinct orders |
| `total_quantity` | BIGINT | Sum of quantity |

## `quarantine_orders`

Orders that failed validation (currently 0 rows). The table keeps every raw order column, plus `_dq_errors`, a comma-separated list of reasons:

* `null_order_id`
* `null_customer_id`
* `null_product_id`
* `invalid_order_date`
* `null_profit`
* `corrupt_record`
