# Data quality rules and assumptions

## Findings in the source data

| Dataset | Finding | Impact if ignored |
| --- | --- | --- |
| Orders.json | Dates are text in `d/M/yyyy` with single-digit day/month (`21/8/2016`, `1/1/2017`) | Wrong or NULL dates with a default parser |
| Orders.json | 204 orders (44 distinct product IDs) reference products missing from the product master | Inner join would drop their profit |
| Products.csv | 1,851 rows but 1,818 distinct `Product ID`s; duplicates differ in name/state but share category/sub-category | Join fan-out would double-count profit |
| Customer.xlsx | 70 names contain noise: digits, symbols, look-alike characters (`Gary567 Hansen`, `Bi1l Eplett`, `Rick Wi 4567@#$lson`, `Ad.   ..am Hart`) | Wrong names in reports |
| Customer.xlsx | Some names are blank | NULL names in reports |
| Customer.xlsx | Names with Unicode letters (`Anna Häberlin`, `Peter Bühler`) | A naive `[A-Za-z]` filter would corrupt them |

## Rules applied

| Rule | Where | Behaviour |
| --- | --- | --- |
| Trim strings, blank -> NULL | all enriched tables | `trim_strings` |
| Keys upper-cased | customers, products, orders | Case-insensitive matching |
| Mandatory order fields | `clean_orders` | Null `order_id` / `customer_id` / `product_id` / `profit`, unparseable `order_date`, or corrupt JSON -> `quarantine_orders` with reasons in `_dq_errors` |
| Exact duplicate order lines | `clean_orders` | Dropped by `row_id` |
| One row per product | `enrich_products` | Deterministic pick: first by `product_name`, then `state` |
| One row per customer | `enrich_customers` | Prefer a row with a known name |
| Non-numeric price | `enrich_products` | `try_cast` -> NULL (no failure) |
| Missing dimension attributes | `build_enriched_orders` | LEFT joins; `Unknown` for name, country, category, sub-category |
| Profit rounding | `build_enriched_orders` | `round(profit, 2)` stored as `DECIMAL(18,2)` |

## Customer name cleaning (`clean_name`)

1. Restore look-alike characters inside a word: `1`->`l`, `0`->`o`, `5`->`s`, `@`->`a`, `!`->`i`. This covers a single character between letters (`Bi1l`), a doubled one (`Wa55erman`, `Ji11`), and one at the start of a lowercase word (`5chnelling`).
2. Handle junk runs. A junk run is a stretch of non-letters that contains at least one digit or symbol; spaces inside it count as part of the run.
   * If a lowercase letter follows the run, the run split a word, so it is removed: `Wi 4567@#$lson` becomes `Wilson`.
   * Otherwise the run separated two words, so it becomes a space: `Gary567 Hansen` becomes `Gary Hansen`.
3. Strip leading and trailing apostrophes or hyphens, then collapse whitespace.
4. Keep Unicode letters, apostrophes and hyphens (`O'Neil Smith-Jones`, `Häberlin`).
5. If nothing is left, set the name to `Unknown`.

Examples from the data:

| Raw | Cleaned |
| --- | --- |
| `Gary567 Hansen` | Gary Hansen |
| `Tam&^*ara Willing___)ham` | Tamara Willingham |
| `Ad.       ..am Hart` | Adam Hart |
| `Bi1l Eplett` | Bill Eplett |
| `Helen Wa55erman` | Helen Wasserman |
| `Rick Wi 4567@#$lson` | Rick Wilson |
| `Tho   12 =-.mas Boland` | Thomas Boland |
| `C@thy Armstrong` | Cathy Armstrong |
| `Anna Häberlin` | Anna Häberlin |

## Assumptions

* `Profit` from the source is authoritative. It is not recomputed from price, quantity and discount.
* "Year" means the calendar year of `Order Date`.
* Orders whose product is missing from the master are valid sales, so they are kept under `Unknown`.

## Known limitations

* A stray space inside a word with no other noise (`Kat rina Willman`) can't be detected without a name dictionary, so it is left as-is.
* Letters that were replaced entirely by digits can't be recovered (`Bi 8761l Shonely` becomes `Bil Shonely`).
* The look-alike rule assumes that digits sitting inside a name are typos; real names with digits would be altered.

## Reconciliation checks (run after every load)

1. distinct raw rows <= enriched + quarantined <= raw rows. Only exact duplicates may be dropped.
2. `row_id` is unique in `enriched_orders`, so the joins did not fan out.
3. Total profit in `enriched_orders` equals total profit in `agg_profit`.
4. `product_id` is unique in `enriched_products` and `customer_id` is unique in `enriched_customers`.

If any check fails, `DataQualityError` is raised.
