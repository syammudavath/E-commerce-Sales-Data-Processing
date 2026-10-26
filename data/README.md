# data/

Source datasets for the pipeline. Place these three files here:

| File | Format | Description |
| --- | --- | --- |
| `Orders.json` | Multi-line JSON array | Order line items (9,994 rows) |
| `Customer.xlsx` | Excel, sheet `Worksheet` | Customer master (793 rows) |
| `Products.csv` | CSV with header | Product master (1,851 rows) |

The pipeline reads from this folder by default (`data_path` widget in `notebooks/01_ecommerce_sales_pipeline`). To read from a Unity Catalog volume instead, set `data_path` to e.g. `/Volumes/<catalog>/<schema>/<volume>/ecommerce`.

If the data is confidential, do not commit it: add `data/*` (except this README) to `.gitignore` and load the files from a volume.
