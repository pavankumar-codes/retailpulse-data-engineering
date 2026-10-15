# RetailPulse — End-to-End Retail Data Engineering Platform

RetailPulse is a hands-on data engineering project built around the Olist e-commerce dataset. The goal is to develop a reliable retail data platform while practicing data ingestion, profiling, data quality validation, relationship checks, and eventually lakehouse modeling and analytics.

## Project Goals

- Ingest and explore real-world retail CSV datasets.
- Profile datasets to understand their size, schema, missing values, duplicates, and data-quality issues.
- Validate candidate primary keys and foreign-key relationships.
- Build toward a layered lakehouse architecture using Databricks, PySpark, SQL, and Delta Lake.
- Document data-quality findings and design decisions as the project evolves.

## Technology Stack

- **Platform:** Databricks
- **Processing:** Apache Spark / PySpark
- **Languages:** Python and SQL
- **Source data:** Olist e-commerce dataset (CSV files)
- **Version control:** Git and GitHub
- **Planned next stages:** Delta Lake, Bronze/Silver/Gold layers, data modeling, and AWS integration

> The project is under active development. Planned technologies and stages are not yet represented as completed implementations.

## Dataset

The project currently uses nine CSV files:

1. `olist_customers_dataset.csv`
2. `olist_geolocation_dataset.csv`
3. `olist_order_items_dataset.csv`
4. `olist_order_payments_dataset.csv`
5. `olist_order_reviews_dataset.csv`
6. `olist_orders_dataset.csv`
7. `olist_products_dataset.csv`
8. `olist_sellers_dataset.csv`
9. `product_category_name_translation.csv`

The raw files are stored in the Databricks volume:

```text
/Volumes/retailpulse/dev/olist_raw/
```

The dataset includes customers, orders, order items, payments, reviews, products, sellers, geolocation, and product-category translations.

## Repository Structure

```text
retailpulse-data-engineering/
├── notebooks/
│   ├── 02_dataset_profiling.py
│   └── 03_key_relationship_validation.py
└── README.md
```

The notebook files are maintained as Databricks source notebooks. The repository structure may expand as the project progresses.

## Work Completed So Far

### 1. Environment and project setup

- Created the GitHub repository and connected it to the Databricks workspace.
- Set up the development catalog/schema and raw-data volume.
- Uploaded the nine Olist CSV files.
- Created notebooks for profiling and key/relationship validation.

### 2. Dataset profiling

Notebook: `notebooks/02_dataset_profiling.py`

- Loaded and profiled the nine datasets.
- Examined row counts, column counts, schemas, missing values, duplicate records, and selected domain-specific quality checks.
- Identified data-quality items that need attention, including duplicate geolocation records, missing values in product/review data, and timestamp gaps in some orders.

Selected observations from the profiling work:

- `customers`: 99,441 rows; `customer_id` had no NULLs or duplicate key groups.
- `orders`: 99,441 rows; `order_id` had no NULLs or duplicate key groups.
- `order_items`: 112,650 rows; composite key `(order_id, order_item_id)` had no duplicate groups.
- `order_payments`: 103,886 rows; composite key `(order_id, payment_sequential)` had no duplicate groups.
- `products`: 32,951 rows; `product_id` was unique and non-NULL.
- `sellers`: 3,095 rows; `seller_id` was unique and non-NULL.
- `geolocation`: 1,000,163 rows; exact duplicate rows were observed and require a deliberate handling strategy.
- The original review-file load produced suspicious column alignment and counts, which led to a separate CSV parsing investigation.

These are profiling observations from the development work, not a claim that all datasets have been cleaned.

### 3. Candidate key and relationship validation

Notebook: `notebooks/03_key_relationship_validation.py`

The notebook is designed to:

- Check candidate keys for NULL values and duplicate key groups.
- Support composite keys.
- Check foreign-key NULLs and unmatched child records using left-anti joins.
- Display validation results for configured relationships.

Relationships currently configured for validation:

- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `order_items.seller_id` → `sellers.seller_id`
- `order_payments.order_id` → `orders.order_id`
- `order_reviews.order_id` → `orders.order_id`

#### Review CSV parsing investigation

The review file contains comments that can include quotation marks and line breaks. The default CSV load produced suspicious rows, NULL keys, and misleading relationship-validation failures.

A revised Spark CSV reader configuration using multiline parsing and explicit quote handling returned **99,224 rows**, with zero NULLs in `review_id`, `order_id`, and `review_score` in the tested DataFrame. The `(review_id, order_id)` pair was unique across those 99,224 rows.

This is a promising correction, but the full validation notebook must be rerun and its final results reviewed. The difference from the earlier 104,162-row load is a parsing investigation finding; it should not be interpreted as proof that every source record was preserved until the load is fully reconciled.

## Running the Notebooks

1. Open the repository in the connected Databricks workspace.
2. Ensure the nine CSV files are available under `/Volumes/retailpulse/dev/olist_raw/`.
3. Open and run `notebooks/02_dataset_profiling.py` to review dataset profiles.
4. Open and run `notebooks/03_key_relationship_validation.py` to execute the candidate-key and relationship checks.
5. Review the displayed validation results and investigate any failures before treating them as confirmed data-quality issues.

The notebooks currently assume that the raw files are available at the Databricks volume path above.

## Current Status

- [x] Databricks and GitHub setup
- [x] Upload Olist source CSVs
- [x] Initial profiling of the nine datasets
- [x] Create candidate primary-key and foreign-key validation logic
- [x] Investigate multiline CSV parsing for order reviews
- [ ] Rerun and review the complete key/relationship validation results after the review parsing correction
- [ ] Define data-quality rules and document resolutions
- [ ] Design the retail data model / star schema
- [ ] Implement Bronze, Silver, and Gold layers with Delta Lake
- [ ] Add orchestration and cloud integration

## Next Steps

1. Rerun `03_key_relationship_validation.py` from the top and review the complete output.
2. Confirm the review-file load and investigate any remaining validation failures.
3. Document candidate keys, foreign keys, and accepted data-quality rules.
4. Design a retail analytical model and identify facts and dimensions.
5. Begin implementing the lakehouse layers with Delta tables.

## Notes

- A failed validation is a signal to investigate; it does not automatically mean the source data is wrong. Parsing errors, NULLs, duplicates, and genuine orphan records need to be distinguished.
- The project is being built incrementally. This README describes work completed or observed so far and marks future stages as planned rather than complete