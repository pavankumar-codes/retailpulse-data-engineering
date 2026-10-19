# RetailPulse — End-to-End Retail Data Engineering Platform

RetailPulse is a hands-on data engineering project built around the Olist e-commerce dataset. The goal is to develop a reliable, production-oriented retail data platform using Databricks, PySpark, SQL, Delta Lake, and AWS.

The project is being developed incrementally, following a 45-day roadmap. It focuses on data ingestion, profiling, data quality, relationship validation, lakehouse architecture, analytical modelling, incremental processing, orchestration, and monitoring.

> **Current milestone:** Day 5 — Project conventions, ingestion metadata, and data-quality policy design. Bronze ingestion implementation begins on Day 6.

## 1. Project Goals

- Ingest and explore real-world retail CSV datasets.
- Profile datasets to understand schemas, row counts, missing values, duplicates, and data-quality issues.
- Validate candidate primary keys, composite keys, and foreign-key relationships.
- Establish consistent data-quality, reconciliation, and error-handling policies.
- Design a layered lakehouse architecture using Bronze, Silver, and Gold.
- Build reusable and reliable data ingestion pipelines.
- Implement incremental loading, safe reruns, and historical data management.
- Develop analytical fact and dimension tables.
- Add orchestration, monitoring, testing, performance optimization, and cloud integration.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Data platform | Databricks |
| Processing | Apache Spark / PySpark |
| Query language | SQL |
| Programming language | Python |
| Storage format | Delta Lake — planned implementation |
| Source data | Olist e-commerce CSV datasets |
| Version control | Git and GitHub |
| Cloud integration | AWS — planned |
| Orchestration | Databricks Workflows and/or Apache Airflow — planned |
| Streaming | Kafka and Spark Structured Streaming — optional future extension |

Technologies marked as planned are part of the intended architecture but have not yet been implemented.

## 3. Dataset

The project uses nine Olist CSV datasets:

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

The datasets cover customers, orders, order items, payments, reviews, products, sellers, geolocation, and product-category translations.

## 4. Repository Structure

```text
retailpulse-data-engineering/
├── notebooks/
│   ├── 02_dataset_profiling.py
│   └── 03_key_relationship_validation.py
├── docs/
│   └── data_model_design_advanced.md
└── README.md
```

The notebooks are maintained as Databricks source notebooks. The repository may expand as ingestion, transformation, testing, and orchestration components are implemented.

## 5. Work Completed — Days 1–4

### 5.1 Environment and Project Setup

- Created the GitHub repository and connected it to Databricks.
- Configured the development catalog and schema.
- Created the raw-data volume.
- Uploaded the nine Olist CSV files.
- Created notebooks for dataset profiling and key/relationship validation.
- Prepared the advanced data model design document and pushed it to GitHub.

### 5.2 Dataset Profiling

Notebook: `notebooks/02_dataset_profiling.py`

The profiling work examined:

- Total rows and columns.
- Column names and data types.
- Missing values.
- Duplicate records and candidate key uniqueness.
- Selected domain-specific quality characteristics.

Selected observations:

| Dataset | Observed rows | Finding |
|---|---:|---|
| Customers | 99,441 | `customer_id` had no NULLs or duplicate key groups |
| Orders | 99,441 | `order_id` had no NULLs or duplicate key groups |
| Order items | 112,650 | `(order_id, order_item_id)` had no duplicate key groups |
| Order payments | 103,886 | `(order_id, payment_sequential)` had no duplicate key groups |
| Order reviews | 99,224 after corrected parsing | Review CSV reconciliation remains outstanding |
| Products | 32,951 | `product_id` was unique and non-NULL |
| Sellers | 3,095 | `seller_id` was unique and non-NULL |
| Geolocation | 1,000,163 | Exact duplicate rows were observed |
| Product category translation | 71 | `product_category_name` was checked as a candidate key |

These are development profiling observations, not a claim that the source datasets have been fully cleaned or reconciled.

### 5.3 Candidate Primary-Key and Foreign-Key Validation

Notebook: `notebooks/03_key_relationship_validation.py`

The validation notebook supports:

- NULL checks on candidate keys.
- Duplicate key-group detection.
- Composite-key validation.
- Foreign-key NULL checks.
- Orphan detection using left-anti joins.

Candidate keys checked:

| Dataset | Candidate key |
|---|---|
| Customers | `customer_id` |
| Orders | `order_id` |
| Order items | `(order_id, order_item_id)` |
| Order payments | `(order_id, payment_sequential)` |
| Order reviews | `(review_id, order_id)` |
| Products | `product_id` |
| Sellers | `seller_id` |
| Product category translation | `product_category_name` |

The latest reviewed validation output reported zero NULL key rows and zero duplicate key groups for these configured candidate keys.

Configured foreign-key relationships:

| Child dataset | Child column | Parent dataset | Parent column |
|---|---|---|---|
| Orders | `customer_id` | Customers | `customer_id` |
| Order items | `order_id` | Orders | `order_id` |
| Order items | `product_id` | Products | `product_id` |
| Order items | `seller_id` | Sellers | `seller_id` |
| Order payments | `order_id` | Orders | `order_id` |
| Order reviews | `order_id` | Orders | `order_id` |

The latest reviewed output reported zero NULL foreign keys and zero orphan records for these configured relationships.

These results apply to the loaded DataFrames and configured checks. They do not prove that every possible relationship or source-data quality rule has been validated.

### 5.4 Review CSV Parsing Investigation

The review dataset required additional investigation because review comments may contain quotation marks and line breaks.

The original parsing attempt produced suspicious row counts, column alignment, NULL keys, and misleading relationship-validation failures.

A revised Spark CSV reader configuration used multiline parsing and explicit quote-handling options. The corrected DataFrame contained **99,224 rows**, with no NULLs in `review_id`, `order_id`, or `review_score` in the checks performed. The `(review_id, order_id)` pair was unique across those rows.

The earlier load contained 104,162 rows. The difference indicates that parsing behavior materially affected the result. The corrected count still needs reconciliation against the source's logical records before source integrity can be confirmed.

## 6. Day 5 — Project Conventions and Data-Quality Policy Design

Day 5 focuses on defining how the future pipeline should behave when it encounters missing records, duplicate keys, invalid values, schema changes, and failed ingestion runs.

The policies below are **design decisions under development**. They have not yet been fully implemented in the pipeline.

### 6.1 Ingestion Metadata

The planned ingestion metadata includes:

- `source_file` — identifies the source file.
- `ingested_at` — records when the data was ingested.
- `run_id` — identifies the ingestion execution.
- Dataset-level status and relevant row counts.
- Validation results and error details where applicable.

These fields will support troubleshooting, auditing, and safe reruns.

### 6.2 Data-Quality Failure Categories

| Category | Example | Planned response |
|---|---|---|
| Critical schema error | Required column `order_id` is missing | Fail the dataset and record the error |
| Row-level validation error | Required key is missing | Quarantine the record when policy permits |
| Business-rule-valid NULL | Delivery date is missing for an order that has not been delivered | Accept when allowed by the defined rule |
| Unexpected business-rule violation | Delivered order has no delivery date | Flag and investigate; apply the configured quality policy |
| Count mismatch | Source and target row counts differ unexpectedly | Investigate before declaring ingestion successful |

### 6.3 Quarantine Policy

The proposed quarantine mechanism will:

- Retain invalid records for investigation.
- Record the validation failure reason.
- Preserve traceability through source-file and ingestion metadata.
- Allow valid records to continue when the applicable policy permits.
- Avoid silently discarding invalid records.

Quarantine rules will be based on explicit technical and business requirements rather than treating every NULL or unusual value as invalid.

### 6.4 Reconciliation Policy

The planned pipeline will compare source and target row counts and investigate discrepancies.

Where applicable, reconciliation will distinguish:

- Source records received.
- Records accepted for processing.
- Records quarantined.
- Records missing, duplicated, or unexpectedly added.

For a stage where every input record must be accounted for:

`Input records = Accepted records + Quarantined records`

This equation applies only when the stage's processing rules and record accounting make those categories exhaustive.

Row-count equality alone is insufficient to establish data integrity. Key comparisons and relevant data-quality checks are also required.

### 6.5 Duplicate Detection and Safe Reruns

Duplicate records will be investigated before deletion. Potential causes include repeated ingestion, source duplicates, incorrect join behavior, or faulty deduplication logic.

The intended ingestion design will use run tracking and appropriate idempotent processing so that rerunning the same input does not unintentionally create duplicate data.

Tracking a `run_id` alone does not guarantee idempotency; the actual ingestion logic must enforce safe rerun behavior.

### 6.6 Pipeline Status Policy

The proposed run-level statuses are:

- `SUCCESS` — required processing and quality checks pass.
- `PARTIAL_SUCCESS` — processing completes with permitted record-level failures or quarantined records.
- `FAILED` — a critical processing or quality requirement fails.

Dataset-level status and overall pipeline status will be tracked separately. The final status will depend on explicitly defined severity rules and quality thresholds.

## 7. Planned Lakehouse Architecture

The target architecture uses three logical layers.

### Bronze — Raw Ingestion

Planned responsibilities:

- Preserve source information.
- Store ingested data in Delta format.
- Track ingestion metadata and batch status.
- Support reconciliation and safe reruns.

### Silver — Validated and Standardized Data

Planned responsibilities:

- Apply schema and data-type standardization.
- Validate business rules and key relationships.
- Handle duplicates and missing values according to defined policies.
- Retain quarantined records and validation results.
- Support incremental processing.

### Gold — Analytical Data Model

The advanced design proposes four fact tables:

| Fact table | Intended grain |
|---|---|
| `fact_orders` | One row per order |
| `fact_order_items` | One row per `(order_id, order_item_id)` |
| `fact_payments` | One row per `(order_id, payment_sequential)` |
| `fact_reviews` | One row per validated review record, using `(review_id, order_id)` as the current candidate key |

Potential shared dimensions include customers, products, sellers, and dates. Additional dimensions for payment type, order status, or geography will be evaluated as the model develops.

The Gold layer and fact tables are design targets; they have not yet been implemented.

## 8. How to Run the Current Notebooks

1. Open the connected repository in Databricks.
2. Confirm that the nine CSV files are available under `/Volumes/retailpulse/dev/olist_raw/`.
3. Run `notebooks/02_dataset_profiling.py` to review the dataset profiles.
4. Run `notebooks/03_key_relationship_validation.py` from the beginning.
5. Review the complete validation output, including the corrected review dataset.
6. Investigate failures and reconcile source counts before treating validation as complete.

The current notebooks depend on the configured Databricks volume path.

## 9. Current Project Status — End of Day 5

- [x] Create GitHub repository and connect Databricks.
- [x] Configure development catalog/schema and raw-data volume.
- [x] Upload the nine Olist CSV files.
- [x] Profile the nine datasets.
- [x] Implement candidate-key and configured relationship checks.
- [x] Investigate review CSV parsing and apply corrected reader settings.
- [x] Prepare and push the advanced data model design document.
- [x] Discuss ingestion metadata, quarantine, reconciliation, safe reruns, and pipeline status policies.
- [ ] Reconcile the corrected review row count against source logical records.
- [ ] Finalize and commit the Day 5 data-quality and ingestion-policy documentation.
- [ ] Complete any remaining Day 5 README and design-document review.
- [ ] Begin Bronze schema and Delta Lake setup on Day 6.

## 10. Next Steps

1. Finish Day 5 documentation and review the outstanding review-data reconciliation.
2. Create the Bronze schema and learn managed versus external tables.
3. Load the first three datasets into Bronze, then load the remaining six.
4. Validate the ingested datasets and investigate any discrepancies.
5. Learn Delta Lake transactions, history, time travel, and schema evolution.
6. Build reusable ingestion logic with status tracking and safe reruns.
7. Continue through Silver transformations, Gold modelling, incremental processing, AWS integration, orchestration, monitoring, testing, and optimization according to the 45-day roadmap.

## 11. Project Development Principles

- A failed validation is a signal to investigate, not automatic proof that the source is wrong.
- Preserve traceability and avoid silently deleting data.
- Define business rules before classifying missing or unusual values as invalid.
- Treat row-count reconciliation, key validation, and content checks as complementary controls.
- Implement and test planned capabilities before describing them as complete.
- Follow the roadmap sequentially: understand, implement, review, validate, and commit.

RetailPulse is under active development. This README distinguishes observed results, design decisions, outstanding investigations, and planned implementation stages.
