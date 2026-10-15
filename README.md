# RetailPulse — End-to-End Retail Data Engineering Platform

## 1. Project Overview

RetailPulse is an end-to-end data engineering project built using the Brazilian E-Commerce Public Dataset by Olist.

The goal is to build a scalable retail data platform that processes raw e-commerce data, performs data-quality checks, models relationships between datasets, and prepares reliable data for downstream analytics.

The project is being developed incrementally using Databricks, PySpark, SQL, and Delta Lake, with cloud integration and orchestration planned for later stages.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data processing and validation logic |
| PySpark | Distributed data processing |
| Databricks | Development and execution environment |
| SQL | Data querying and analysis |
| Delta Lake | Planned storage format for curated datasets |
| GitHub | Version control and project documentation |
| AWS | Planned cloud integration |
| Apache Airflow | Planned workflow orchestration |
| Apache Kafka | Potential future streaming extension |

## 3. Dataset Overview

The project uses nine CSV files from the Olist dataset.

| Dataset | Description |
|---|---|
| `olist_customers_dataset.csv` | Customer information |
| `olist_orders_dataset.csv` | Order information and order lifecycle timestamps |
| `olist_order_items_dataset.csv` | Products and sellers associated with orders |
| `olist_order_payments_dataset.csv` | Payment information for orders |
| `olist_order_reviews_dataset.csv` | Customer reviews and ratings |
| `olist_products_dataset.csv` | Product attributes |
| `olist_sellers_dataset.csv` | Seller information |
| `olist_geolocation_dataset.csv` | Geographic information associated with postal-code prefixes |
| `product_category_name_translation.csv` | Portuguese-to-English product category translation |

### Raw Data Location

The CSV files are stored in a Databricks Volume:

`/Volumes/retailpulse/dev/olist_raw/`

## 4. Project Structure

The current notebook organization separates profiling from key and relationship validation.

```text
retailpulse-data-engineering/
│
├── notebooks/
│   ├── 02_dataset_profiling.py
│   └── 03_key_relationship_validation.py
│
└── README.md
```

Additional notebooks and directories will be introduced as the project progresses.

## 5. Dataset Profiling

**Status: Completed initial profiling**

The nine datasets were individually inspected to understand their structure and data quality.

The profiling process included:

- Record and column counts
- Column names and data types
- NULL-value analysis
- Exact duplicate-row analysis
- Candidate primary-key uniqueness checks
- Basic numeric validation
- Timestamp and order-status checks
- Initial identification of data-quality issues

### Initial Profiling Findings

| Dataset | Records | Columns | Notable finding |
|---|---:|---:|---|
| Customers | 99,441 | 5 | 2,997 duplicate groups for `customer_unique_id`; `customer_id` had no NULLs or duplicate groups |
| Orders | 99,441 | 8 | 160 NULL approval timestamps; 1,783 NULL carrier timestamps; 2,965 NULL customer-delivery timestamps |
| Order Items | 112,650 | 7 | No duplicate groups for the composite key `(order_id, order_item_id)` |
| Order Payments | 103,886 | 5 | No duplicate groups for the composite key `(order_id, payment_sequential)` |
| Order Reviews | 104,162 | 7 | 85 exact duplicate rows; missing values and repeated key groups |
| Products | 32,951 | 9 | 2,448 NULL cells across the dataset |
| Sellers | 3,095 | 4 | No NULL cells found during initial profiling |
| Geolocation | 1,000,163 | 5 | 261,831 exact duplicate rows |
| Category Translation | 71 | 2 | No NULLs or exact duplicate rows in initial profiling |

These are initial profiling results. Missing values and duplicate records require interpretation according to the relevant business rules; they should not automatically be removed.

## 6. Primary-Key Validation

**Status: Automated validation implemented**

A reusable PySpark function, `validate_primary_key()`, was implemented to validate candidate primary keys.

The function checks:

1. NULL values in candidate key columns.
2. Duplicate groups for the candidate key.
3. PASS/FAIL status based on those checks.

The function supports both single-column and composite candidate keys.

### Candidate Primary-Key Configuration

| Dataset | Candidate key | Type |
|---|---|---|
| Customers | `customer_id` | Single-column |
| Orders | `order_id` | Single-column |
| Order Reviews | `review_id` | Single-column |
| Products | `product_id` | Single-column |
| Sellers | `seller_id` | Single-column |
| Product Category Translation | `product_category_name` | Single-column |
| Order Items | `(order_id, order_item_id)` | Composite |
| Order Payments | `(order_id, payment_sequential)` | Composite |

No primary key has yet been confirmed for the geolocation dataset.

### Primary-Key Validation Results

| Dataset | Candidate key | NULL count | Duplicate groups | Status |
|---|---|---:|---:|---|
| Customers | `customer_id` | 0 | 0 | PASS |
| Orders | `order_id` | 0 | 0 | PASS |
| Order Reviews | `review_id` | 1 | 955 | FAIL |
| Products | `product_id` | 0 | 0 | PASS |
| Sellers | `seller_id` | 0 | 0 | PASS |
| Product Category Translation | `product_category_name` | 0 | 0 | PASS |

The composite-key candidates for order items and order payments are configured in the automated validation code. Their final results should be taken from the latest consolidated validation report.

### Important Finding: Order Reviews

The `review_id` candidate failed because it contains NULL values and duplicate groups.

Initial profiling also found:

- 85 exact duplicate rows.
- 955 duplicate groups for `review_id`.
- 951 duplicate groups for `order_id`.
- 77 duplicate groups for the combination `(review_id, order_id)`.

These findings indicate that further investigation is required before deciding how review records should be uniquely identified.

## 7. Foreign-Key Validation

**Status: Automated validation implemented**

A reusable function, `validate_foreign_key()`, was implemented to check relationships between child and parent datasets.

The function reports:

- NULL foreign-key rows
- Orphan rows whose non-NULL foreign-key values have no matching parent key
- PASS/FAIL status
- Child and parent dataset and column names

A left anti join is used to identify orphan records.

### Relationships Validated

| Child dataset | Foreign key | Parent dataset | Referenced key |
|---|---|---|---|
| Orders | `customer_id` | Customers | `customer_id` |
| Order Items | `order_id` | Orders | `order_id` |
| Order Items | `product_id` | Products | `product_id` |
| Order Items | `seller_id` | Sellers | `seller_id` |
| Order Payments | `order_id` | Orders | `order_id` |
| Order Reviews | `order_id` | Orders | `order_id` |

### Foreign-Key Validation Results

| Child dataset | Foreign key | NULL count | Orphan count | Status |
|---|---|---:|---:|---|
| Orders | `customer_id` | 0 | 0 | PASS |
| Order Items | `order_id` | 0 | 0 | PASS |
| Order Items | `product_id` | 0 | 0 | PASS |
| Order Items | `seller_id` | 0 | 0 | PASS |
| Order Payments | `order_id` | 0 | 0 | PASS |
| Order Reviews | `order_id` | 2,236 | 2,702 | FAIL |

### Important Finding: Order Reviews Relationship

The relationship `order_reviews.order_id` → `orders.order_id` failed validation.

The current report shows:

- 2,236 rows with a NULL `order_id`.
- 2,702 non-NULL orphan rows whose `order_id` does not match a record in the orders dataset.

The cause has not yet been established. These records must be investigated before making any data corrections or deleting rows.

The validation function currently treats any NULL foreign key as a failure. If a relationship permits NULLs, the rule will need to reflect that business requirement.

## 8. Relationship Model

The following diagram represents the intended relationships among the validated datasets.

```mermaid
erDiagram
    CUSTOMERS ||--o{ ORDERS : places
    ORDERS ||--o{ ORDER_ITEMS : contains
    PRODUCTS ||--o{ ORDER_ITEMS : includes
    SELLERS ||--o{ ORDER_ITEMS : supplies
    ORDERS ||--o{ ORDER_PAYMENTS : has
    ORDERS ||--o{ ORDER_REVIEWS : receives

    CUSTOMERS {
        string customer_id PK
    }

    ORDERS {
        string order_id PK
        string customer_id FK
    }

    ORDER_ITEMS {
        string order_id PK, FK
        int order_item_id PK
        string product_id FK
        string seller_id FK
    }

    ORDER_PAYMENTS {
        string order_id PK, FK
        int payment_sequential PK
    }

    ORDER_REVIEWS {
        string review_id candidate
        string order_id FK
    }

    PRODUCTS {
        string product_id PK
    }

    SELLERS {
        string seller_id PK
    }
```

The diagram describes the intended logical model. It does not imply that every relationship has passed data validation. The order-review relationship remains unresolved.

## 9. Engineering Practices Implemented

The work completed so far demonstrates several data engineering practices:

- Reusable PySpark validation functions
- Configuration-driven validation
- Automated processing across multiple datasets
- Composite-key validation
- NULL and duplicate-group checks
- Left anti joins for referential-integrity checks
- Consolidated validation reports using Spark DataFrames
- Separation of profiling and relationship-validation notebooks
- GitHub-based version control

## 10. Current Project Status

### Completed

- Databricks environment and GitHub integration setup
- Raw Olist dataset loading
- Initial profiling of all nine datasets
- Automated candidate primary-key validation
- Composite-key support in the validation function
- Automated validation of six candidate foreign-key relationships
- Consolidated validation reports

### In Progress

- Investigating `order_reviews` primary-key and foreign-key failures
- Confirming the composite-key validation results
- Documenting the final relationship model
- Completing and committing the Day 3 deliverables

## 11. Future Roadmap

The planned development phases are:

1. Complete key and relationship validation.
2. Design the retail analytical data model and star schema.
3. Implement Bronze, Silver, and Gold data layers using Delta Lake.
4. Develop data-cleaning and transformation pipelines.
5. Implement incremental loading and appropriate Slowly Changing Dimension strategies.
6. Integrate AWS services.
7. Introduce orchestration using Apache Airflow.
8. Explore streaming ingestion using Apache Kafka.
9. Prepare curated datasets for analytics and reporting.

## 12. Project Objective

RetailPulse aims to demonstrate practical data engineering skills through a real-world e-commerce dataset, with emphasis on data quality, reliable transformations, scalable processing, version control, and a progressively developed lakehouse architecture.
