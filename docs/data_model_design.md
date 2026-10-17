# RetailPulse --- Advanced Gold-Layer Data Model

**Status:** Proposed target design. This document describes the intended
model; the Gold tables, SCD pipelines, and orchestration are not yet
implemented.

## 1. Goals and design principles

RetailPulse will turn the Olist source datasets into reliable,
analytics-ready tables in Databricks using a dimensional model.

Design principles:

-   Define the grain of every fact table before choosing columns or
    joins.
-   Keep facts with different grains in separate tables to prevent row
    multiplication and inflated metrics.
-   Preserve source identifiers for traceability and use warehouse
    surrogate keys for dimensions.
-   Define business metrics explicitly, especially sales, cancellations,
    delivery performance, and customer counts.
-   Validate relationships, row counts, and aggregations at each
    transformation stage.
-   Keep raw source data and document any filtering, deduplication, or
    business rule.
-   Add incremental processing, SCD Type 1/2, audit logging, retries,
    quarantine, and orchestration in later implementation phases.

## 2. Target architecture

``` mermaid
flowchart TB
    subgraph Sources["Olist source datasets"]
      O["orders"]
      I["order_items"]
      P["products + category translation"]
      S["sellers"]
      C["customers"]
      Pay["order_payments"]
      R["order_reviews"]
      G["geolocation"]
    end

    subgraph Facts["Gold fact tables"]
      FO["fact_orders<br/>one row per order"]
      FI["fact_order_items<br/>one row per order item"]
      FP["fact_payments<br/>one row per payment sequence"]
      FR["fact_reviews<br/>one row per validated review record"]
    end

    subgraph Dims["Shared dimensions"]
      DP["dim_products"]
      DS["dim_sellers"]
      DC["dim_customers"]
      DD["dim_date"]
      DPay["dim_payment_type"]
      DOS["dim_order_status"]
      DG["dim_geography (optional, after ZIP validation)"]
    end

    O --> FO
    O --> FI
    I --> FI
    Pay --> FP
    R --> FR
    P --> DP
    S --> DS
    C --> DC
    G --> DG
    FI --> DP
    FI --> DS
    FI --> DC
    FI --> DD
    FO --> DC
    FO --> DD
    FO --> DOS
    FP --> DPay
    FP --> FO
    FR --> FO
    FR --> DD
```

The diagram is a logical target. Some relationships require careful
implementation: fact tables must not be joined together at detail level
for a single aggregate query unless they are first aggregated to
compatible grains. Optional dimensions should be included only after
their source data is validated.

## 3. Fact tables and their grain

### 3.1 `fact_orders`

**Grain:** one row per `order_id`.

Proposed columns:

  -----------------------------------------------------------------------
  Column                              Purpose
  ----------------------------------- -----------------------------------
  `order_id`                          Source business identifier; retain
                                      for traceability

  `customer_key`                      FK to `dim_customers`, resolved
                                      through `orders.customer_id`

  `purchase_date_key`                 Role-playing date key for purchase
                                      timestamp

  `approved_date_key`                 Optional date key for approval
                                      timestamp

  `carrier_date_key`                  Optional date key for carrier
                                      handoff timestamp

  `delivery_date_key`                 Optional date key for customer
                                      delivery timestamp

  `estimated_delivery_date_key`       Optional date key for estimated
                                      delivery timestamp

  `order_status_key`                  FK to `dim_order_status`, or status
                                      attribute if the dimension is
                                      deferred

  `order_count`                       Derived constant 1, useful for
                                      additive order counts
  -----------------------------------------------------------------------

Keep source timestamps in a suitable Silver representation for interval
calculations and auditability, even if the Gold model exposes date keys
for calendar analysis.

Useful metrics: - Total orders by purchase date, status, customer
geography. - Cancellation and unavailable-order rates with documented
denominators. - Approval, dispatch, and delivery durations where both
timestamps exist. - Delivered-after-estimated rate and delivery delay
distribution. - Orders with missing lifecycle timestamps.

Do not interpret every non-delivered status as cancelled. Define status
groupings explicitly.

### 3.2 `fact_order_items`

**Grain:** one row per `(order_id, order_item_id)`.

Proposed columns:

  Column                Purpose
  --------------------- ----------------------------------------
  `order_id`            Source order identifier
  `order_item_id`       Item number within the order
  `product_key`         FK to `dim_products`
  `seller_key`          FK to `dim_sellers`
  `purchase_date_key`   Purchase date role
  `delivery_date_key`   Delivery date role; may be missing
  `price`               Source item price
  `freight_value`       Source freight charge
  `item_quantity`       Derived constant 1 per source item row

Customer and order status can be obtained through `fact_orders` or an
explicit source orders join during transformation. If denormalizing them
into this fact for query convenience, document that design and preserve
the grain. Initial proposal: avoid duplicating all order-level
descriptive fields here; keep order-level measures/status in
`fact_orders`.

Useful metrics: - Item sales value, freight total, item count. - Sales
by product, category, seller, purchase month, and geography. - Average
item price; calculate as total price divided by item count or use a
clearly named average. - Freight-to-item-price ratios with
divide-by-zero protection.

**Metric caveat:** Olist's `price` is the item-level price recorded in
the source. `item_quantity = 1` means number of order-item rows, not
number of units in a multi-quantity line. Do not claim profit without
cost data.

### 3.3 `fact_payments`

**Grain:** one row per `(order_id, payment_sequential)`.

Proposed columns: - `order_id` - `payment_sequential` -
`payment_type_key` - `payment_value` - `payment_installments`

Useful metrics: - Total recorded payment value. - Payment method mix. -
Installment distribution. - Payment value per order and comparison with
item/freight totals.

Do not join payment rows directly to item rows and then sum both
measures: an order can have multiple items and multiple payment rows,
causing a many-to-many multiplication. Aggregate payments by order
before comparing to order/item metrics.

### 3.4 `fact_reviews`

**Grain:** one row per validated review record, initially using the
source candidate key `(review_id, order_id)` unless source investigation
supports a better business key.

Proposed columns: - `review_id` - `order_id` - `review_score` -
`review_creation_date_key` - `review_answer_date_key` - Optional derived
`review_response_hours` where timestamps are valid and comparable.

Useful metrics: - Review count and score distribution. - Average review
score, with review coverage denominator defined. - Review response
duration and unanswered review count.

The source review CSV required multiline/quote parsing. The tested
corrected read produced 99,224 rows with no nulls in `review_id`,
`order_id`, or `review_score`, and unique `(review_id, order_id)` pairs.
This needs a final validation rerun and source-count reconciliation
before treating it as resolved.

## 4. Dimensions

### 4.1 `dim_products`

Proposed attributes: - `product_key` --- warehouse surrogate key. -
`product_id` --- source natural key. - `product_category_name`. -
`product_category_name_english` --- from translation dataset where
available. - `product_weight_g`, `product_length_cm`,
`product_height_cm`, `product_width_cm`. - Optional product
name/description length and photo count fields, if useful for
analysis. - SCD Type 2 metadata when implemented: `effective_from`,
`effective_to`, `is_current`.

Do not fabricate missing product attributes. Decide Type 1 vs Type 2 per
attribute and business need.

### 4.2 `dim_sellers`

Proposed attributes: - `seller_key`, `seller_id`,
`seller_zip_code_prefix`, `seller_city`, `seller_state`. - SCD metadata
if historical seller attributes must be preserved.

### 4.3 `dim_customers`

Proposed attributes: - `customer_key`, `customer_id`,
`customer_unique_id`, `customer_zip_code_prefix`, `customer_city`,
`customer_state`. - Use `customer_id` to resolve `orders.customer_id`. -
Use `customer_unique_id` for underlying-customer analyses; count
distinct values for unique customer counts. - SCD metadata if historical
customer attributes are required.

### 4.4 `dim_date`

One shared calendar dimension with role-playing keys: - `date_key`
(typically `YYYYMMDD`) - `full_date`, `day`, `month`, `month_name`,
`quarter`, `year`, `day_of_week`, and optional weekend flags.

Use separate fact columns for purchase, approval, carrier handoff,
actual delivery, estimated delivery, and review dates as needed. Missing
dates must remain explicitly missing or use a documented unknown-date
key; do not assign an invented real date.

### 4.5 `dim_payment_type`

Small lookup dimension derived from observed `payment_type` values,
including handling of `not_defined` and unexpected future values. Could
be deferred if the source field remains a simple attribute in
`fact_payments`.

### 4.6 `dim_order_status`

Optional small dimension or controlled status mapping for source
statuses such as delivered, shipped, canceled, unavailable, invoiced,
processing, created, and approved. Maintain a separate documented
grouping for metrics; do not conflate all non-delivered statuses.

### 4.7 `dim_geography` (optional)

Potential enrichment from seller/customer ZIP prefixes and geolocation
data. Do not join raw geolocation directly on ZIP prefix until
duplicates and multiple coordinate combinations are investigated. Define
a deterministic mapping or aggregation policy first.

## 5. Relationship rules and join safety

-   `fact_orders.order_id` must be unique.
-   `fact_order_items` must remain unique on
    `(order_id, order_item_id)`.
-   `fact_payments` must remain unique on
    `(order_id, payment_sequential)`.
-   `fact_reviews` must use a validated review grain.
-   Product, seller, and customer dimensions must have a clear key
    strategy and tested uniqueness at the chosen grain.
-   Date keys must resolve consistently; delivery and other lifecycle
    dates may be missing.
-   Validate row counts before and after every join.
-   Do not join `fact_order_items`, `fact_payments`, and `fact_reviews`
    at detail level for combined sums. Aggregate to order grain first,
    then join summaries to `fact_orders` or another compatible grain.

## 6. Business metric definitions to document

  -----------------------------------------------------------------------
  Metric                              Suggested definition / caveat
  ----------------------------------- -----------------------------------
  Item sales value                    `SUM(price)` at order-item grain;
                                      state which order statuses are
                                      included

  Freight total                       `SUM(freight_value)`; status and
                                      refund/adjustment coverage must be
                                      documented

  Order count                         Count rows in `fact_orders` or
                                      distinct `order_id`, not item rows

  Item count                          `SUM(item_quantity)` where each
                                      source item row has quantity 1

  Average order value                 Defined order-level sales total
                                      divided by eligible order count; do
                                      not divide item sales by item rows
                                      and call it AOV

  Cancellation rate                   Cancelled orders divided by a
                                      documented eligible order
                                      population

  Delivery duration                   Actual delivery timestamp minus
                                      purchase timestamp, only for valid
                                      timestamp pairs

  Late-delivery rate                  Delivered orders later than
                                      estimated delivery divided by
                                      eligible delivered orders with both
                                      timestamps

  Average review score                Average valid review score; report
                                      review coverage separately

  Repeat-customer rate                Based on `customer_unique_id` and a
                                      defined observation period

  Payment mix                         Sum/count by payment type at
                                      payment grain

  Profit                              Not available without cost/expense
                                      data; do not infer it from item
                                      price alone
  -----------------------------------------------------------------------

## 7. Missing dimensions and invalid records

Use three distinct paths with precedence rules:

1.  **Retry/pending:** expected late-arriving dimension; hold and retry
    within a defined time or run limit.
2.  **Unknown/inferred dimension member:** valid fact whose dimension
    context is temporarily unavailable, if publication is permitted.
    Preserve the source ID and log the unresolved lookup.
3.  **Quarantine:** invalid identifier, corrupted data, or violation of
    a blocking rule. Store source data, reason, and run ID.

Unknown members must be reconciled and not used to hide persistent
errors. Decide which checks are blocking versus warning-level.

## 8. SCD and historical tracking

-   **Type 1:** overwrite a value when only the current value matters or
    for approved corrections.
-   **Type 2:** expire the old version and insert a new dimension row
    with a distinct surrogate key and `effective_from`, `effective_to`,
    `is_current`.
-   Fact loading must resolve the appropriate dimension version for the
    business event time.
-   Define late-arriving facts, late-arriving dimensions, backfills, and
    corrections before implementing SCD.

## 9. Production pipeline capabilities (planned)

-   Raw/Bronze retention and source metadata.
-   Silver cleansing, schema/type normalization, deduplication policy,
    key and relationship checks.
-   Gold dimensional tables and documented metric logic.
-   Incremental processing with idempotent reruns and safe merge keys.
-   Audit/run logging: run ID, source/target counts, start/end time,
    status, error details.
-   Retry/pending queues, quarantine tables, alerting, and
    reconciliation.
-   Orchestration with dependencies and failure handling.
-   Unit/data-quality tests and CI/CD.
-   AWS integration and optional streaming are later extensions, not
    part of the current implemented state.

## 10. Implementation sequence

1.  Finish and rerun the corrected source relationship validations,
    especially reviews.
2.  Finalize this logical model and draw the schema diagram.
3.  Build Bronze Delta tables from raw files.
4.  Build Silver clean tables with reproducible quality checks.
5.  Implement dimensions and surrogate keys.
6.  Implement `fact_orders`, `fact_order_items`, `fact_payments`, and
    `fact_reviews`.
7.  Validate counts, joins, metric reconciliation, and edge cases.
8.  Add SCD, incremental loads, logging, retries/quarantine, and
    orchestration.
9.  Create analytical SQL queries and dashboard-ready aggregates.
10. Document decisions and commit artifacts to GitHub.

## 11. Repository placement

Save as `docs/data_model_design.md`.

Suggested commit message:
`docs: expand RetailPulse dimensional model for advanced analytics`
