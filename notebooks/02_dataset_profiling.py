# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # RetailPulse — Day 2: Source Data Profiling
# MAGIC
# MAGIC ## Objective
# MAGIC Profile the nine Olist CSV source datasets before designing ingestion and cleaning rules.
# MAGIC
# MAGIC ## Scope
# MAGIC - Inspect dataset sizes, schemas, NULL values, and exact duplicate rows.
# MAGIC - Run dataset-specific quality checks and candidate-key checks.
# MAGIC - Record observations without modifying or deleting source records.
# MAGIC
# MAGIC ## Important
# MAGIC Spark schema inference is used for this initial profiling pass. We will decide on explicit schemas after reviewing the findings. Key uniqueness checks here identify *candidate* keys; relationships will be validated separately on Day 3.

# COMMAND ----------

# 1. Imports and source paths
from pyspark.sql import functions as F

common_path = "/Volumes/retailpulse/dev/olist_raw"

datasets = {
    "customers": f"{common_path}/olist_customers_dataset.csv",
    "orders": f"{common_path}/olist_orders_dataset.csv",
    "order_items": f"{common_path}/olist_order_items_dataset.csv",
    "order_payments": f"{common_path}/olist_order_payments_dataset.csv",
    "order_reviews": f"{common_path}/olist_order_reviews_dataset.csv",
    "products": f"{common_path}/olist_products_dataset.csv",
    "sellers": f"{common_path}/olist_sellers_dataset.csv",
    "geolocation": f"{common_path}/olist_geolocation_dataset.csv",
    "product_category_name_translation": f"{common_path}/product_category_name_translation.csv",
}

print(f"Configured source datasets: {len(datasets)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load all source datasets
# MAGIC
# MAGIC Each CSV is loaded once and stored in `dataframes`. The named DataFrame variables below are also created for readability in dataset-specific checks.

# COMMAND ----------

# 2. Load the nine CSV files

dataframes = {}
for dataset_name, dataset_path in datasets.items():
    dataframes[dataset_name] = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(dataset_path)
    )

# Convenient names used in the individual profiling sections
customers_df = dataframes["customers"]
orders_df = dataframes["orders"]
order_items_df = dataframes["order_items"]
orders_payment_df = dataframes["order_payments"]
order_reviews_df = dataframes["order_reviews"]
products_df = dataframes["products"]
sellers_df = dataframes["sellers"]
geolocation_df = dataframes["geolocation"]
translation_df = dataframes["product_category_name_translation"]

print(f"Successfully loaded {len(dataframes)} datasets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reusable profiling function
# MAGIC
# MAGIC The function below captures record count, column count, inferred data types, per-column NULL counts, distinct full rows, and exact duplicate-row count. Dataset-specific business checks follow in later sections.

# COMMAND ----------

def profile_dataset(dataframe):
    total_records = dataframe.count()
    total_columns = len(dataframe.columns)

    null_expressions = [
        F.coalesce(
            F.sum(F.when(F.col(column_name).isNull(), 1)),
            F.lit(0)
        ).alias(column_name)
        for column_name in dataframe.columns
    ]
    null_counts = dataframe.agg(*null_expressions).first().asDict()

    distinct_rows = dataframe.distinct().count()

    column_profile = {
        field.name: {
            "data_type": field.dataType.simpleString(),
            "null_count": null_counts[field.name],
        }
        for field in dataframe.schema.fields
    }

    return {
        "total_records": total_records,
        "total_columns": total_columns,
        "distinct_rows": distinct_rows,
        "duplicate_rows": total_records - distinct_rows,
        "total_nulls": sum(item["null_count"] for item in column_profile.values()),
        "column_profile": column_profile,
    }

all_profiles = {}
for dataset_name, dataframe in dataframes.items():
    print(f"Profiling: {dataset_name}")
    all_profiles[dataset_name] = profile_dataset(dataframe)

summary_rows = [
    {
        "dataset_name": name,
        "total_records": profile["total_records"],
        "total_columns": profile["total_columns"],
        "total_nulls": profile["total_nulls"],
        "duplicate_rows": profile["duplicate_rows"],
        "null_percentage": round(
            profile["total_nulls"] /
            (profile["total_records"] * profile["total_columns"]) * 100, 2
        ) if profile["total_records"] and profile["total_columns"] else 0.0,
    }
    for name, profile in all_profiles.items()
]

summary_df = spark.createDataFrame(summary_rows)
display(summary_df.orderBy("dataset_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dataset-level details
# MAGIC
# MAGIC Review each dataset's schema, sample rows, and per-column NULL counts. The reusable summary above already provides the overall row, column, NULL, and duplicate totals.

# COMMAND ----------

# 3.1 Customers
print("CUSTOMERS — SAMPLE")
display(customers_df.limit(5))
customers_df.printSchema()
print("NULL counts by column")
display(customers_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in customers_df.columns
]))

print("Duplicate customer_id groups:", customers_df.groupBy("customer_id").count().filter(F.col("count") > 1).count())
repeated_unique_ids = customers_df.groupBy("customer_unique_id").count().filter(F.col("count") > 1)
print("Repeated customer_unique_id groups:", repeated_unique_ids.count())
display(repeated_unique_ids.agg(F.sum("count").alias("rows_in_repeated_id_groups"), F.max("count").alias("max_occurrences")))

# COMMAND ----------

# 3.2 Orders
print("ORDERS — SAMPLE")
display(orders_df.limit(5))
orders_df.printSchema()
print("NULL counts by column")
display(orders_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in orders_df.columns
]))

print("Duplicate order_id groups:", orders_df.groupBy("order_id").count().filter(F.col("count") > 1).count())
print("Order counts by status")
display(orders_df.groupBy("order_status").count().orderBy(F.desc("count")))

print("Missing delivery timestamp counts by status")
display(orders_df.groupBy("order_status").agg(
    F.sum(F.when(F.col("order_delivered_customer_date").isNull(), 1).otherwise(0)).alias("missing_customer_delivery_timestamp")
).orderBy(F.desc("missing_customer_delivery_timestamp")))

print("Delivered orders missing customer-delivery timestamp")
display(orders_df.filter(
    (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()
))

print("Missing approval timestamp counts by status")
display(orders_df.groupBy("order_status").agg(
    F.sum(F.when(F.col("order_approved_at").isNull(), 1).otherwise(0)).alias("missing_approval_timestamp")
).orderBy(F.desc("missing_approval_timestamp")))

print("Delivered orders missing approval timestamp")
display(orders_df.filter(
    (F.col("order_status") == "delivered") & F.col("order_approved_at").isNull()
))

late_delivered_orders_df = orders_df.filter(
    (F.col("order_status") == "delivered") &
    (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
)
print("Delivered orders later than estimated delivery timestamp:", late_delivered_orders_df.count())

# COMMAND ----------

# 3.3 Order items
print("ORDER ITEMS — SAMPLE")
display(order_items_df.limit(5))
order_items_df.printSchema()
print("NULL counts by column")
display(order_items_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in order_items_df.columns
]))

order_item_key_duplicates = order_items_df.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1)
print("Duplicate (order_id, order_item_id) key groups:", order_item_key_duplicates.count())
display(order_item_key_duplicates.limit(20))

print("Non-positive price and freight counts")
display(order_items_df.agg(
    F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("non_positive_price_rows"),
    F.sum(F.when(F.col("freight_value") == 0, 1).otherwise(0)).alias("zero_freight_rows"),
    F.sum(F.when(F.col("freight_value") < 0, 1).otherwise(0)).alias("negative_freight_rows"),
))
print("Price and freight summary")
display(order_items_df.agg(
    F.min("price").alias("min_price"), F.max("price").alias("max_price"), F.avg("price").alias("avg_price"),
    F.min("freight_value").alias("min_freight_value"), F.max("freight_value").alias("max_freight_value"), F.avg("freight_value").alias("avg_freight_value")
))
print("Records at maximum price or freight values")
order_item_stats = order_items_df.agg(F.max("price").alias("max_price"), F.max("freight_value").alias("max_freight_value")).first()
display(order_items_df.filter((F.col("price") == order_item_stats["max_price"]) | (F.col("freight_value") == order_item_stats["max_freight_value"])))

# COMMAND ----------

# 3.4 Order payments
print("ORDER PAYMENTS — SAMPLE")
display(orders_payment_df.limit(5))
orders_payment_df.printSchema()
print("NULL counts by column")
display(orders_payment_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in orders_payment_df.columns
]))

print("Duplicate (order_id, payment_sequential) key groups")
payment_key_duplicates = orders_payment_df.groupBy("order_id", "payment_sequential").count().filter(F.col("count") > 1)
print(payment_key_duplicates.count())
display(payment_key_duplicates.limit(20))

print("Payment value and installment checks")
display(orders_payment_df.agg(
    F.sum(F.when(F.col("payment_value") == 0, 1).otherwise(0)).alias("zero_payment_values"),
    F.sum(F.when(F.col("payment_value") < 0, 1).otherwise(0)).alias("negative_payment_values"),
    F.min("payment_value").alias("min_payment_value"),
    F.max("payment_value").alias("max_payment_value"),
    F.avg("payment_value").alias("avg_payment_value"),
    F.sum(F.when(F.col("payment_installments") == 0, 1).otherwise(0)).alias("zero_installments"),
    F.sum(F.when(F.col("payment_installments") < 0, 1).otherwise(0)).alias("negative_installments"),
))
print("Payment type distribution")
display(orders_payment_df.groupBy("payment_type").agg(
    F.count("*").alias("record_count"), F.sum("payment_value").alias("total_payment_value")
).orderBy(F.desc("record_count")))
print("Installment distribution")
display(orders_payment_df.groupBy("payment_installments").count().orderBy("payment_installments"))

# COMMAND ----------

# 3.5 Order reviews
print("ORDER REVIEWS — SAMPLE")
display(order_reviews_df.limit(5))
order_reviews_df.printSchema()
print("NULL counts by column")
display(order_reviews_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in order_reviews_df.columns
]))

print("Exact duplicate rows:", order_reviews_df.count() - order_reviews_df.distinct().count())
for key_columns in [["review_id"], ["order_id"], ["review_id", "order_id"]]:
    duplicate_groups = order_reviews_df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    print(f"Duplicate groups for {key_columns}: {duplicate_groups.count()}")

# Inspect raw rows if candidate-key results look misaligned or suspicious.
print("Review rows with missing review_id or order_id")
display(order_reviews_df.filter(F.col("review_id").isNull() | F.col("order_id").isNull()).limit(30))

# COMMAND ----------

# 3.6 Products
print("PRODUCTS — SAMPLE")
display(products_df.limit(5))
products_df.printSchema()
print("NULL counts by column")
display(products_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in products_df.columns
]))

print("NULL product IDs:", products_df.filter(F.col("product_id").isNull()).count())
duplicate_product_ids = products_df.groupBy("product_id").count().filter(F.col("count") > 1)
print("Duplicate product_id groups:", duplicate_product_ids.count())
display(duplicate_product_ids.limit(20))

# COMMAND ----------

# 3.7 Sellers
print("SELLERS — SAMPLE")
display(sellers_df.limit(5))
sellers_df.printSchema()
print("NULL counts by column")
display(sellers_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in sellers_df.columns
]))

print("NULL seller IDs:", sellers_df.filter(F.col("seller_id").isNull()).count())
duplicate_seller_ids = sellers_df.groupBy("seller_id").count().filter(F.col("count") > 1)
print("Duplicate seller_id groups:", duplicate_seller_ids.count())
display(duplicate_seller_ids.limit(20))

# COMMAND ----------

# 3.8 Geolocation
print("GEOLOCATION — SAMPLE")
display(geolocation_df.limit(5))
geolocation_df.printSchema()
print("NULL counts by column")
display(geolocation_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in geolocation_df.columns
]))

repeated_zip_prefixes = geolocation_df.groupBy("geolocation_zip_code_prefix").count().filter(F.col("count") > 1).orderBy(F.desc("count"))
print("Postal-code prefix groups with multiple records:", repeated_zip_prefixes.count())
display(repeated_zip_prefixes.limit(20))

# Focused investigation of prefix 24220; this does not modify source data.
geolocation_24220_df = geolocation_df.filter(F.col("geolocation_zip_code_prefix") == 24220)
print("Records for prefix 24220:", geolocation_24220_df.count())
display(geolocation_24220_df.select(
    "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"
).limit(20))
print("Distinct coordinate combinations:", geolocation_24220_df.select("geolocation_lat", "geolocation_lng").distinct().count())
unique_prefix_records = geolocation_24220_df.distinct().count()
print("Distinct complete records for prefix 24220:", unique_prefix_records)
print("Exact duplicate rows for prefix 24220:", geolocation_24220_df.count() - unique_prefix_records)

# COMMAND ----------

# 3.9 Product category name translation
print("PRODUCT CATEGORY NAME TRANSLATION — SAMPLE")
display(translation_df.limit(10))
translation_df.printSchema()
print("NULL counts by column")
display(translation_df.agg(*[
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in translation_df.columns
]))

print("Exact duplicate rows:", translation_df.count() - translation_df.distinct().count())
print("Distinct values, NULLs, and repeated-value groups by column")
for column_name in translation_df.columns:
    distinct_values = translation_df.select(column_name).distinct().count()
    null_count = translation_df.filter(F.col(column_name).isNull()).count()
    duplicate_groups = translation_df.groupBy(column_name).count().filter(F.col("count") > 1).count()
    blank_count = translation_df.filter(F.trim(F.col(column_name).cast("string")) == "").count()
    print(f"{column_name}: distinct={distinct_values}, nulls={null_count}, repeated-value groups={duplicate_groups}, blanks={blank_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consolidated summary
# MAGIC
# MAGIC Use this as the final at-a-glance source profiling table. `null_percentage` is the percentage of all cells that are NULL, not the percentage of rows with at least one NULL.

# COMMAND ----------

display(summary_df.orderBy("dataset_name"))

print("Datasets loaded:", len(dataframes))
print("Datasets profiled:", len(all_profiles))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Initial observations and follow-up
# MAGIC
# MAGIC - Repeated `customer_unique_id` values are not automatically errors; `customer_id` is the order-specific customer record identifier in this source.
# MAGIC - The `orders` dataset has missing timestamps. Interpret these in the context of `order_status`; do not fill them automatically.
# MAGIC - The `order_items` composite key candidate is (`order_id`, `order_item_id`). Zero freight values should be investigated, not automatically rejected.
# MAGIC - The `order_payments` composite key candidate is (`order_id`, `payment_sequential`). Zero payment values, zero installments, and `not_defined` payment types need documented follow-up.
# MAGIC - The `order_reviews` dataset has missing values and exact duplicates. Candidate-key results need verification because earlier duplicate-group output appeared suspicious.
# MAGIC - `product_id` and `seller_id` passed their individual NULL/uniqueness checks in the original profiling.
# MAGIC - Geolocation contains exact duplicate rows. A postal-code prefix can have multiple coordinates; repeated prefixes are not themselves proof of duplicate data.
# MAGIC - Postal-code prefixes should be considered identifiers; use strings in the future explicit schemas if preserving leading zeros is important.
# MAGIC - The category translation dataset is a small lookup table; check both columns for uniqueness and blanks before choosing its key.
# MAGIC
# MAGIC ## Day 2 outcome
# MAGIC The source datasets have been profiled at an initial level. No records were deleted or modified. Day 3 will focus on validating primary-key candidates, foreign-key references, and relationships between datasets. Explicit schemas and cleaning rules will be finalized separately.