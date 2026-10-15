# ============================================================
# RetailPulse - Key and Relationship Validation
# Notebook: 03_key_relationship_validation
# ============================================================
# Purpose:
# 1. Load the raw Olist datasets.
# 2. Validate candidate primary keys.
# 3. Validate foreign-key relationships between datasets.
#
# Important:
# order_reviews requires multiline CSV parsing because review
# comments can contain line breaks and quotation marks.
# ============================================================


# ============================================================
# 1. IMPORT LIBRARIES
# ============================================================

import pyspark.sql.functions as F
from functools import reduce
from operator import or_


# ============================================================
# 2. DEFINE DATASET PATHS
# ============================================================

common_path = "/Volumes/retailpulse/dev/olist_raw"

datasets = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_name_translation":
        "product_category_name_translation.csv"
}


# ============================================================
# 3. LOAD DATASETS
# ============================================================
# Most datasets use the standard CSV reader settings.
# order_reviews is loaded separately with multiline and quote
# handling so that line breaks inside comments are not treated
# as separate records.
# ============================================================

dataframes = {}

for dataset_name, file_name in datasets.items():

    file_path = f"{common_path}/{file_name}"

    if dataset_name == "order_reviews":
        dataframe = (
            spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("unescapedQuoteHandling", "STOP_AT_DELIMITER")
            .load(file_path)
        )
    else:
        dataframe = (
            spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(file_path)
        )

    dataframes[dataset_name] = dataframe

print(f"Loaded {len(dataframes)} datasets.")


# ============================================================
# 4. DISPLAY DATASET ROW COUNTS
# ============================================================

for dataset_name, dataframe in dataframes.items():
    print(f"{dataset_name}: {dataframe.count()} rows")


# ============================================================
# 5. DEFINE PRIMARY-KEY VALIDATION FUNCTION
# ============================================================

def validate_primary_key(dataframe, primary_key, dataset_name):
    """
    Check a candidate primary key for NULL values and duplicates.

    primary_key is a list so that composite keys are supported.
    duplicate_count counts duplicate key groups, not all rows
    that participate in those groups.
    """

    null_conditions = [
        F.col(column).isNull()
        for column in primary_key
    ]

    combined_null_condition = reduce(or_, null_conditions)

    null_count = (
        dataframe
        .filter(combined_null_condition)
        .count()
    )

    duplicate_count = (
        dataframe
        .groupBy(*primary_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status = (
        "PASS"
        if null_count == 0 and duplicate_count == 0
        else "FAIL"
    )

    print(
        f"{status}: {dataset_name} candidate key "
        f"({', '.join(primary_key)}) | "
        f"NULL rows: {null_count} | "
        f"Duplicate key groups: {duplicate_count}"
    )

    return {
        "dataset_name": dataset_name,
        "primary_key": ", ".join(primary_key),
        "null_count": null_count,
        "duplicate_count": duplicate_count,
        "status": status
    }


# ============================================================
# 6. CONFIGURE CANDIDATE PRIMARY KEYS
# ============================================================
# review_id alone is not unique in this source data. The
# (review_id, order_id) combination was checked and found to
# have 0 NULL composite-key rows and 0 duplicate groups.
# ============================================================

primary_key_config = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_name_translation": [
        "product_category_name"
    ],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"]
}


# ============================================================
# 7. RUN PRIMARY-KEY VALIDATION
# ============================================================

primary_key_validation_results = []

for dataset_name, primary_key in primary_key_config.items():

    result = validate_primary_key(
        dataframe=dataframes[dataset_name],
        primary_key=primary_key,
        dataset_name=dataset_name
    )

    primary_key_validation_results.append(result)

primary_key_validation_df = spark.createDataFrame(
    primary_key_validation_results
)

display(primary_key_validation_df)


# ============================================================
# 8. DEFINE FOREIGN-KEY VALIDATION FUNCTION
# ============================================================

def validate_foreign_key(
    child_df,
    foreign_key,
    child_dataset,
    parent_df,
    referenced_key,
    parent_dataset
):
    """
    Check a child foreign-key column for NULLs and values that
    do not exist in the referenced parent key.

    Orphan count is the number of child rows with a non-NULL
    foreign-key value that has no match in the parent dataset.
    """

    child = child_df.alias("child")
    parent = parent_df.alias("parent")

    null_count = (
        child
        .filter(F.col(f"child.{foreign_key}").isNull())
        .count()
    )

    orphan_count = (
        child
        .filter(F.col(f"child.{foreign_key}").isNotNull())
        .join(
            parent,
            F.col(f"child.{foreign_key}")
            == F.col(f"parent.{referenced_key}"),
            "left_anti"
        )
        .count()
    )

    status = (
        "PASS"
        if null_count == 0 and orphan_count == 0
        else "FAIL"
    )

    print(
        f"{status}: {child_dataset}.{foreign_key} -> "
        f"{parent_dataset}.{referenced_key} | "
        f"NULL rows: {null_count} | "
        f"Orphan rows: {orphan_count}"
    )

    return {
        "child_dataset": child_dataset,
        "foreign_key": foreign_key,
        "parent_dataset": parent_dataset,
        "referenced_key": referenced_key,
        "null_count": null_count,
        "orphan_count": orphan_count,
        "status": status
    }


# ============================================================
# 9. CONFIGURE FOREIGN-KEY RELATIONSHIPS
# ============================================================

foreign_key_config = [
    {
        "child_dataset": "orders",
        "foreign_key": "customer_id",
        "parent_dataset": "customers",
        "referenced_key": "customer_id"
    },
    {
        "child_dataset": "order_items",
        "foreign_key": "order_id",
        "parent_dataset": "orders",
        "referenced_key": "order_id"
    },
    {
        "child_dataset": "order_items",
        "foreign_key": "product_id",
        "parent_dataset": "products",
        "referenced_key": "product_id"
    },
    {
        "child_dataset": "order_items",
        "foreign_key": "seller_id",
        "parent_dataset": "sellers",
        "referenced_key": "seller_id"
    },
    {
        "child_dataset": "order_payments",
        "foreign_key": "order_id",
        "parent_dataset": "orders",
        "referenced_key": "order_id"
    },
    {
        "child_dataset": "order_reviews",
        "foreign_key": "order_id",
        "parent_dataset": "orders",
        "referenced_key": "order_id"
    }
]


# ============================================================
# 10. RUN FOREIGN-KEY VALIDATION
# ============================================================

foreign_key_validation_results = []

for relationship in foreign_key_config:

    child_dataset = relationship["child_dataset"]
    parent_dataset = relationship["parent_dataset"]

    result = validate_foreign_key(
        child_df=dataframes[child_dataset],
        foreign_key=relationship["foreign_key"],
        child_dataset=child_dataset,
        parent_df=dataframes[parent_dataset],
        referenced_key=relationship["referenced_key"],
        parent_dataset=parent_dataset
    )

    foreign_key_validation_results.append(result)

foreign_key_validation_df = spark.createDataFrame(
    foreign_key_validation_results
)

display(foreign_key_validation_df)


# ============================================================
# 11. REVIEW-SPECIFIC SANITY CHECKS
# ============================================================
# These checks verify the corrected reviews load and the
# candidate composite key. They do not replace the validation
# reports above.
# ============================================================

reviews_df = dataframes["order_reviews"]

print("Correctly loaded review rows:", reviews_df.count())

reviews_df.select(
    F.count("*").alias("total_rows"),
    F.sum(F.col("review_id").isNull().cast("int")).alias("null_review_id"),
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
    F.sum(F.col("review_score").isNull().cast("int")).alias("null_review_score")
).show()

print(
    "Distinct (review_id, order_id) pairs:",
    reviews_df
    .select("review_id", "order_id")
    .distinct()
    .count()
)
