
from pathlib import Path
import os
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Load canonical schema
# ==========================================================

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # Fallback for Databricks notebook execution.
    PROJECT_ROOT = Path(os.getcwd()).resolve().parents[1]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "schemas"
    / "silver_canonical_schema"
    / "reviews.yml"
)

if not SCHEMA_PATH.is_file():
    raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")

with open(SCHEMA_PATH, "r") as file:
    REVIEWS_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = REVIEWS_SCHEMA["source_table"]
SILVER_TABLE = REVIEWS_SCHEMA["target_table"]
RESTAURANT_TABLE = "zomato_dev.silver.restaurant"
QUARANTINE_TABLE = "zomato_dev.quarantine.reviews_quarantine"

REVIEW_COLUMNS = REVIEWS_SCHEMA["columns"]
CANONICAL_COLUMN_NAMES = [column["name"] for column in REVIEW_COLUMNS]

EXPECTED_COLUMNS = {
    "review_id",
    "order_id",
    "customer_id",
    "restaurant_id",
    "rating",
    "comment",
    "review_date",
    "city",
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError("Invalid reviews.yml canonical columns.")

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names in reviews.yml.")

REQUIRED_COLUMNS = {
    column["name"]
    for column in REVIEW_COLUMNS
    if not column.get("nullable", True)
}

if REQUIRED_COLUMNS != {"review_id", "comment"}:
    raise ValueError(
        "reviews.yml must mark review_id and comment as non-nullable; "
        "other fields retain the source SQL's nullable behavior."
    )


# ==========================================================
# 2. Data quality rules
# ==========================================================

REVIEWS_QUALITY_RULES = {
    "valid_review_id": "NOT _invalid_review_id",
    "valid_order_id_cast": "NOT _invalid_order_id_cast",
    "valid_customer_id_cast": "NOT _invalid_customer_id_cast",
    "valid_rating_cast": "NOT _invalid_rating_cast",
    "valid_review_date_cast": "NOT _invalid_review_date_cast",
    "comment_present": "NOT _invalid_comment",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence",
    "no_rescued_data": "NOT _invalid_rescued_data",
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="reviews_validation_stage")
@dp.expect_all(REVIEWS_QUALITY_RULES)
def reviews_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve raw values before applying the staging conversions.
    df = raw_df.select(
        F.col("review_id").alias("source_review_id"),
        F.col("order_id").alias("source_order_id"),
        F.col("user_id").alias("source_user_id"),
        F.col("restaurant_id").alias("source_restaurant_id"),
        F.col("rating").alias("source_rating"),
        F.col("comment").alias("source_comment"),
        F.col("review_date").alias("source_review_date"),
        F.col("_rescued_data"),
        F.col("_source_file"),
        F.col("_source_file_modification_time").cast("timestamp"),
        F.col("_ingested_at").cast("timestamp").alias(
            "_bronze_ingested_at"
        ),
    )

    # ------------------------------------------------------
    # Canonical conversions from Darshil's stg_reviews.sql
    # ------------------------------------------------------

    df = (
        df
        .withColumn(
            "review_id",
            F.expr("try_cast(source_review_id AS DECIMAL(38,0))")
        )
        .withColumn(
            "order_id",
            F.expr("try_cast(source_order_id AS DECIMAL(38,0))")
        )
        .withColumn(
            "customer_id",
            F.expr("try_cast(source_user_id AS DECIMAL(38,0))")
        )
        # Darshil explicitly casts restaurant_id to STRING in Silver.
        .withColumn(
            "restaurant_id",
            F.col("source_restaurant_id").cast("string")
        )
        .withColumn(
            "rating",
            F.expr("try_cast(source_rating AS DECIMAL(38,0))")
        )
        .withColumn(
            "comment",
            F.col("source_comment").cast("string")
        )
        .withColumn(
            "review_date",
            F.expr("try_cast(source_review_date AS DATE)")
        )
        # Numeric join key used only for the restaurant lookup.
        .withColumn(
            "_restaurant_lookup_id",
            F.expr("try_cast(source_restaurant_id AS DECIMAL(38,0))")
        )
    )

    # ------------------------------------------------------
    # Darshil's LEFT JOIN to stg_restaurants for city
    # ------------------------------------------------------
    # Restaurant Silver is the static side of this stream-static join.
    # The restaurant_id key in Silver.restaurant is DECIMAL(38,0).

    restaurant_df = (
        spark.read.table(RESTAURANT_TABLE)
        .select(
            F.col("restaurant_id").cast("decimal(38,0)").alias(
                "_lookup_restaurant_id"
            ),
            F.col("city").alias("city"),
        )
    )

    reviews_alias = df.alias("reviews")
    restaurant_alias = restaurant_df.alias("restaurants")

    joined_df = reviews_alias.join(
        restaurant_alias,
        F.col("reviews._restaurant_lookup_id")
        == F.col("restaurants._lookup_restaurant_id"),
        "left",
    )

    df = joined_df.select(
        "reviews.*",
        F.col("restaurants.city").alias("city"),
    )

    # ------------------------------------------------------
    # Validation and quarantine classification
    # ------------------------------------------------------

    df = (
        df
        .withColumn(
            "_invalid_review_id",
            F.col("review_id").isNull()
            | (F.trim(F.col("source_review_id")) == "")
        )
        # Source SQL does not filter these nullable fields. Only reject
        # non-null source values that fail the requested type conversion.
        .withColumn(
            "_invalid_order_id_cast",
            F.col("source_order_id").isNotNull()
            & F.col("order_id").isNull()
        )
        .withColumn(
            "_invalid_customer_id_cast",
            F.col("source_user_id").isNotNull()
            & F.col("customer_id").isNull()
        )
        .withColumn(
            "_invalid_rating_cast",
            F.col("source_rating").isNotNull()
            & F.col("rating").isNull()
        )
        .withColumn(
            "_invalid_review_date_cast",
            F.col("source_review_date").isNotNull()
            & F.col("review_date").isNull()
        )
        # Darshil's exact filter is comment IS NOT NULL.
        # Empty strings are not changed into NULL here.
        .withColumn(
            "_invalid_comment",
            F.col("source_comment").isNull()
        )
        # Ingestion timestamp is the AUTO CDC ordering field.
        .withColumn(
            "_invalid_cdc_sequence",
            F.col("_bronze_ingested_at").isNull()
        )
        .withColumn(
            "_invalid_rescued_data",
            F.col("_rescued_data").isNotNull()
        )
    )

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_review_id")
        | F.col("_invalid_order_id_cast")
        | F.col("_invalid_customer_id_cast")
        | F.col("_invalid_rating_cast")
        | F.col("_invalid_review_date_cast")
        | F.col("_invalid_comment")
        | F.col("_invalid_cdc_sequence")
        | F.col("_invalid_rescued_data")
    )

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws(
            "|",
            F.when(F.col("_invalid_review_id"), F.lit("INVALID_REVIEW_ID")),
            F.when(F.col("_invalid_order_id_cast"), F.lit("INVALID_ORDER_ID")),
            F.when(F.col("_invalid_customer_id_cast"), F.lit("INVALID_CUSTOMER_ID")),
            F.when(F.col("_invalid_rating_cast"), F.lit("INVALID_RATING")),
            F.when(F.col("_invalid_review_date_cast"), F.lit("INVALID_REVIEW_DATE")),
            F.when(F.col("_invalid_comment"), F.lit("MISSING_COMMENT")),
            F.when(F.col("_invalid_cdc_sequence"), F.lit("INVALID_CDC_SEQUENCE")),
            F.when(F.col("_invalid_rescued_data"), F.lit("SCHEMA_DRIFT_RESCUED_DATA")),
        ),
    )

    df = df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    return df


# ==========================================================
# 4. Temporary view: valid review records for AUTO CDC
# ==========================================================

@dp.temporary_view(name="reviews_cdc_source")
def reviews_cdc_source():

    return (
        spark.readStream.table("reviews_validation_stage")
        .filter(~F.col("_is_quarantined"))
        .select(
            *CANONICAL_COLUMN_NAMES,
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_silver_processed_at",
        )
    )


# ==========================================================
# 5. Silver target: AUTO CDC SCD Type 1
# ==========================================================

dp.create_streaming_table(
    name=SILVER_TABLE,
    comment=(
        "Validated Zomato reviews enriched with restaurant city. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="reviews_cdc_source",
    keys=["review_id"],

    # Last-arriving-record-wins ordering.
    sequence_by=F.col("_bronze_ingested_at"),

    stored_as_scd_type=1,
    column_list=(
        CANONICAL_COLUMN_NAMES
        + [
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_silver_processed_at",
        ]
    ),
    name="reviews_upsert_flow",
)


# ==========================================================
# 6. Incremental quarantine streaming table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment="Incrementally captured rejected Zomato review records.",
)
def silver_reviews_quarantine():

    return (
        spark.readStream.table("reviews_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            "source_review_id",
            "source_order_id",
            "source_user_id",
            "source_restaurant_id",
            "source_rating",
            "source_comment",
            "source_review_date",
            "city",
            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_invalid_review_id",
            "_invalid_order_id_cast",
            "_invalid_customer_id_cast",
            "_invalid_rating_cast",
            "_invalid_review_date_cast",
            "_invalid_comment",
            "_invalid_cdc_sequence",
            "_invalid_rescued_data",
            F.col("_silver_processed_at").alias("_quarantined_at"),
            "_rejection_reason",
        )
    )
