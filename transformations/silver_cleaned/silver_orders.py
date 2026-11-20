
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
    / "orders.yml"
)

if not SCHEMA_PATH.is_file():
    raise FileNotFoundError(f"Canonical schema not found: {SCHEMA_PATH}")

with open(SCHEMA_PATH, "r") as file:
    ORDERS_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = ORDERS_SCHEMA["source_table"]
SILVER_TABLE = ORDERS_SCHEMA["target_table"]
QUARANTINE_TABLE = "zomato_dev.quarantine.orders_quarantine"

ORDERS_COLUMNS = ORDERS_SCHEMA["columns"]
CANONICAL_COLUMN_NAMES = [column["name"] for column in ORDERS_COLUMNS]

EXPECTED_COLUMNS = {
    "order_id",
    "order_timestamp",
    "order_date",
    "customer_id",
    "restaurant_id",
    "city",
    "cuisine",
    "items_count",
    "sales_qty",
    "subtotal",
    "discount",
    "delivery_fee",
    "gst",
    "sales_amount",
    "currency",
    "payment_method",
    "order_status",
    "is_delivered",
    "customer_rating",
    "delivery_time_min",
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError("Invalid orders.yml canonical columns.")

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names in orders.yml.")

# Nullable columns are permitted only where the source profile supports them.
EXPECTED_NULLABLE_COLUMNS = {"customer_rating", "delivery_time_min"}
ACTUAL_NULLABLE_COLUMNS = {
    column["name"] for column in ORDERS_COLUMNS
    if column.get("nullable", False)
}

if ACTUAL_NULLABLE_COLUMNS != EXPECTED_NULLABLE_COLUMNS:
    raise ValueError(
        "Expected only customer_rating and delivery_time_min to be nullable."
    )

# A source column can feed more than one canonical column (order_status ->
# order_status and is_delivered). Select each source column only once.
SOURCE_COLUMN_NAMES = list(dict.fromkeys(
    column["source_column"] for column in ORDERS_COLUMNS
))


# ==========================================================
# 2. Data quality expectations
# ==========================================================

INVALID_COLUMNS = [
    "_invalid_order_id",
    "_invalid_order_timestamp",
    "_invalid_order_date",
    "_invalid_customer_id",
    "_invalid_restaurant_id",
    "_invalid_city",
    "_invalid_cuisine",
    "_invalid_items_count",
    "_invalid_sales_qty",
    "_invalid_subtotal",
    "_invalid_discount",
    "_invalid_delivery_fee",
    "_invalid_gst",
    "_invalid_sales_amount",
    "_invalid_currency",
    "_invalid_payment_method",
    "_invalid_order_status",
    "_invalid_customer_rating",
    "_invalid_delivery_time_min",
    "_invalid_cdc_sequence",
    "_invalid_rescued_data",
]

ORDERS_QUALITY_RULES = {
    "valid_order_id": "NOT _invalid_order_id",
    "valid_order_timestamp": "NOT _invalid_order_timestamp",
    "valid_order_date": "NOT _invalid_order_date",
    "valid_customer_id": "NOT _invalid_customer_id",
    "valid_restaurant_id": "NOT _invalid_restaurant_id",
    "valid_city": "NOT _invalid_city",
    "valid_cuisine": "NOT _invalid_cuisine",
    "valid_items_count": "NOT _invalid_items_count",
    "valid_sales_qty": "NOT _invalid_sales_qty",
    "valid_subtotal": "NOT _invalid_subtotal",
    "valid_discount": "NOT _invalid_discount",
    "valid_delivery_fee": "NOT _invalid_delivery_fee",
    "valid_gst": "NOT _invalid_gst",
    "valid_sales_amount": "NOT _invalid_sales_amount",
    "valid_currency": "NOT _invalid_currency",
    "valid_payment_method": "NOT _invalid_payment_method",
    "valid_order_status": "NOT _invalid_order_status",
    "valid_customer_rating_format": "NOT _invalid_customer_rating",
    "valid_delivery_time_format": "NOT _invalid_delivery_time_min",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence",
    "no_rescued_data": "NOT _invalid_rescued_data",
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="orders_validation_stage")
@dp.expect_all(ORDERS_QUALITY_RULES)
def orders_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve original values for quarantine.
    source_columns = [
        F.col(source_column).alias(f"source_{source_column}")
        for source_column in SOURCE_COLUMN_NAMES
    ]

    # Preserve technical metadata separately from business columns.
    metadata_columns = [
        F.col("_source_file"),
        F.col("_source_file_modification_time").cast("timestamp"),
        F.col("_ingested_at").cast("timestamp").alias(
            "_bronze_ingested_at"
        ),
        F.col("_rescued_data"),
    ]

    df = raw_df.select(*source_columns, *metadata_columns)

    # ------------------------------------------------------
    # Canonical transformations driven by YAML
    # ------------------------------------------------------

    for column in ORDERS_COLUMNS:
        source_column = f"source_{column['source_column']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        # Darshil's city expression:
        # TRIM(COALESCE(REGEXP_SUBSTR(restaurant_city, '[^,]+$'), restaurant_city))
        if target_column == "city":
            city_suffix = F.regexp_extract(
                F.col(source_column),
                r"([^,]+)$",
                1,
            )
            city_value = F.when(
                city_suffix != "",
                city_suffix,
            ).otherwise(F.col(source_column))
            value = F.trim(city_value)

        # Darshil's derived expression: order_status = 'Delivered'
        elif target_column == "is_delivered":
            value = F.col(source_column) == F.lit("Delivered")

        elif data_type == "STRING":
            # Preserve source text exactly except for Darshil's city trim.
            value = F.col(source_column)

        else:
            value = F.expr(
                f"try_cast(`{source_column}` AS {data_type})"
            )

        df = df.withColumn(target_column, value)

    # ------------------------------------------------------
    # Validation flags
    # ------------------------------------------------------

    df = df.withColumn(
        "_invalid_order_id",
        F.col("order_id").isNull(),
    )

    df = df.withColumn(
        "_invalid_order_timestamp",
        F.col("order_timestamp").isNull(),
    )

    df = df.withColumn(
        "_invalid_order_date",
        F.col("order_date").isNull(),
    )

    df = df.withColumn(
        "_invalid_customer_id",
        F.col("customer_id").isNull(),
    )

    df = df.withColumn(
        "_invalid_restaurant_id",
        F.col("restaurant_id").isNull(),
    )

    df = df.withColumn(
        "_invalid_city",
        F.col("city").isNull()
        | (F.length(F.trim(F.col("city"))) == 0),
    )

    df = df.withColumn(
        "_invalid_cuisine",
        F.col("cuisine").isNull()
        | (F.length(F.trim(F.col("cuisine"))) == 0),
    )

    for column_name in [
        "items_count",
        "sales_qty",
        "subtotal",
        "discount",
        "delivery_fee",
        "gst",
        "sales_amount",
    ]:
        df = df.withColumn(
            f"_invalid_{column_name}",
            F.col(column_name).isNull(),
        )

    df = df.withColumn(
        "_invalid_currency",
        F.col("currency").isNull()
        | (F.length(F.trim(F.col("currency"))) == 0),
    )

    df = df.withColumn(
        "_invalid_payment_method",
        F.col("payment_method").isNull()
        | (F.length(F.trim(F.col("payment_method"))) == 0),
    )

    df = df.withColumn(
        "_invalid_order_status",
        F.col("order_status").isNull()
        | (F.length(F.trim(F.col("order_status"))) == 0),
    )

    # Ratings and delivery times are nullable in the observed source.
    # A genuinely missing value is allowed; a non-null value that fails
    # conversion is rejected.
    df = df.withColumn(
        "_invalid_customer_rating",
        F.col("source_customer_rating").isNotNull()
        & F.col("customer_rating").isNull(),
    )

    df = df.withColumn(
        "_invalid_delivery_time_min",
        F.col("source_delivery_time_min").isNotNull()
        & F.col("delivery_time_min").isNull(),
    )

    # Ingestion timestamp is the AUTO CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull(),
    )

    df = df.withColumn(
        "_invalid_rescued_data",
        F.col("_rescued_data").isNotNull(),
    )

    # ------------------------------------------------------
    # Quarantine decision
    # ------------------------------------------------------

    quarantine_condition = F.lit(False)
    for invalid_column in INVALID_COLUMNS:
        quarantine_condition = (
            quarantine_condition | F.col(invalid_column)
        )

    df = df.withColumn("_is_quarantined", quarantine_condition)

    # ------------------------------------------------------
    # Rejection reasons
    # ------------------------------------------------------

    rejection_reason_expressions = [
        F.when(F.col(flag), F.lit(reason))
        for flag, reason in [
            ("_invalid_order_id", "INVALID_ORDER_ID"),
            ("_invalid_order_timestamp", "INVALID_ORDER_TIMESTAMP"),
            ("_invalid_order_date", "INVALID_ORDER_DATE"),
            ("_invalid_customer_id", "INVALID_CUSTOMER_ID"),
            ("_invalid_restaurant_id", "INVALID_RESTAURANT_ID"),
            ("_invalid_city", "INVALID_CITY"),
            ("_invalid_cuisine", "MISSING_CUISINE"),
            ("_invalid_items_count", "INVALID_ITEMS_COUNT"),
            ("_invalid_sales_qty", "INVALID_SALES_QTY"),
            ("_invalid_subtotal", "INVALID_SUBTOTAL"),
            ("_invalid_discount", "INVALID_DISCOUNT"),
            ("_invalid_delivery_fee", "INVALID_DELIVERY_FEE"),
            ("_invalid_gst", "INVALID_GST"),
            ("_invalid_sales_amount", "INVALID_SALES_AMOUNT"),
            ("_invalid_currency", "INVALID_CURRENCY"),
            ("_invalid_payment_method", "INVALID_PAYMENT_METHOD"),
            ("_invalid_order_status", "INVALID_ORDER_STATUS"),
            ("_invalid_customer_rating", "INVALID_CUSTOMER_RATING"),
            ("_invalid_delivery_time_min", "INVALID_DELIVERY_TIME_MIN"),
            ("_invalid_cdc_sequence", "INVALID_CDC_SEQUENCE"),
            ("_invalid_rescued_data", "SCHEMA_DRIFT_RESCUED_DATA"),
        ]
    ]

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("|", *rejection_reason_expressions),
    )

    df = df.withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )

    return df


# ==========================================================
# 4. Temporary view: valid records for AUTO CDC
# ==========================================================

@dp.temporary_view(name="orders_cdc_source")
def orders_cdc_source():

    return (
        spark.readStream.table("orders_validation_stage")
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
        "Validated Zomato orders. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="orders_cdc_source",
    keys=["order_id"],

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
    name="orders_upsert_flow",
)


# ==========================================================
# 6. Persistent incremental quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment=(
        "Incrementally captured rejected Zomato order records "
        "with original values and rejection reasons."
    ),
)
def silver_orders_quarantine():

    return (
        spark.readStream.table("orders_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"source_{source_column}"
                for source_column in SOURCE_COLUMN_NAMES
            ],
            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            *INVALID_COLUMNS,
            F.col("_silver_processed_at").alias("_quarantined_at"),
            "_rejection_reason",
        )
    )
