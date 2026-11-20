
from pathlib import Path
import yaml
import os

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
    / "order_items.yml"
)

with open(SCHEMA_PATH, "r") as file:
    ORDER_ITEMS_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = ORDER_ITEMS_SCHEMA["source_table"]
SILVER_TABLE = ORDER_ITEMS_SCHEMA["target_table"]

QUARANTINE_TABLE = "zomato_dev.quarantine.order_items_quarantine"

ORDER_ITEM_COLUMNS = ORDER_ITEMS_SCHEMA["columns"]

CANONICAL_COLUMN_NAMES = [
    column["name"] for column in ORDER_ITEM_COLUMNS
]

EXPECTED_COLUMNS = {
    "order_item_id",
    "order_id",
    "restaurant_id",
    "f_id",
    "price",
    "quantity",
    "line_amount"
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError(
        "Invalid order_items.yml: expected order_item_id, order_id, "
        "restaurant_id, f_id, price, quantity and line_amount."
    )

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names.")

if any(column.get("nullable", True) for column in ORDER_ITEM_COLUMNS):
    raise ValueError("Canonical order_items columns must be non-nullable.")


# ==========================================================
# 2. Data quality rules
# ==========================================================

ORDER_ITEMS_QUALITY_RULES = {
    "valid_order_item_id": "NOT _invalid_order_item_id",
    "valid_order_id": "NOT _invalid_order_id",
    "valid_restaurant_id": "NOT _invalid_restaurant_id",
    "valid_food_id": "NOT _invalid_f_id",
    "valid_price": "NOT _invalid_price",
    "valid_quantity": "NOT _invalid_quantity",
    "valid_line_amount": "NOT _invalid_line_amount",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence",
    "no_rescued_data": "NOT _invalid_rescued_data"
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="order_items_validation_stage")
@dp.expect_all(ORDER_ITEMS_QUALITY_RULES)
def order_items_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve original source values for quarantine.
    source_columns = [
        F.col(column["source_column"]).alias(
            f"source_{column['source_column']}"
        )
        for column in ORDER_ITEM_COLUMNS
    ]

    # Preserve technical metadata separately.
    metadata_columns = [
        F.col("_source_file"),
        F.col("_source_file_modification_time").cast("timestamp"),
        F.col("_ingested_at").cast("timestamp").alias(
            "_bronze_ingested_at"
        ),
        F.col("_rescued_data")
    ]

    df = raw_df.select(
        *source_columns,
        *metadata_columns
    )

    # ------------------------------------------------------
    # Canonical transformations driven by YAML.
    # These match Darshil's stg_order_items.sql:
    #   r_id -> restaurant_id
    #   price -> DECIMAL(10,2)
    #   quantity -> NUMBER (Spark DECIMAL(38,0))
    #   line_amount -> DECIMAL(10,2)
    # Other business columns pass through unchanged.
    # ------------------------------------------------------

    for column in ORDER_ITEM_COLUMNS:

        source_column = f"source_{column['source_column']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        if data_type == "STRING":
            value = F.col(source_column)
        else:
            value = F.expr(
                f"try_cast(`{source_column}` AS {data_type})"
            )

        df = df.withColumn(target_column, value)

    # ------------------------------------------------------
    # Validation
    # No additional business filters are added here.
    # ------------------------------------------------------

    df = df.withColumn(
        "_invalid_order_item_id",
        F.col("order_item_id").isNull()
        | (F.length(F.trim(F.col("order_item_id"))) == 0)
    )

    df = df.withColumn(
        "_invalid_order_id",
        F.col("order_id").isNull()
        | (F.length(F.trim(F.col("order_id"))) == 0)
    )

    df = df.withColumn(
        "_invalid_restaurant_id",
        F.col("restaurant_id").isNull()
        | (F.length(F.trim(F.col("restaurant_id"))) == 0)
    )

    df = df.withColumn(
        "_invalid_f_id",
        F.col("f_id").isNull()
        | (F.length(F.trim(F.col("f_id"))) == 0)
    )

    df = df.withColumn(
        "_invalid_price",
        F.col("price").isNull()
    )

    df = df.withColumn(
        "_invalid_quantity",
        F.col("quantity").isNull()
    )

    df = df.withColumn(
        "_invalid_line_amount",
        F.col("line_amount").isNull()
    )

    # Ingestion timestamp is the AUTO CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull()
    )

    df = df.withColumn(
        "_invalid_rescued_data",
        F.col("_rescued_data").isNotNull()
    )

    # ------------------------------------------------------
    # Quarantine identification
    # ------------------------------------------------------

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_order_item_id")
        | F.col("_invalid_order_id")
        | F.col("_invalid_restaurant_id")
        | F.col("_invalid_f_id")
        | F.col("_invalid_price")
        | F.col("_invalid_quantity")
        | F.col("_invalid_line_amount")
        | F.col("_invalid_cdc_sequence")
        | F.col("_invalid_rescued_data")
    )

    # ------------------------------------------------------
    # Rejection reasons
    # ------------------------------------------------------

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("_invalid_order_item_id"),
                F.lit("INVALID_ORDER_ITEM_ID")
            ),
            F.when(
                F.col("_invalid_order_id"),
                F.lit("INVALID_ORDER_ID")
            ),
            F.when(
                F.col("_invalid_restaurant_id"),
                F.lit("INVALID_RESTAURANT_ID")
            ),
            F.when(
                F.col("_invalid_f_id"),
                F.lit("INVALID_FOOD_ID")
            ),
            F.when(
                F.col("_invalid_price"),
                F.lit("INVALID_PRICE_CAST")
            ),
            F.when(
                F.col("_invalid_quantity"),
                F.lit("INVALID_QUANTITY_CAST")
            ),
            F.when(
                F.col("_invalid_line_amount"),
                F.lit("INVALID_LINE_AMOUNT_CAST")
            ),
            F.when(
                F.col("_invalid_cdc_sequence"),
                F.lit("INVALID_CDC_SEQUENCE")
            ),
            F.when(
                F.col("_invalid_rescued_data"),
                F.lit("SCHEMA_DRIFT_RESCUED_DATA")
            )
        )
    )

    df = df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    return df


# ==========================================================
# 4. Temporary view: valid records for AUTO CDC
# ==========================================================

@dp.temporary_view(name="order_items_cdc_source")
def order_items_cdc_source():

    return (
        spark.readStream.table("order_items_validation_stage")
        .filter(~F.col("_is_quarantined"))
        .select(
            *CANONICAL_COLUMN_NAMES,
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_silver_processed_at"
        )
    )


# ==========================================================
# 5. Silver target: AUTO CDC upsert, SCD Type 1
# ==========================================================

dp.create_streaming_table(
    name=SILVER_TABLE,
    comment=(
        "Validated Zomato order-item records. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="order_items_cdc_source",

    # Candidate key. Verify order_item_id uniqueness in Bronze
    # before deploying this flow.
    keys=["order_item_id"],

    # Last-arriving-record-wins ordering.
    sequence_by=F.col("_bronze_ingested_at"),

    stored_as_scd_type=1,

    column_list=(
        CANONICAL_COLUMN_NAMES
        + [
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_silver_processed_at"
        ]
    ),

    name="order_items_upsert_flow"
)


# ==========================================================
# 6. Persistent incremental quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment=(
        "Incrementally captured rejected Zomato order-item records."
    )
)
def silver_order_items_quarantine():

    return (
        spark.readStream.table("order_items_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"source_{column['source_column']}"
                for column in ORDER_ITEM_COLUMNS
            ],
            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_invalid_order_item_id",
            "_invalid_order_id",
            "_invalid_restaurant_id",
            "_invalid_f_id",
            "_invalid_price",
            "_invalid_quantity",
            "_invalid_line_amount",
            "_invalid_cdc_sequence",
            "_invalid_rescued_data",
            F.col("_silver_processed_at").alias("_quarantined_at"),
            "_rejection_reason"
        )
    )
