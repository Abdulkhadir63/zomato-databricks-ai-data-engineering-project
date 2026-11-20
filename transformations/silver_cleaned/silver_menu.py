
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
    / "menu.yml"
)

with open(SCHEMA_PATH, "r") as file:
    MENU_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = MENU_SCHEMA["source_table"]
SILVER_TABLE = MENU_SCHEMA["target_table"]

QUARANTINE_TABLE = "zomato_dev.quarantine.menu_quarantine"

MENU_COLUMNS = MENU_SCHEMA["columns"]

CANONICAL_COLUMN_NAMES = [
    column["name"] for column in MENU_COLUMNS
]

EXPECTED_COLUMNS = {
    "menu_id",
    "restaurant_id",
    "food_id",
    "cuisine",
    "price"
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError(
        "Invalid menu.yml: expected menu_id, restaurant_id, "
        "food_id, cuisine and price."
    )

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names.")

if any(column.get("nullable", True) for column in MENU_COLUMNS):
    raise ValueError("Canonical menu columns must be non-nullable.")


# ==========================================================
# 2. Data quality rules
# ==========================================================

MENU_QUALITY_RULES = {
    "valid_menu_id": "NOT _invalid_menu_id",
    "valid_restaurant_id": "NOT _invalid_restaurant_id",
    "valid_food_id": "NOT _invalid_food_id",
    "valid_cuisine": "NOT _invalid_cuisine",
    "valid_price": "NOT _invalid_price",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence"
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="menu_validation_stage")
@dp.expect_all(MENU_QUALITY_RULES)
def menu_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve original source values.
    source_columns = [
        F.col(column["source_column"]).alias(
            f"source_{column['source_column']}"
        )
        for column in MENU_COLUMNS
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
    # Canonical transformations driven by YAML
    # ------------------------------------------------------

    for column in MENU_COLUMNS:

        source_column = f"source_{column['source_column']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        if data_type == "STRING":

            value = F.trim(F.col(source_column))

        else:

            # TRY_CAST prevents malformed numeric values
            # from failing the entire streaming query.
            value = F.expr(
                f"try_cast(`{source_column}` AS {data_type})"
            )

        df = df.withColumn(
            target_column,
            value
        )

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    df = df.withColumn(
        "_invalid_menu_id",
        F.col("menu_id").isNull()
        | (F.length(F.col("menu_id")) == 0)
    )

    df = df.withColumn(
        "_invalid_restaurant_id",
        F.col("restaurant_id").isNull()
    )

    df = df.withColumn(
        "_invalid_food_id",
        F.col("food_id").isNull()
        | (F.length(F.col("food_id")) == 0)
    )

    df = df.withColumn(
        "_invalid_cuisine",
        F.col("cuisine").isNull()
        | (F.length(F.col("cuisine")) == 0)
    )

    # Darshil Parmar transformation:
    # TRY_TO_DECIMAL(price, 10, 2) > 0
    df = df.withColumn(
        "_invalid_price",
        F.col("price").isNull()
        | (F.col("price") <= 0)
    )

    # Ingestion timestamp is the AUTO CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull()
    )

    # NOTE:
    # _rescued_data is intentionally not treated as invalid.
    # Current profiling shows it populated for every Bronze row
    # with known fields such as _c0 and _file_path.

    # ------------------------------------------------------
    # Quarantine identification
    # ------------------------------------------------------

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_menu_id")
        | F.col("_invalid_restaurant_id")
        | F.col("_invalid_food_id")
        | F.col("_invalid_cuisine")
        | F.col("_invalid_price")
        | F.col("_invalid_cdc_sequence")
    )

    # ------------------------------------------------------
    # Rejection reasons
    # ------------------------------------------------------

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("_invalid_menu_id"),
                F.lit("INVALID_MENU_ID")
            ),
            F.when(
                F.col("_invalid_restaurant_id"),
                F.lit("INVALID_RESTAURANT_ID")
            ),
            F.when(
                F.col("_invalid_food_id"),
                F.lit("INVALID_FOOD_ID")
            ),
            F.when(
                F.col("_invalid_cuisine"),
                F.lit("MISSING_CUISINE")
            ),
            F.when(
                F.col("_invalid_price"),
                F.lit("INVALID_OR_NON_POSITIVE_PRICE")
            ),
            F.when(
                F.col("_invalid_cdc_sequence"),
                F.lit("INVALID_CDC_SEQUENCE")
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

@dp.temporary_view(name="menu_cdc_source")
def menu_cdc_source():

    return (
        spark.readStream.table("menu_validation_stage")
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
# 5. Silver target: AUTO CDC upsert
# ==========================================================

dp.create_streaming_table(
    name=SILVER_TABLE,
    comment=(
        "Validated Zomato menu records. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="menu_cdc_source",

    # Composite business key retained from existing design.
    keys=[
        "menu_id",
        "restaurant_id",
        "food_id"
    ],

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

    name="menu_upsert_flow"
)


# ==========================================================
# 6. Persistent incremental quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment="Incrementally captured rejected Zomato menu records."
)
def silver_menu_quarantine():

    return (
        spark.readStream.table("menu_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"source_{column['source_column']}"
                for column in MENU_COLUMNS
            ],

            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",

            "_invalid_menu_id",
            "_invalid_restaurant_id",
            "_invalid_food_id",
            "_invalid_cuisine",
            "_invalid_price",
            "_invalid_cdc_sequence",

            F.col("_silver_processed_at").alias(
                "_quarantined_at"
            ),

            "_rejection_reason"
        )
    )
