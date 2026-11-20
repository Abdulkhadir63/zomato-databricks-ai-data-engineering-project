
from pathlib import Path
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import os


# ==========================================================
# 1. Load canonical schema
# ==========================================================

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # Safe fallback when executing inside a Databricks notebook REPL
    PROJECT_ROOT = Path(os.getcwd()).resolve().parents[1]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "schemas"
    / "silver_canonical_schema"
    / "food.yml"
)

with open(SCHEMA_PATH, "r") as file:
    FOOD_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = FOOD_SCHEMA["source_table"]
SILVER_TABLE = FOOD_SCHEMA["target_table"]

QUARANTINE_TABLE = "zomato_dev.quarantine.food_quarantine"

FOOD_COLUMNS = FOOD_SCHEMA["columns"]

CANONICAL_COLUMN_NAMES = [
    column["name"] for column in FOOD_COLUMNS
]

EXPECTED_COLUMNS = {
    "food_id",
    "food_name",
    "food_type"
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError(
        "Invalid food.yml: expected food_id, food_name, food_type"
    )

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names")

if any(column.get("nullable", True) for column in FOOD_COLUMNS):
    raise ValueError("Canonical food columns must be non-nullable")


# ==========================================================
# 2. Data quality rules
# ==========================================================

FOOD_QUALITY_RULES = {
    "valid_food_id": "NOT _invalid_food_id",
    "valid_food_name": "NOT _invalid_food_name",
    "valid_food_type": "NOT _invalid_food_type",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence"
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="food_validation_stage")
@dp.expect_all(FOOD_QUALITY_RULES)
def food_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve original source values.
    source_columns = [
        F.col(column["source_column"]).alias(
            f"source_{column['source_column']}"
        )
        for column in FOOD_COLUMNS
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

    for column in FOOD_COLUMNS:

        source_column = f"source_{column['source_column']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        value = F.col(source_column).cast(data_type)

        if data_type == "STRING":
            value = F.trim(value)

        df = df.withColumn(target_column, value)

    # Standardize food type.
    df = df.withColumn(
        "food_type",
        F.when(
            F.upper(F.col("food_type")) == "VEG",
            F.lit("Veg")
        )
        .when(
            F.upper(F.col("food_type")) == "NON-VEG",
            F.lit("Non-veg")
        )
        .otherwise(F.col("food_type"))
    )

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    df = df.withColumn(
        "_invalid_food_id",
        F.col("food_id").isNull()
        | (F.length(F.col("food_id")) == 0)
        | F.upper(F.col("food_id")).isin("VEG", "NON-VEG")
    )

    df = df.withColumn(
        "_invalid_food_name",
        F.col("food_name").isNull()
        | (F.length(F.col("food_name")) == 0)
    )

    df = df.withColumn(
        "_invalid_food_type",
        F.col("food_type").isNull()
        | (F.length(F.col("food_type")) == 0)
        | (~F.upper(F.col("food_type")).isin("VEG", "NON-VEG"))
    )

    # Ingestion timestamp is the CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull()
    )

    # Preserve rescued data for investigation.
    # Non-null rescued data alone does NOT make a record invalid.
    df = df.withColumn(
        "_has_rescued_data",
        F.col("_rescued_data").isNotNull()
    )

    # ------------------------------------------------------
    # Quarantine identification
    # ------------------------------------------------------

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_food_id")
        | F.col("_invalid_food_name")
        | F.col("_invalid_food_type")
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
                F.col("_invalid_food_id"),
                F.lit("INVALID_FOOD_ID")
            ),
            F.when(
                F.col("_invalid_food_name"),
                F.lit("MISSING_FOOD_NAME")
            ),
            F.when(
                F.col("_invalid_food_type"),
                F.lit("INVALID_FOOD_TYPE")
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

@dp.temporary_view(name="food_cdc_source")
def food_cdc_source():

    return (
        spark.readStream.table("food_validation_stage")
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
        "Validated Zomato food records. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="food_cdc_source",

    # Business key
    keys=["food_id"],

    # Last-arriving-record-wins ordering
    sequence_by=F.col("_bronze_ingested_at"),

    # Keep only the latest state of each food_id
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

    name="food_upsert_flow"
)


# ==========================================================
# 6. Persistent quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment="Rejected Zomato food records with validation reasons"
)
def silver_food_quarantine():

    return (
        spark.readStream.table("food_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"source_{column['source_column']}"
                for column in FOOD_COLUMNS
            ],
            "_rescued_data",
            "_has_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_invalid_food_id",
            "_invalid_food_name",
            "_invalid_food_type",
            "_invalid_cdc_sequence",
            F.col("_silver_processed_at").alias(
                "_quarantined_at"
            ),
            "_rejection_reason"
        )
    )
