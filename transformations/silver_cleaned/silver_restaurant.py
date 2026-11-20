
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
    / "restaurant.yml"
)

with open(SCHEMA_PATH, "r") as file:
    RESTAURANT_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = RESTAURANT_SCHEMA["source_table"]
SILVER_TABLE = RESTAURANT_SCHEMA["target_table"]

QUARANTINE_TABLE = "zomato_dev.quarantine.restaurant_quarantine"

RESTAURANT_COLUMNS = RESTAURANT_SCHEMA["columns"]

CANONICAL_COLUMN_NAMES = [
    column["name"] for column in RESTAURANT_COLUMNS
]

EXPECTED_COLUMNS = {
    "restaurant_id",
    "restaurant_name",
    "city",
    "rating",
    "rating_count",
    "cost_for_two",
    "cuisine",
    "license_no",
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError(
        "Invalid restaurant.yml: expected restaurant_id, restaurant_name, "
        "city, rating, rating_count, cost_for_two, cuisine and license_no."
    )

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names.")

if RESTAURANT_SCHEMA["columns"][0]["name"] != "restaurant_id":
    raise ValueError("restaurant_id must be the first canonical column.")


# ==========================================================
# 2. Data quality rules
# ==========================================================

RESTAURANT_QUALITY_RULES = {
    "valid_restaurant_id": "NOT _invalid_restaurant_id",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence",
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="restaurant_validation_stage")
@dp.expect_all(RESTAURANT_QUALITY_RULES)
def restaurant_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve original source values.
    source_columns = [
        F.col(column["source_column"]).alias(
            f"source_{column['source_column']}"
        )
        for column in RESTAURANT_COLUMNS
    ]

    # Preserve technical metadata separately.
    metadata_columns = [
        F.col("_source_file"),
        F.col("_source_file_modification_time").cast("timestamp"),
        F.col("_ingested_at").cast("timestamp").alias(
            "_bronze_ingested_at"
        ),
        F.col("_rescued_data"),
    ]

    # The source columns in the canonical schema include only columns
    # selected by Darshil's staging model. link, address and menu are
    # intentionally not part of the Silver business schema.
    df = raw_df.select(
        *source_columns,
        *metadata_columns
    )

    # ------------------------------------------------------
    # Canonical transformations driven by the source mapping
    # ------------------------------------------------------

    for column in RESTAURANT_COLUMNS:

        source_column = f"source_{column['source_column']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        # Darshil: id::NUMBER AS restaurant_id
        if target_column == "restaurant_id":
            value = F.expr(
                f"try_cast(`{source_column}` AS {data_type})"
            )

        # Darshil: name AS restaurant_name
        elif target_column == "restaurant_name":
            value = F.col(source_column)

        # Darshil: TRIM(COALESCE(REGEXP_SUBSTR(city, '[^,]+$'), city))
        elif target_column == "city":
            city_tail = F.regexp_extract(
                F.col(source_column),
                r"[^,]+$",
                0,
            )
            value = F.trim(
                F.when(
                    city_tail != "",
                    city_tail,
                ).otherwise(F.col(source_column))
            )

        # Darshil: TRY_TO_DECIMAL(NULLIF(rating, '--'), 3, 1)
        elif target_column == "rating":
            value = F.expr(
                f"try_cast(nullif(`{source_column}`, '--') AS {data_type})"
            )

        # Darshil: TRY_TO_NUMBER(REGEXP_SUBSTR(rating_count, '[0-9]+'))
        elif target_column == "rating_count":
            value = F.expr(
                "try_cast("
                f"regexp_extract(`{source_column}`, '[0-9]+', 0) "
                f"AS {data_type})"
            )

        # Darshil: TRY_TO_NUMBER(REGEXP_SUBSTR(cost, '[0-9]+'))
        elif target_column == "cost_for_two":
            value = F.expr(
                "try_cast("
                f"regexp_extract(`{source_column}`, '[0-9]+', 0) "
                f"AS {data_type})"
            )

        # Darshil: cuisine (pass-through)
        elif target_column == "cuisine":
            value = F.col(source_column)

        # Darshil: lic_no AS license_no
        elif target_column == "license_no":
            value = F.col(source_column)

        else:
            raise ValueError(
                f"No transformation defined for {target_column}."
            )

        df = df.withColumn(target_column, value)

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    # Darshil filters rows where TRY_TO_NUMBER(id) IS NOT NULL.
    # Invalid IDs are routed to quarantine rather than silently dropped.
    df = df.withColumn(
        "_invalid_restaurant_id",
        F.col("restaurant_id").isNull()
    )

    # Ingestion timestamp is the AUTO CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull()
    )

    # _rescued_data is preserved as lineage/debug information.
    # It is not treated as invalid because the source CSV has a leading
    # unnamed index column and the rescued payload may contain _c0/_file_path.

    # ------------------------------------------------------
    # Quarantine identification
    # ------------------------------------------------------

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_restaurant_id")
        | F.col("_invalid_cdc_sequence")
    )

    # Rejection reasons.
    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("_invalid_restaurant_id"),
                F.lit("INVALID_RESTAURANT_ID")
            ),
            F.when(
                F.col("_invalid_cdc_sequence"),
                F.lit("INVALID_CDC_SEQUENCE")
            ),
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

@dp.temporary_view(name="restaurant_cdc_source")
def restaurant_cdc_source():

    return (
        spark.readStream.table("restaurant_validation_stage")
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
        "Validated Zomato restaurant records. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="restaurant_cdc_source",

    keys=["restaurant_id"],

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

    name="restaurant_upsert_flow",
)


# ==========================================================
# 6. Persistent incremental quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment=(
        "Incrementally captured rejected Zomato restaurant records."
    )
)
def silver_restaurant_quarantine():

    return (
        spark.readStream.table("restaurant_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"source_{column['source_column']}"
                for column in RESTAURANT_COLUMNS
            ],
            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_invalid_restaurant_id",
            "_invalid_cdc_sequence",
            F.col("_silver_processed_at").alias("_quarantined_at"),
            "_rejection_reason",
        )
    )
