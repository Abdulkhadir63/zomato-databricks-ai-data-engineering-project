
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
    # Safe fallback when executing inside a Databricks notebook REPL.
    PROJECT_ROOT = Path(os.getcwd()).resolve().parents[1]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "schemas"
    / "silver_canonical_schema"
    / "users.yml"
)

with open(SCHEMA_PATH, "r") as file:
    USERS_SCHEMA = yaml.safe_load(file)

SOURCE_TABLE = USERS_SCHEMA["source_table"]
SILVER_TABLE = USERS_SCHEMA["target_table"]

QUARANTINE_TABLE = "zomato_dev.quarantine.users_quarantine"

USERS_COLUMNS = USERS_SCHEMA["columns"]

CANONICAL_COLUMN_NAMES = [
    column["name"] for column in USERS_COLUMNS
]

EXPECTED_COLUMNS = {
    "customer_id",
    "customer_name",
    "email",
    "age",
    "gender",
    "marital_status",
    "occupation",
    "income_band",
    "education",
    "family_size"
}

if set(CANONICAL_COLUMN_NAMES) != EXPECTED_COLUMNS:
    raise ValueError(
        "Invalid users.yml: expected customer_id, customer_name, email, "
        "age, gender, marital_status, occupation, income_band, "
        "education and family_size."
    )

if len(CANONICAL_COLUMN_NAMES) != len(set(CANONICAL_COLUMN_NAMES)):
    raise ValueError("Duplicate canonical column names.")

CUSTOMER_ID_CONFIG = next(
    column for column in USERS_COLUMNS
    if column["name"] == "customer_id"
)

if CUSTOMER_ID_CONFIG.get("nullable", True):
    raise ValueError("customer_id must be non-nullable.")


# ==========================================================
# 2. Data quality rules
# ==========================================================

USERS_QUALITY_RULES = {
    "valid_customer_id": "NOT _invalid_customer_id",
    "valid_cdc_sequence": "NOT _invalid_cdc_sequence"
}


# ==========================================================
# 3. Temporary view: Bronze transformation and validation
# ==========================================================

@dp.temporary_view(name="users_validation_stage")
@dp.expect_all(USERS_QUALITY_RULES)
def users_validation_stage():

    raw_df = spark.readStream.table(SOURCE_TABLE)

    # Preserve source values required for lineage and quarantine.
    # The password column is intentionally excluded and must never
    # enter Silver or the quarantine output.
    source_columns = [
        F.col(f"`{column['source_column']}`").alias(
            f"_source_{column['name']}"
        )
        for column in USERS_COLUMNS
    ]

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

    for column in USERS_COLUMNS:

        source_column = f"_source_{column['name']}"
        target_column = column["name"]
        data_type = column["data_type"].upper()

        if target_column == "email":
            # Darshil: LOWER(email)
            value = F.lower(F.col(f"`{source_column}`"))

        elif data_type == "STRING":
            # Other string columns are passed through unchanged.
            value = F.col(f"`{source_column}`")

        else:
            # Darshil: TRY_TO_NUMBER for user_id, age and family_size.
            value = F.expr(
                f"try_cast(`{source_column}` AS {data_type})"
            )

        df = df.withColumn(target_column, value)

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    # Darshil's WHERE TRY_TO_NUMBER(user_id) IS NOT NULL.
    df = df.withColumn(
        "_invalid_customer_id",
        F.col("customer_id").isNull()
    )

    # Ingestion timestamp is the AUTO CDC ordering field.
    df = df.withColumn(
        "_invalid_cdc_sequence",
        F.col("_bronze_ingested_at").isNull()
    )

    # Do not reject null age/family_size or other nullable attributes.
    # Darshil's model does not filter those fields.
    # _rescued_data is retained for inspection, not used as a filter.

    # ------------------------------------------------------
    # Quarantine identification
    # ------------------------------------------------------

    df = df.withColumn(
        "_is_quarantined",
        F.col("_invalid_customer_id")
        | F.col("_invalid_cdc_sequence")
    )

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("_invalid_customer_id"),
                F.lit("INVALID_CUSTOMER_ID")
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

@dp.temporary_view(name="users_cdc_source")
def users_cdc_source():

    return (
        spark.readStream.table("users_validation_stage")
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
# 5. Silver target: AUTO CDC SCD Type 1
# ==========================================================

dp.create_streaming_table(
    name=SILVER_TABLE,
    comment=(
        "Validated Zomato customer records. "
        "Maintained using AUTO CDC SCD Type 1."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="users_cdc_source",

    keys=["customer_id"],

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

    name="users_upsert_flow"
)


# ==========================================================
# 6. Persistent incremental quarantine table
# ==========================================================

@dp.table(
    name=QUARANTINE_TABLE,
    comment="Incrementally captured rejected Zomato user records."
)
def silver_users_quarantine():

    return (
        spark.readStream.table("users_validation_stage")
        .filter(F.col("_is_quarantined"))
        .select(
            *[
                f"_source_{column['name']}"
                for column in USERS_COLUMNS
            ],
            "_rescued_data",
            "_source_file",
            "_source_file_modification_time",
            "_bronze_ingested_at",
            "_invalid_customer_id",
            "_invalid_cdc_sequence",
            F.col("_silver_processed_at").alias(
                "_quarantined_at"
            ),
            "_rejection_reason"
        )
    )
