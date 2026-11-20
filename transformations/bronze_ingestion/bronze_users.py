# from pyspark import pipelines as dp
# from pyspark.sql import functions as F


# @dp.table(
#     name="zomato_dev.bronze.users",
#     comment="Raw Bronze ingestion of Zomato users data from S3",
#     table_properties={
#         "delta.columnMapping.mode": "name"
#     }
# )
# def users():

#     source_path = (
#         "s3://databricks-zomato-ai-datalake/"
#         "raw-source/users/"
#     )

#     expected_columns = [
#         "user_id",
#         "name",
#         "email",
#         "password",
#         "Age",
#         "Gender",
#         "Marital Status",
#         "Occupation",
#         "Monthly Income",
#         "Educational Qualifications",
#         "Family size"
#     ]

#     df = (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "false")
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .option("rescuedDataColumn", "_rescued_data")
#         .load(source_path)
#     )

#     missing_columns = [
#         column for column in expected_columns
#         if column not in df.columns
#     ]

#     if missing_columns:
#         raise ValueError(
#             f"Users schema validation failed. "
#             f"Missing required columns: {missing_columns}"
#         )

#     return (
#         df
#         .withColumn("_source_file", F.col("_metadata.file_path"))
#         .withColumn(
#             "_source_file_modification_time",
#             F.col("_metadata.file_modification_time")
#         )
#         .withColumn("_ingested_at", F.current_timestamp())
#     )

# ========================================= test the model ======================================================
# ==================================================================================================================
# ====================================================================================================================


# ============================================
# USERS CSV HEADER CONTRACT VALIDATION
# ============================================

RAW_PATH = (
    "s3://databricks-zomato-ai-datalake/"
    "raw-source/users/"
)

EXPECTED_COLUMNS = [
    "user_id",
    "name",
    "email",
    "password",
    "Age",
    "Gender",
    "Marital Status",
    "Occupation",
    "Monthly Income",
    "Educational Qualifications",
    "Family size"
]


# 1. Discover CSV files recursively

def discover_csv_files(path):

    files = []

    for item in dbutils.fs.ls(path):

        if item.isDir():
            files.extend(discover_csv_files(item.path))

        elif item.path.lower().endswith(".csv"):
            files.append(item.path)

    return files


# 2. Discover incoming files

csv_files = discover_csv_files(RAW_PATH)

if not csv_files:
    raise RuntimeError("No users CSV files found in S3.")


# 3. Validate every actual CSV header

failures = []

for file_path in csv_files:

    try:

        actual_columns = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(file_path)
            .columns
        )

        missing_columns = [
            column
            for column in EXPECTED_COLUMNS
            if column not in actual_columns
        ]

        if missing_columns:

            failures.append({
                "file": file_path,
                "missing_columns": missing_columns
            })

            print(f"FAILED: {file_path}")
            print(f"Missing columns: {missing_columns}")

        else:

            print(f"PASSED: {file_path}")


    except Exception as error:

        failures.append({
            "file": file_path,
            "error": str(error)
        })


# 4. Fail the task if ANY file violates the contract

if failures:

    raise RuntimeError(
        f"USERS SOURCE CONTRACT VIOLATION: "
        f"{len(failures)} invalid CSV file(s). "
        f"Lakeflow pipeline execution is blocked. "
        f"Details: {failures}"
    )


print(
    f"SUCCESS: All {len(csv_files)} users CSV files "
    f"contain all {len(EXPECTED_COLUMNS)} required columns."
)