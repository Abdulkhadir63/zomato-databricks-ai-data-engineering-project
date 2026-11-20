from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="zomato_dev.bronze.menu",
    comment="Raw Bronze ingestion of Zomato menu data from S3"
)
def menu():

    source_path = (
        "s3://databricks-zomato-ai-datalake/"
        "raw-source/menu/"
    )

    expected_columns = [
        "menu_id",
        "r_id",
        "f_id",
        "cuisine",
        "price"
    ]

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )

    missing_columns = [
        column for column in expected_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Menu schema validation failed. "
            f"Missing required columns: {missing_columns}"
        )

    return (
        df
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time")
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )