from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="zomato_dev.bronze.reviews",
    comment="Raw Bronze ingestion of Zomato reviews data from S3"
)
def reviews():

    source_path = (
        "s3://databricks-zomato-ai-datalake/"
        "raw-source/reviews/"
    )

    expected_columns = [
        "review_id",
        "order_id",
        "user_id",
        "restaurant_id",
        "rating",
        "comment",
        "review_date"
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
            f"Reviews schema validation failed. "
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