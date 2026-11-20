from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="zomato_dev.bronze.order_items",
    comment="Raw Bronze ingestion of Zomato order items data from S3"
)
def order_items():

    source_path = (
        "s3://databricks-zomato-ai-datalake/"
        "raw-source/order-items/"
    )

    expected_columns = [
        "order_item_id",
        "order_id",
        "r_id",
        "f_id",
        "price",
        "quantity",
        "line_amount"
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
            f"Order items schema validation failed. "
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