from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="zomato_dev.bronze.orders",
    comment="Raw Bronze ingestion of Zomato orders data from S3"
)
def orders():

    source_path = (
        "s3://databricks-zomato-ai-datalake/"
        "raw-source/orders/"
    )

    expected_columns = [
        "order_id",
        "order_timestamp",
        "order_date",
        "user_id",
        "r_id",
        "restaurant_city",
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
        "customer_rating",
        "delivery_time_min"
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
            f"Orders schema validation failed. "
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