
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Sources and target
# ==========================================================

ORDER_ITEMS_TABLE = "zomato_dev.silver.order_items"
ORDERS_TABLE = "zomato_dev.silver.orders"

TARGET_TABLE = "zomato_dev.gold.fact_order_items"


# ==========================================================
# 2. Gold fact: Order Items
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Order items fact table. "
        "One row per order line item."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)
def fact_order_items():

    order_items = spark.read.table(ORDER_ITEMS_TABLE).alias("oi")

    orders = spark.read.table(ORDERS_TABLE).alias("o")

    return (
        order_items
        .join(
            orders,
            on=F.col("oi.order_id") == F.col("o.order_id"),
            how="inner"
        )
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("oi.restaurant_id"),
            F.col("oi.f_id"),
            F.col("o.order_timestamp").alias("order_timestamp"),
            F.col("o.order_date"),
            F.col("o.city"),
            F.col("oi.price"),
            F.col("oi.quantity"),
            F.col("oi.line_amount")
        )
    )
