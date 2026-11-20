
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Source and target
# ==========================================================

ORDERS_TABLE = "zomato_dev.gold.fct_orders"

TARGET_TABLE = "zomato_dev.gold.mart_delivery_sla"


# ==========================================================
# 2. Gold mart: Delivery SLA
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Delivery SLA mart. "
        "One row per city per order hour."
    )
)
def mart_delivery_sla():

    orders = spark.read.table(ORDERS_TABLE)

    # ------------------------------------------------------
    # Filter delivered orders only
    # ------------------------------------------------------

    delivered_orders = orders.filter(
        F.col("is_delivered") == True
    )

    # ------------------------------------------------------
    # City and order-hour aggregations
    # ------------------------------------------------------

    return (
        delivered_orders
        .groupBy(
            F.col("city"),
            F.hour("order_timestamp").alias("order_hour")
        )
        .agg(
            F.count_if(
                F.col("is_delivered")
            ).alias("delivered_orders"),

            F.round(
                F.expr("percentile(delivery_time_min, 0.5)"),
                1
            ).alias("p50"),

            F.round(
                F.expr("percentile(delivery_time_min, 0.9)"),
                1
            ).alias("p90")
        )
        .select(
            "city",
            "order_hour",
            "delivered_orders",
            "p50",
            "p90"
        )
    )
