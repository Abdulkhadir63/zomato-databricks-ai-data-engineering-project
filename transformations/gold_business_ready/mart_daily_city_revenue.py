
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Sources and target
# ==========================================================

ORDERS_TABLE = "zomato_dev.gold.fct_orders"

RESTAURANTS_TABLE = "zomato_dev.gold.dim_restaurants"

TARGET_TABLE = "zomato_dev.gold.mart_daily_city_revenue"


# ==========================================================
# 2. Gold mart: Daily City Revenue
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Daily city-level business performance. "
        "One row per city per order date."
    )
)
def mart_daily_city_revenue():

    orders = spark.read.table(ORDERS_TABLE).alias("o")

    restaurants = (
        spark.read.table(RESTAURANTS_TABLE)
        .alias("r")
    )

    # ------------------------------------------------------
    # Join orders with restaurant dimension
    # ------------------------------------------------------

    joined = orders.join(
        restaurants,
        F.col("o.restaurant_id") == F.col("r.restaurant_id"),
        "inner"
    )

    # ------------------------------------------------------
    # Daily city-level aggregations
    # ------------------------------------------------------

    daily_metrics = (
        joined
        .groupBy(
            F.col("o.order_date").alias("order_date"),
            F.col("r.city").alias("city")
        )
        .agg(
            F.countDistinct(
                F.col("o.order_id")
            ).alias("total_orders"),

            F.countDistinct(
                F.when(
                    F.col("o.is_delivered") == True,
                    F.col("o.order_id")
                )
            ).alias("delivered_orders"),

            F.countDistinct(
                F.when(
                    F.upper(F.trim(F.col("o.order_status"))) == "CANCELLED",
                    F.col("o.order_id")
                )
            ).alias("cancelled_orders"),

            F.sum(
                F.when(
                    F.col("o.is_delivered") == True,
                    F.col("o.sales_amount")
                ).otherwise(F.lit(0))
            ).alias("gmv"),

            F.avg(
                F.when(
                    F.col("o.is_delivered") == True,
                    F.col("o.sales_amount")
                )
            ).alias("aov")
        )
    )

    # ------------------------------------------------------
    # Final business metrics
    # ------------------------------------------------------

    return (
        daily_metrics
        .select(
            "order_date",
            "city",
            "total_orders",
            "delivered_orders",
            "cancelled_orders",

            F.round(
                F.col("cancelled_orders") * 100.0
                / F.when(
                    F.col("total_orders") > 0,
                    F.col("total_orders")
                ),
                2
            ).alias("cancellation_rate_pct"),

            "gmv",

            F.round(
                F.col("aov"),
                2
            ).alias("aov")
        )
    )
