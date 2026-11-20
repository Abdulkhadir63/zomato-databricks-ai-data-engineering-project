
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Sources and target
# ==========================================================

ORDERS_TABLE = "zomato_dev.gold.fct_orders"

RESTAURANTS_TABLE = "zomato_dev.gold.dim_restaurants"

TARGET_TABLE = "zomato_dev.gold.mart_restaurant_performance"


# ==========================================================
# 2. Gold mart: Restaurant Performance
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Restaurant performance mart. "
        "One row per restaurant."
    )
)
def mart_restaurant_performance():

    orders = spark.read.table(ORDERS_TABLE).alias("f")

    restaurants = (
        spark.read.table(RESTAURANTS_TABLE)
        .alias("r")
    )

    return (
        orders
        .join(
            restaurants,
            F.col("f.restaurant_id") == F.col("r.restaurant_id"),
            "left"
        )
        .groupBy(
            F.col("f.restaurant_id"),
            F.col("r.restaurant_name"),
            F.col("r.city"),
            F.col("r.cuisine")
        )
        .agg(
            F.count("*").alias("orders"),

            F.sum(
                F.when(
                    F.col("f.is_delivered"),
                    F.col("f.sales_amount")
                ).otherwise(F.lit(0))
            ).alias("revenue"),

            F.round(
                F.avg(F.col("f.customer_rating")),
                2
            ).alias("avg_customer_rating"),

            F.round(
                F.avg(F.col("f.delivery_time_min")),
                1
            ).alias("avg_delivery_min")
        )
    )
