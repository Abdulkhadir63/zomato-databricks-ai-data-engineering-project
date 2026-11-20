
from pyspark import pipelines as dp


# ==========================================================
# 1. Source and target
# ==========================================================

SOURCE_TABLE = "zomato_dev.silver.restaurant"

TARGET_TABLE = "zomato_dev.gold.dim_restaurants"


# ==========================================================
# 2. Gold dimension: Restaurants
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Restaurant dimension. "
        "One row per restaurant."
    )
)
def dim_restaurants():

    return (
        spark.read.table(SOURCE_TABLE)
        .select(
            "restaurant_id",
            "restaurant_name",
            "city",
            "cuisine",
            "rating",
            "rating_count",
            "cost_for_two"
        )
    )
