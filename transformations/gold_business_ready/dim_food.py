
from pyspark import pipelines as dp


# ==========================================================
# 1. Source and target
# ==========================================================

SOURCE_TABLE = "zomato_dev.silver.food"

TARGET_TABLE = "zomato_dev.gold.dim_food"


# ==========================================================
# 2. Gold dimension: Food
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Food dimension. "
        "One row per food item."
    )
)
def dim_food():

    return (
        spark.read.table(SOURCE_TABLE)
        .select(
            "food_id",
            "food_name",
            "food_type"
        )
    )
