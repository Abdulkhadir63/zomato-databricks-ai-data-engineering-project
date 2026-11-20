
from pyspark import pipelines as dp


# ==========================================================
# 1. Source and target
# ==========================================================

SOURCE_TABLE = "zomato_dev.silver.users"

TARGET_TABLE = "zomato_dev.gold.dim_customer"


# ==========================================================
# 2. Gold dimension: Customers
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Customer dimension. "
        "One row per customer."
    )
)
def dim_customer():

    return (
        spark.read.table(SOURCE_TABLE)
        .select(
            "customer_id",
            "customer_name",
            "email",
            "age",
            "gender",
            "marital_status",
            "occupation",
            "income_band",
            "education",
            "family_size"
        )
    )
