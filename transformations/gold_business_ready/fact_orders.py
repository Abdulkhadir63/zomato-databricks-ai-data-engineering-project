
from pyspark import pipelines as dp


# ==========================================================
# 1. Source and target
# ==========================================================

SOURCE_TABLE = "zomato_dev.silver.orders"

TARGET_TABLE = "zomato_dev.gold.fct_orders"


# ==========================================================
# 2. Gold fact: Orders
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Orders fact table. "
        "One row per order."
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)
def fct_orders():

    return (
        spark.read.table(SOURCE_TABLE)
        .select(
            "order_id",
            "order_timestamp",
            "order_date",
            "customer_id",
            "restaurant_id",
            "city",
            "cuisine",
            "payment_method",
            "order_status",
            "is_delivered",
            "items_count",
            "sales_qty",
            "subtotal",
            "discount",
            "delivery_fee",
            "gst",
            "sales_amount",
            "customer_rating",
            "delivery_time_min"
        )
    )
