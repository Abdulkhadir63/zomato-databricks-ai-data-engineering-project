
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================
# 1. Date dimension configuration
# ==========================================================

TARGET_TABLE = "zomato_dev.gold.dim_date"

START_DATE = "2024-01-01"
END_DATE = "2026-12-31"


# ==========================================================
# 2. Gold dimension: Date
# ==========================================================

@dp.materialized_view(
    name=TARGET_TABLE,
    comment=(
        "Date dimension. "
        "One row per calendar date."
    )
)
def dim_date():

    date_spine = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(START_DATE)),
                    F.to_date(F.lit(END_DATE)),
                    F.expr("INTERVAL 1 DAY")
                )
            ).alias("date_day")
        )
    )

    return (
        date_spine
        .select(
            F.col("date_day"),

            F.year("date_day").alias("year"),

            F.month("date_day").alias("month"),

            F.date_format(
                "date_day",
                "MMM"
            ).alias("month_name"),

            F.date_format(
                "date_day",
                "EEE"
            ).alias("day_name"),

            F.dayofweek("date_day")
            .isin(1, 7)
            .alias("is_weekend")
        )
    )
