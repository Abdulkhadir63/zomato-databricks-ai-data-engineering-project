
"""
Read-only data quality and quarantine evidence tools.

Scope:
- Zomato quarantine tables only.
- Aggregated statistics and rejection reasons.
- No raw customer records.
- No unsupported pipeline-run correlation.
"""

from typing import Any, Dict, Optional

from pyspark.sql import functions as F


DATASET_TABLES = {
    "food": "zomato_dev.quarantine.food_quarantine",
    "menu": "zomato_dev.quarantine.menu_quarantine",
    "order_items": "zomato_dev.quarantine.order_items_quarantine",
    "orders": "zomato_dev.quarantine.orders_quarantine",
    "restaurant": "zomato_dev.quarantine.restaurant_quarantine",
    "reviews": "zomato_dev.quarantine.reviews_quarantine",
    "users": "zomato_dev.quarantine.users_quarantine",
}


def _as_int(value: Any) -> int:
    return int(value) if value is not None else 0


def _as_iso(value: Any) -> Optional[str]:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return str(value)


def _inspect_dataset(
    spark: Any,
    dataset: str,
    table_name: str,
) -> Dict[str, Any]:

    df = spark.table(table_name)

    columns = set(df.columns)

    required_columns = {
        "_rejection_reason",
        "_quarantined_at",
        "_source_file",
    }

    missing_columns = sorted(required_columns - columns)

    if missing_columns:
        return {
            "status": "SCHEMA_MISMATCH",
            "dataset": dataset,
            "table": table_name,
            "missing_columns": missing_columns,
        }

    invalid_columns = sorted(
        column
        for column in columns
        if column.startswith("_invalid_")
    )

    aggregates = [
        F.count(F.lit(1)).alias("quarantined_rows"),
        F.countDistinct("_source_file").alias("source_file_count"),
        F.max("_quarantined_at").alias("latest_quarantine_at"),
    ]

    aggregates.extend(
        F.sum(
            F.when(F.col(column) == True, 1).otherwise(0)
        ).alias(column)
        for column in invalid_columns
    )

    summary = df.agg(*aggregates).first().asDict()

    rejection_rows = (
        df.groupBy("_rejection_reason")
        .count()
        .orderBy(F.desc("count"))
        .limit(50)
        .collect()
    )

    rejection_reasons = [
        {
            "reason": row["_rejection_reason"] or "UNSPECIFIED",
            "count": _as_int(row["count"]),
        }
        for row in rejection_rows
    ]

    invalid_counts = {
        column: _as_int(summary.get(column))
        for column in invalid_columns
    }

    return {
        "status": "OK",
        "dataset": dataset,
        "table": table_name,
        "quarantined_rows": _as_int(
            summary.get("quarantined_rows")
        ),
        "source_file_count": _as_int(
            summary.get("source_file_count")
        ),
        "latest_quarantine_at": _as_iso(
            summary.get("latest_quarantine_at")
        ),
        "rejection_reason_counts": rejection_reasons,
        "invalid_field_counts": invalid_counts,
        "evidence_scope": "DATASET_LEVEL",
        "run_correlation": "NOT_AVAILABLE",
    }


def get_data_quality_results(
    spark: Any,
    *,
    dataset: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Retrieve aggregated quarantine evidence.

    Args:
        spark: Active SparkSession.
        dataset: Optional supported dataset name.

    Returns:
        Structured evidence dictionary.
    """

    if dataset is not None:

        if not isinstance(dataset, str) or not dataset.strip():
            return {
                "status": "INVALID_ARGUMENT",
                "error": "dataset must be a non-empty string.",
            }

        dataset = dataset.strip().lower()

        if dataset not in DATASET_TABLES:
            return {
                "status": "INVALID_ARGUMENT",
                "error": "Unsupported dataset.",
                "allowed_datasets": sorted(DATASET_TABLES),
            }

        selected_datasets = [dataset]

    else:
        selected_datasets = sorted(DATASET_TABLES)

    results = {}
    errors = []

    for name in selected_datasets:

        table_name = DATASET_TABLES[name]

        try:
            results[name] = _inspect_dataset(
                spark=spark,
                dataset=name,
                table_name=table_name,
            )

            if results[name]["status"] != "OK":
                errors.append(name)

        except Exception as exc:
            results[name] = {
                "status": "ERROR",
                "dataset": name,
                "table": table_name,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }

            errors.append(name)

    overall_status = (
        "PARTIAL" if errors else "OK"
    )

    return {
        "status": overall_status,
        "tool_name": "get_data_quality_results",
        "dataset_filter": dataset,
        "datasets": results,
        "run_correlation": "NOT_AVAILABLE",
        "correlation_limitation": (
            "Quarantine tables do not expose pipeline run IDs "
            "or update IDs. Their records cannot be attributed "
            "to a specific pipeline run using these tables alone."
        ),
        "failed_datasets": errors,
    }
