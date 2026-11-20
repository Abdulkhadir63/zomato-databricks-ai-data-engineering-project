
"""
Read-only Delta table history inspection.

Supported:
- Approved Bronze Delta tables.
- Approved Silver Delta tables.
- Approved Gold materialized views are identified but do not
  receive direct DESCRIBE HISTORY requests.

Security:
- Strict object allowlist.
- No arbitrary SQL identifiers.
- Bounded history limit.
- Email principals and potentially sensitive operation
  parameters are redacted.
- No data modification.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict


DATASETS = [
    "food",
    "menu",
    "order_items",
    "orders",
    "restaurant",
    "reviews",
    "users",
]

PHYSICAL_TABLES = {
    f"zomato_dev.{layer}.{dataset}"
    for layer in ("bronze", "silver")
    for dataset in DATASETS
}

GOLD_MATERIALIZED_VIEWS = {
    "zomato_dev.gold.dim_customer",
    "zomato_dev.gold.dim_date",
    "zomato_dev.gold.dim_food",
    "zomato_dev.gold.dim_restaurants",
    "zomato_dev.gold.fact_order_items",
    "zomato_dev.gold.fct_orders",
    "zomato_dev.gold.mart_daily_city_revenue",
    "zomato_dev.gold.mart_delivery_sla",
    "zomato_dev.gold.mart_restaurant_performance",
}

ALLOWED_OBJECTS = (
    PHYSICAL_TABLES | GOLD_MATERIALIZED_VIEWS
)

MAX_HISTORY_LIMIT = 100

SENSITIVE_PARAMETER_KEYS = {
    "predicate",
    "matchedpredicates",
    "notmatchedpredicates",
    "condition",
    "where",
    "query",
    "sql",
    "path",
    "location",
}


def _safe_value(value: Any) -> Any:
    """Convert Spark values into JSON-compatible values."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {
            str(key): _safe_value(item)
            for key, item in value.items()
            if str(key).lower() not in SENSITIVE_PARAMETER_KEYS
        }

    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]

    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    return str(value)


def _safe_actor(value: Any) -> str:
    """Avoid exposing email principals in agent responses."""

    if value is None:
        return "UNKNOWN"

    actor = str(value)

    if "@" in actor:
        return "[REDACTED_EMAIL_PRINCIPAL]"

    return actor


def _object_parts(table_name: str):
    parts = table_name.split(".")

    if len(parts) != 3:
        raise ValueError("Expected a three-part table identifier.")

    return parts


def get_delta_table_history(
    spark: Any,
    *,
    table_name: str,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Retrieve bounded history for an approved Delta table.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified approved object name.
        limit: Number of recent operations, between 1 and 100.

    Returns:
        Structured history evidence.
    """

    # 1. Validate object argument.
    if not isinstance(table_name, str) or not table_name.strip():
        return {
            "status": "INVALID_ARGUMENT",
            "error": "table_name must be a non-empty string.",
        }

    table_name = table_name.strip().lower()

    if table_name not in ALLOWED_OBJECTS:
        return {
            "status": "INVALID_ARGUMENT",
            "error": "Object is outside the approved allowlist.",
            "allowed_object_count": len(ALLOWED_OBJECTS),
        }

    # 2. Validate bounded history limit.
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAX_HISTORY_LIMIT
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "error": (
                f"limit must be an integer between 1 and "
                f"{MAX_HISTORY_LIMIT}."
            ),
        }

    catalog, schema, object_name = _object_parts(table_name)

    try:
        # Information schema visibility is governed by UC privileges.
        metadata = spark.sql(
            f"""
            SELECT table_type
            FROM `{catalog}`.information_schema.tables
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{object_name}'
            """
        ).first()

        if metadata is None:
            return {
                "status": "NOT_FOUND_OR_NOT_VISIBLE",
                "table_name": table_name,
                "message": (
                    "Object was not found in visible Unity Catalog "
                    "metadata. It may be missing or inaccessible."
                ),
            }

        object_type = metadata["table_type"]

        # 3. Materialized views do not have direct Delta history.
        if object_type == "MATERIALIZED_VIEW":
            return {
                "status": "NOT_APPLICABLE",
                "table_name": table_name,
                "object_type": object_type,
                "message": (
                    "Direct DESCRIBE HISTORY is not supported "
                    "for this materialized view."
                ),
                "alternative_evidence": (
                    "Use operations.pipeline_events.get_pipeline_events "
                    "with the Zomato pipeline ID to inspect refresh events."
                ),
            }

        # 4. Accept supported physical table types.
        # Lakeflow pipeline tables appear as STREAMING_TABLE
        # in Unity Catalog information_schema.tables.
        if object_type not in {
            "MANAGED",
            "STREAMING_TABLE",
        }:
            return {
                "status": "UNSUPPORTED_OBJECT_TYPE",
                "table_name": table_name,
                "object_type": object_type,
                "message": (
                    "This tool only inspects approved physical "
                    "managed or streaming Delta tables."
                ),
            }


        # The SQL identifier is safe because it was resolved
        # exclusively from the static allowlist above.
        qualified_identifier = (
            f"`{catalog}`.`{schema}`.`{object_name}`"
        )

        history_df = spark.sql(
            f"DESCRIBE HISTORY {qualified_identifier} LIMIT {limit}"
        )

        rows = history_df.collect()

        history_records = []

        for row in rows:
            record = row.asDict(recursive=True)

            history_records.append({
                "version": _safe_value(record.get("version")),
                "timestamp": _safe_value(record.get("timestamp")),
                "actor": _safe_actor(record.get("userName")),
                "operation": _safe_value(record.get("operation")),
                "operation_parameters": _safe_value(
                    record.get("operationParameters")
                ),
                "operation_metrics": _safe_value(
                    record.get("operationMetrics")
                ),
            })

        return {
            "status": "OK",
            "tool_name": "get_delta_table_history",
            "table_name": table_name,
            "object_type": object_type,
            "requested_history_limit": limit,
            "returned_history_count": len(history_records),
            "latest_version": (
                history_records[0]["version"]
                if history_records
                else None
            ),
            "history": history_records,
            "evidence_scope": "DELTA_TABLE_HISTORY",
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool_name": "get_delta_table_history",
            "table_name": table_name,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:500],
        }
