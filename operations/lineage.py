
"""
Read-only downstream impact inspection using Unity Catalog lineage.

Capabilities:
- Discover historical downstream table dependencies.
- Traverse direct and transitive downstream relationships.
- Restrict investigation to approved Zomato data assets.
- Optionally scope lineage to one Zomato Lakeflow pipeline update.
- Preserve entity and run provenance for every discovered edge.

Limitations:
- Unity Catalog lineage only records relationships it can infer.
- Historical lineage is not automatically incident-specific.
- No downstream dependency is claimed when no lineage evidence exists.
- No data or pipeline state is modified.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pyspark.sql import functions as F


CATALOG = "zomato_dev"

PIPELINE_ID = "a83e1034-e6eb-4b57-acc8-883de68cee58"

LINEAGE_TABLE = "system.access.table_lineage"

MAX_DEPTH = 5
MAX_LOOKBACK_DAYS = 395
MAX_RESULTS = 200

ALLOWED_SCHEMAS = {
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "ai_data",
    "zomato_reviews_enrich",
}

DATASETS = [
    "food",
    "menu",
    "order_items",
    "orders",
    "restaurant",
    "reviews",
    "users",
]

GOLD_OBJECTS = [
    "dim_customer",
    "dim_date",
    "dim_food",
    "dim_restaurants",
    "fact_order_items",
    "fct_orders",
    "mart_daily_city_revenue",
    "mart_delivery_sla",
    "mart_restaurant_performance",
]

ALLOWED_SOURCE_OBJECTS = {
    *[
        f"{CATALOG}.{layer}.{dataset}"
        for layer in ("bronze", "silver", "quarantine")
        for dataset in DATASETS
    ],
    *[
        f"{CATALOG}.gold.{name}"
        for name in GOLD_OBJECTS
    ],
    f"{CATALOG}.ai_data.enriched_reviews",
    f"{CATALOG}.zomato_reviews_enrich.enriched_reviews",
}

REQUIRED_LINEAGE_COLUMNS = {
    "event_date",
    "event_time",
    "source_table_full_name",
    "source_table_catalog",
    "source_table_schema",
    "target_table_full_name",
    "target_table_catalog",
    "target_table_schema",
    "target_type",
    "entity_type",
    "entity_id",
    "entity_run_id",
    "direct_access",
}


def _is_valid_integer(
    value: Any,
    *,
    minimum: int,
    maximum: int,
) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and minimum <= value <= maximum
    )


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return str(value)


def _validate_pipeline_update_id(
    pipeline_update_id: Optional[str],
) -> Optional[str]:

    if pipeline_update_id is None:
        return None

    if not isinstance(pipeline_update_id, str):
        raise ValueError(
            "pipeline_update_id must be a valid UUID string."
        )

    try:
        return str(UUID(pipeline_update_id))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(
            "pipeline_update_id must be a valid UUID string."
        ) from exc


def _lineage_edge(row: Any) -> Dict[str, Any]:

    return {
        "source_table": row["source_table_full_name"],
        "target_table": row["target_table_full_name"],
        "target_type": row["target_type"],
        "entity_type": row["entity_type"],
        "entity_id": row["entity_id"],
        "entity_run_id": row["entity_run_id"],
        "event_time": _to_iso(row["event_time"]),
        "direct_access": row["direct_access"],
    }


def get_downstream_impact(
    spark: Any,
    *,
    table_name: str,
    pipeline_update_id: Optional[str] = None,
    max_depth: int = 3,
    lookback_days: int = 90,
    max_results: int = 100,
) -> Dict[str, Any]:
    """
    Discover downstream dependencies for an approved Zomato table.

    Args:
        spark:
            Active Databricks SparkSession.

        table_name:
            Fully qualified approved Zomato table name.

        pipeline_update_id:
            Optional exact Lakeflow pipeline update UUID.
            When supplied, only lineage records from the configured
            Zomato pipeline update are considered.

        max_depth:
            Maximum dependency traversal depth (1..5).

        lookback_days:
            Historical lineage window (1..395).

        max_results:
            Maximum lineage edges returned (1..200).

    Returns:
        Structured downstream dependency evidence.

    Status values:
        OK
        NO_MATCH
        PARTIAL
        INVALID_ARGUMENT
        MALFORMED_OUTPUT
        ERROR
    """

    # 1. Validate the source table.
    if not isinstance(table_name, str) or not table_name.strip():
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": "table_name must be a non-empty string.",
        }

    table_name = table_name.strip().lower()

    if table_name not in ALLOWED_SOURCE_OBJECTS:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": "Table is outside the approved Zomato data-asset allowlist.",
            "allowed_table_count": len(ALLOWED_SOURCE_OBJECTS),
        }

    # 2. Validate traversal bounds.
    if not _is_valid_integer(
        max_depth,
        minimum=1,
        maximum=MAX_DEPTH,
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": f"max_depth must be an integer between 1 and {MAX_DEPTH}.",
        }

    if not _is_valid_integer(
        lookback_days,
        minimum=1,
        maximum=MAX_LOOKBACK_DAYS,
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": (
                f"lookback_days must be an integer between "
                f"1 and {MAX_LOOKBACK_DAYS}."
            ),
        }

    if not _is_valid_integer(
        max_results,
        minimum=1,
        maximum=MAX_RESULTS,
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": (
                f"max_results must be an integer between "
                f"1 and {MAX_RESULTS}."
            ),
        }

    try:
        pipeline_update_id = _validate_pipeline_update_id(
            pipeline_update_id
        )

    except ValueError as exc:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": str(exc),
        }

    if spark is None:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_downstream_impact",
            "error": "spark must be the active SparkSession.",
        }

    try:
        # 3. Load and validate the Unity Catalog lineage source.
        lineage_df = spark.table(LINEAGE_TABLE)

        available_columns = set(lineage_df.columns)
        missing_columns = sorted(
            REQUIRED_LINEAGE_COLUMNS - available_columns
        )

        if missing_columns:
            return {
                "status": "MALFORMED_OUTPUT",
                "tool_name": "get_downstream_impact",
                "error": "Unity Catalog lineage schema is incomplete.",
                "missing_columns": missing_columns,
            }

        # 4. Restrict the query to the approved Zomato data platform.
        filtered_lineage = (
            lineage_df
            .where(
                F.col("event_date")
                >= F.date_sub(F.current_date(), lookback_days)
            )
            .where(
                F.col("source_table_catalog") == CATALOG
            )
            .where(
                F.col("target_table_catalog") == CATALOG
            )
            .where(
                F.col("source_table_schema").isin(
                    sorted(ALLOWED_SCHEMAS)
                )
            )
            .where(
                F.col("target_table_schema").isin(
                    sorted(ALLOWED_SCHEMAS)
                )
            )
            .where(
                F.col("target_table_full_name").isNotNull()
            )
        )

        # 5. Restrict to one specific pipeline update when requested.
        if pipeline_update_id is not None:
            filtered_lineage = (
                filtered_lineage
                .where(F.col("entity_type") == "PIPELINE")
                .where(F.col("entity_id") == PIPELINE_ID)
                .where(
                    F.lower(F.col("entity_run_id"))
                    == pipeline_update_id
                )
            )

        # 6. Traverse the observed downstream dependency graph.
        frontier_paths = {
            table_name: [table_name]
        }

        visited_nodes = {table_name}

        impacted_tables = {}
        discovered_edges = {}

        limit_reached = False

        for depth in range(1, max_depth + 1):

            if not frontier_paths:
                break

            frontier = list(frontier_paths.keys())

            remaining = max_results - len(discovered_edges)

            if remaining <= 0:
                limit_reached = True
                break

            candidates = (
                filtered_lineage
                .where(
                    F.col("source_table_full_name").isin(frontier)
                )
                .select(
                    "source_table_full_name",
                    "target_table_full_name",
                    "target_type",
                    "entity_type",
                    "entity_id",
                    "entity_run_id",
                    "event_time",
                    "direct_access",
                )
                .orderBy(F.col("event_time").desc())
                .limit(remaining + 1)
                .collect()
            )

            if len(candidates) > remaining:
                limit_reached = True
                candidates = candidates[:remaining]

            next_frontier = {}

            for row in candidates:

                source_table = row["source_table_full_name"]
                target_table = row["target_table_full_name"]

                if not source_table or not target_table:
                    continue

                if source_table == target_table:
                    continue

                edge = _lineage_edge(row)

                edge_key = (
                    edge["source_table"],
                    edge["target_table"],
                    edge["entity_type"],
                    edge["entity_id"],
                    edge["entity_run_id"],
                    edge["event_time"],
                )

                discovered_edges[edge_key] = edge

                source_path = frontier_paths.get(source_table)

                if source_path is None:
                    continue

                # Avoid circular dependency paths.
                if target_table in source_path:
                    continue

                # Record each downstream object at its shortest
                # discovered traversal depth.
                if target_table not in visited_nodes:

                    visited_nodes.add(target_table)

                    target_path = source_path + [target_table]

                    impacted_tables[target_table] = {
                        "table_name": target_table,
                        "target_type": edge["target_type"],
                        "depth": depth,
                        "dependency_path": target_path,
                    }

                    next_frontier[target_table] = target_path

            frontier_paths = next_frontier

            if limit_reached:
                break

        # 7. Normalize output.
        impacted = sorted(
            impacted_tables.values(),
            key=lambda item: (
                item["depth"],
                item["table_name"],
            ),
        )

        edges = sorted(
            discovered_edges.values(),
            key=lambda item: (
                item["event_time"] or "",
                item["source_table"],
                item["target_table"],
            ),
            reverse=True,
        )

        direct_downstream = [
            item
            for item in impacted
            if item["depth"] == 1
        ]

        if limit_reached:
            status = "PARTIAL"
        elif impacted:
            status = "OK"
        else:
            status = "NO_MATCH"

        correlation_scope = (
            "SPECIFIC_PIPELINE_UPDATE"
            if pipeline_update_id
            else "HISTORICAL_LINEAGE_WINDOW"
        )

        return {
            "status": status,
            "tool_name": "get_downstream_impact",

            "source_table": table_name,

            "query": {
                "pipeline_id": (
                    PIPELINE_ID
                    if pipeline_update_id
                    else None
                ),
                "pipeline_update_id": pipeline_update_id,
                "correlation_scope": correlation_scope,
                "max_depth": max_depth,
                "lookback_days": lookback_days,
                "max_results": max_results,
            },

            "summary": {
                "direct_downstream_count": len(direct_downstream),
                "total_downstream_count": len(impacted),
                "lineage_edge_count": len(edges),
                "limit_reached": limit_reached,
            },

            "direct_downstream": direct_downstream,
            "downstream_tables": impacted,
            "lineage_edges": edges,

            "evidence_scope": "UNITY_CATALOG_TABLE_LINEAGE",

            "limitations": [
                "Unity Catalog lineage records only relationships that Databricks can infer.",
                "No lineage match does not prove that no downstream dependency exists.",
                "Historical lineage without pipeline_update_id is not incident-specific.",
                "A pipeline-update-scoped result only describes lineage recorded for that exact update.",
                "Lineage does not by itself prove that downstream data was corrupted or required recovery.",
                "The query is restricted to approved Zomato data schemas.",
                "No pipeline, table, schema, checkpoint, or data state is modified.",
            ],
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool_name": "get_downstream_impact",
            "source_table": table_name,
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "evidence_scope": "UNITY_CATALOG_TABLE_LINEAGE",
        }
