"""Publish the user's six operational runbooks as project-production AI Search data.

Project-production means these documents are intentionally used by this personal
project's assistant. It does NOT claim that Zomato has formally approved them.
Original document_status, evidence_state and production_retrieval_eligible values
are preserved so the future agent can communicate evidence limitations.
"""

import logging
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as F

try:
    from .config import (
        ACTIVE_CHUNKS_VIEW,
        AI_SEARCH_SOURCE_TABLE,
        MAX_EMBEDDING_TEXT_BYTES,
        PROJECT_PRODUCTION_DOCUMENT_IDS,
    )
except ImportError:
    from config import (
        ACTIVE_CHUNKS_VIEW,
        AI_SEARCH_SOURCE_TABLE,
        MAX_EMBEDDING_TEXT_BYTES,
        PROJECT_PRODUCTION_DOCUMENT_IDS,
    )

logger = logging.getLogger("zomato_ai_search_source")

SOURCE_COLUMNS = [
    "chunk_id",
    "chunk_text",
    "document_id",
    "document_title",
    "source_file",
    "source_uri",
    "document_version",
    "source_sha256",
    "section_path",
    "source_line_start",
    "source_line_end",
    "document_status",
    "evidence_state",
    "production_retrieval_eligible",
]
OUTPUT_COLUMNS = SOURCE_COLUMNS + ["project_retrieval_eligible"]


def build_project_production_source(spark) -> DataFrame:
    """Take current active chunks for the six explicitly selected project docs."""
    active_df = spark.table(ACTIVE_CHUNKS_VIEW)
    missing = sorted(set(SOURCE_COLUMNS) - set(active_df.columns))
    if missing:
        raise ValueError(
            f"{ACTIVE_CHUNKS_VIEW} is missing required columns: {missing}"
        )

    source_df = (
        active_df
        .filter(F.col("document_id").isin(PROJECT_PRODUCTION_DOCUMENT_IDS))
        .select(*SOURCE_COLUMNS)
        # This is a project-owner publication decision. Do not overwrite the
        # independent production_retrieval_eligible or evidence-state columns.
        .withColumn("project_retrieval_eligible", F.lit(True))
    )
    return source_df.select(*OUTPUT_COLUMNS)


def validate_project_production_source(source_df: DataFrame) -> Dict[str, Any]:
    row_count = source_df.count()
    if row_count == 0:
        raise ValueError("No active chunks found for the project runbook allowlist")

    actual_document_ids = {
        row["document_id"]
        for row in source_df.select("document_id").distinct().collect()
    }
    expected_document_ids = set(PROJECT_PRODUCTION_DOCUMENT_IDS)
    missing_documents = sorted(expected_document_ids - actual_document_ids)
    unexpected_documents = sorted(actual_document_ids - expected_document_ids)
    if missing_documents or unexpected_documents:
        raise ValueError(
            "Project runbook allowlist mismatch. "
            f"Missing={missing_documents}; unexpected={unexpected_documents}"
        )

    duplicate_chunk = (
        source_df.groupBy("chunk_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_chunk:
        raise ValueError("Duplicate chunk_id values in project-production source")

    invalid = source_df.filter(
        F.col("chunk_id").isNull()
        | (F.trim(F.col("chunk_id")) == "")
        | F.col("chunk_text").isNull()
        | (F.trim(F.col("chunk_text")) == "")
        | (F.col("project_retrieval_eligible") != F.lit(True))
        | F.col("document_status").isNull()
        | F.col("evidence_state").isNull()
    ).limit(1).count()
    if invalid:
        raise ValueError("Invalid key/text/project eligibility/evidence metadata")

    oversized = source_df.filter(
        F.length(F.encode(F.col("chunk_text"), "UTF-8"))
        > MAX_EMBEDDING_TEXT_BYTES
    ).limit(1).count()
    if oversized:
        raise ValueError(
            f"A chunk exceeds {MAX_EMBEDDING_TEXT_BYTES} UTF-8 bytes"
        )

    # Count by document so the owner can confirm the full corpus was selected.
    by_document = {
        row["document_id"]: int(row["count"])
        for row in source_df.groupBy("document_id").count().collect()
    }
    return {
        "row_count": row_count,
        "document_count": len(actual_document_ids),
        "chunks_by_document": by_document,
        "status": "PASS",
    }


def _ensure_source_table(spark, source_df: DataFrame) -> None:
    if not spark.catalog.tableExists(AI_SEARCH_SOURCE_TABLE):
        # Create an empty table first so CDF is enabled before data is published.
        (source_df.limit(0).write.format("delta").mode("errorifexists")
         .saveAsTable(AI_SEARCH_SOURCE_TABLE))
    else:
        existing_columns = set(spark.table(AI_SEARCH_SOURCE_TABLE).columns)
        missing_columns = set(OUTPUT_COLUMNS) - existing_columns
        for column in sorted(missing_columns):
            if column == "project_retrieval_eligible":
                spark.sql(
                    f"ALTER TABLE {AI_SEARCH_SOURCE_TABLE} "
                    "ADD COLUMNS (project_retrieval_eligible BOOLEAN)"
                )
            else:
                raise RuntimeError(
                    f"Existing source table is missing required column {column!r}. "
                    "Review the table schema before publishing."
                )

        # Extra nullable columns on a previously created source table are left
        # untouched. The project index syncs only the explicit columns_to_sync list.

    # Standard AI Search endpoint source tables require CDF. Row tracking is also
    # enabled so downstream Delta features remain available.
    spark.sql(f"""
        ALTER TABLE {AI_SEARCH_SOURCE_TABLE}
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.enableRowTracking' = 'true'
        )
    """)


def publish_project_production_source(spark) -> Dict[str, Any]:
    """Validate and MERGE a full active-document snapshot to AI Search source."""
    source_df = build_project_production_source(spark)
    validation = validate_project_production_source(source_df)
    _ensure_source_table(spark, source_df)

    source_df.createOrReplaceTempView("project_production_ai_search_snapshot")
    update_set = ",\n".join(
        f"target.{column} = source.{column}"
        for column in OUTPUT_COLUMNS
        if column != "chunk_id"
    )
    insert_columns = ", ".join(OUTPUT_COLUMNS)
    insert_values = ", ".join(f"source.{column}" for column in OUTPUT_COLUMNS)

    spark.sql(f"""
        MERGE INTO {AI_SEARCH_SOURCE_TABLE} AS target
        USING project_production_ai_search_snapshot AS source
          ON target.chunk_id = source.chunk_id
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)

    final_df = spark.table(AI_SEARCH_SOURCE_TABLE)
    final_count = final_df.count()
    if final_count != validation["row_count"]:
        raise RuntimeError(
            f"Published row count {final_count} does not match "
            f"validated snapshot {validation['row_count']}"
        )

    final_invalid = final_df.filter(
        F.col("project_retrieval_eligible") != F.lit(True)
    ).limit(1).count()
    if final_invalid:
        raise RuntimeError("Published source contains ineligible project chunks")

    latest_version = spark.sql(
        f"DESCRIBE HISTORY {AI_SEARCH_SOURCE_TABLE} LIMIT 1"
    ).select("version").first()["version"]

    result = {
        "source_table": AI_SEARCH_SOURCE_TABLE,
        "published_row_count": final_count,
        "delta_version": int(latest_version),
        "document_count": validation["document_count"],
        "chunks_by_document": validation["chunks_by_document"],
        "source_change_data_feed": True,
        "source_row_tracking": True,
        "project_scope": "PERSONAL_PROJECT_PRODUCTION",
        "status": "PASS",
    }
    logger.info("Project-production source published: %s", result)
    return result


if __name__ == "__main__":
    raise SystemExit(
        "Run project_production_setup.py to publish and index the six project runbooks."
    )
