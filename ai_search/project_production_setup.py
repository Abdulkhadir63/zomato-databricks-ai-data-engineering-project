"""Create and validate the project runbook AI Search index.

This is the single main production setup script for the six project runbooks.
It does not create, update, merge, delete, or refresh the source Delta table.
It does not modify the separate synthetic Day 22 regression-test source/index.

Run from the Databricks Workspace folder containing config.py, index.py,
search.py, and this file, using the same Serverless Python environment that
successfully ran the Day 22 test workflow.
"""

import logging
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional

from databricks.ai_search.client import AISearchClient
from pyspark.sql import SparkSession, functions as F

try:
    from . import config as project_config
    from .config import (
        AI_SEARCH_ENDPOINT_NAME,
        AI_SEARCH_INDEX_NAME,
        AI_SEARCH_SOURCE_TABLE,
        EMBEDDING_MODEL_ENDPOINT_NAME,
        EMBEDDING_SOURCE_COLUMN,
        PRIMARY_KEY_COLUMN,
        PROJECT_PRODUCTION_DOCUMENT_IDS,
        PROJECT_PRODUCTION_METADATA_COLUMNS,
    )
    from .search import display_results, search_runbooks
except ImportError:
    import config as project_config
    from config import (
        AI_SEARCH_ENDPOINT_NAME,
        AI_SEARCH_INDEX_NAME,
        AI_SEARCH_SOURCE_TABLE,
        EMBEDDING_MODEL_ENDPOINT_NAME,
        EMBEDDING_SOURCE_COLUMN,
        PRIMARY_KEY_COLUMN,
        PROJECT_PRODUCTION_DOCUMENT_IDS,
        PROJECT_PRODUCTION_METADATA_COLUMNS,
    )
    from search import display_results, search_runbooks

logger = logging.getLogger("zomato_project_production_setup")

REQUIRED_SOURCE_COLUMNS = list(dict.fromkeys([
    PRIMARY_KEY_COLUMN,
    EMBEDDING_SOURCE_COLUMN,
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
    "project_retrieval_eligible",
]))

FAILURE_STATES = {
    "ONLINE_PIPELINE_FAILED",
    "ONLINE_FAILED",
    "OFFLINE",
    "OFFLINE_FAILED",
    "ERROR",
}


def read_table_properties(spark, table_name: str) -> Dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    return {row["key"]: row["value"] for row in rows}


def validate_existing_project_source(spark) -> Dict[str, Any]:
    """Read-only validation of the already-created project source table."""
    logger.info("Source table: %s", AI_SEARCH_SOURCE_TABLE)
    logger.info(
        "Config module loaded from: %s",
        getattr(project_config, "__file__", "unknown"),
    )
    logger.info(
        "Config source table value: %s",
        getattr(project_config, "AI_SEARCH_SOURCE_TABLE", "missing"),
    )
    logger.info("Config index name value: %s", AI_SEARCH_INDEX_NAME)

    # Do not use REFRESH TABLE. It is unsupported on this Serverless setup.
    try:
        source_df = spark.table(AI_SEARCH_SOURCE_TABLE)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read configured source table {AI_SEARCH_SOURCE_TABLE!r}. "
            "Check the table name and Unity Catalog permissions."
        ) from exc

    actual_columns = set(source_df.columns)
    missing = sorted(set(REQUIRED_SOURCE_COLUMNS) - actual_columns)
    if missing:
        raise RuntimeError(
            "Configured source table is missing required columns. "
            f"table={AI_SEARCH_SOURCE_TABLE}; missing={missing}; "
            f"actual_columns={sorted(actual_columns)}; "
            f"config_module={getattr(project_config, '__file__', 'unknown')}"
        )

    row_count = source_df.count()
    if row_count != 153:
        raise RuntimeError(f"Expected 153 source chunks, found {row_count}")

    document_ids = {
        row["document_id"]
        for row in source_df.select("document_id").distinct().collect()
    }
    expected_ids = set(PROJECT_PRODUCTION_DOCUMENT_IDS)
    if document_ids != expected_ids:
        raise RuntimeError(
            "Project document allowlist mismatch. "
            f"Missing={sorted(expected_ids - document_ids)}; "
            f"unexpected={sorted(document_ids - expected_ids)}"
        )

    duplicate_ids = (
        source_df.groupBy(PRIMARY_KEY_COLUMN)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_ids:
        raise RuntimeError("Duplicate chunk_id values found")

    invalid = source_df.filter(
        F.col(PRIMARY_KEY_COLUMN).isNull()
        | (F.trim(F.col(PRIMARY_KEY_COLUMN)) == "")
        | F.col(EMBEDDING_SOURCE_COLUMN).isNull()
        | (F.trim(F.col(EMBEDDING_SOURCE_COLUMN)) == "")
        | F.col("source_uri").isNull()
        | F.col("document_status").isNull()
        | F.col("evidence_state").isNull()
        | (F.col("project_retrieval_eligible") != F.lit(True))
    ).limit(1).count()
    if invalid:
        raise RuntimeError(
            "Source contains invalid text/provenance or ineligible chunks"
        )

    properties = read_table_properties(spark, AI_SEARCH_SOURCE_TABLE)
    cdf_enabled = (
        str(properties.get("delta.enableChangeDataFeed", "")).lower() == "true"
    )
    row_tracking_enabled = (
        str(properties.get("delta.enableRowTracking", "")).lower() == "true"
    )
    if not cdf_enabled:
        raise RuntimeError("Change Data Feed is not enabled on the project source")
    if not row_tracking_enabled:
        raise RuntimeError("Delta row tracking is not enabled on the project source")

    history = spark.sql(
        f"DESCRIBE HISTORY {AI_SEARCH_SOURCE_TABLE} LIMIT 1"
    ).first()
    if history is None:
        raise RuntimeError("Could not read Delta history for project source")

    project_eligible = source_df.filter(
        F.col("project_retrieval_eligible") == True
    ).count()
    externally_approved = source_df.filter(
        F.col("production_retrieval_eligible") == True
    ).count()

    result = {
        "source_table": AI_SEARCH_SOURCE_TABLE,
        "row_count": row_count,
        "document_count": len(document_ids),
        "unique_chunk_ids": row_count,
        "project_eligible_rows": project_eligible,
        "externally_approved_rows": externally_approved,
        "cdf_enabled": cdf_enabled,
        "row_tracking_enabled": row_tracking_enabled,
        "delta_version": int(history["version"]),
        "source_write_performed": False,
        "status": "PASS",
    }
    logger.info("Project source validation PASS: %s", result)
    return result


def latest_delta_version(spark) -> int:
    row = spark.sql(
        f"DESCRIBE HISTORY {AI_SEARCH_SOURCE_TABLE} LIMIT 1"
    ).select("version").first()
    if row is None:
        raise RuntimeError("Could not determine latest source Delta version")
    return int(row["version"])


def _int_or_default(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _triggered_progress_is_complete(
    status: Dict[str, Any], expected_rows: int
) -> bool:
    """Check explicit progress counters reported by an active triggered sync."""
    update = status.get("triggered_update_status") or {}
    progress = update.get("triggered_update_progress") or {}

    completion = progress.get("sync_progress_completion")
    try:
        completion_value = float(completion)
    except (TypeError, ValueError):
        completion_value = 0.0

    synced_rows = _int_or_default(
        progress.get("num_synced_rows", progress.get("synced_row_count")), 0
    )
    total_rows = _int_or_default(
        progress.get("total_rows_to_sync", progress.get("total_row_count")), 0
    )

    return (
        completion_value >= 1.0
        and synced_rows >= expected_rows
        and total_rows >= expected_rows
    )


def index_sync_is_complete(
    description: Dict[str, Any],
    expected_source_version: int,
    expected_rows: int,
) -> bool:
    """Confirm serving readiness, row count, source version, and full sync."""
    status = description.get("status") or {}
    state = status.get("detailed_state")
    ready = status.get("ready") is True
    indexed_rows = _int_or_default(status.get("indexed_row_count"), -1)
    update = status.get("triggered_update_status") or {}
    processed_version = _int_or_default(
        update.get("last_processed_commit_version"), -1
    )

    if not ready:
        return False
    if indexed_rows != expected_rows:
        return False
    if processed_version < expected_source_version:
        return False

    if state == "ONLINE_NO_PENDING_UPDATE":
        return True

    # Databricks reports ONLINE_TRIGGERED_UPDATE while a triggered pipeline is
    # active. It can still serve queries (ready=True). Accept it only when the
    # API reports 100% progress and all expected rows synchronized. This also
    # matches the workspace's observed 153/153 rows and successful search.
    if state == "ONLINE_TRIGGERED_UPDATE":
        return _triggered_progress_is_complete(status, expected_rows)

    return False


def wait_for_index_source_version(
    index,
    expected_source_version: int,
    expected_rows: int,
    timeout_seconds: int = 1200,
    poll_seconds: int = 10,
) -> Dict[str, Any]:
    """Wait until the index serves all rows from the expected Delta version."""
    deadline = time.monotonic() + timeout_seconds
    last_description: Dict[str, Any] = {}
    stable_observations = 0

    while time.monotonic() < deadline:
        description = index.describe()
        last_description = description
        status = description.get("status") or {}
        state = status.get("detailed_state")
        ready = status.get("ready") is True
        update = status.get("triggered_update_status") or {}
        processed_version = _int_or_default(
            update.get("last_processed_commit_version"), -1
        )
        indexed_rows = _int_or_default(status.get("indexed_row_count"), -1)
        complete = index_sync_is_complete(
            description, expected_source_version, expected_rows
        )

        logger.info(
            "Index state=%s ready=%s indexed_rows=%s processed_commit=%s "
            "expected_commit=%s sync_complete=%s",
            state,
            ready,
            indexed_rows,
            processed_version,
            expected_source_version,
            complete,
        )

        if state in FAILURE_STATES or (state and "FAILED" in state):
            raise RuntimeError(f"AI Search pipeline failed: {status}")

        if complete:
            stable_observations += 1
            if stable_observations >= 2:
                return description
        else:
            stable_observations = 0

        time.sleep(poll_seconds)

    raise TimeoutError(
        "AI Search index did not reach a verified synchronized state within "
        f"{timeout_seconds}s. Expected source Delta version={expected_source_version}; "
        f"expected rows={expected_rows}; last status="
        f"{last_description.get('status', {})}"
    )


def create_or_sync_project_index(
    spark, expected_rows: int
) -> Dict[str, Any]:
    """Create the UC Delta Sync Hybrid index if absent; sync only when needed."""
    client = AISearchClient()
    if not client.endpoint_exists(AI_SEARCH_ENDPOINT_NAME):
        raise RuntimeError(
            f"AI Search endpoint {AI_SEARCH_ENDPOINT_NAME!r} does not exist. "
            "Do not create a second endpoint; verify the existing endpoint."
        )

    expected_version = latest_delta_version(spark)
    exists = client.index_exists(
        endpoint_name=AI_SEARCH_ENDPOINT_NAME,
        index_name=AI_SEARCH_INDEX_NAME,
    )

    if exists:
        logger.info("Production index exists. Validating its source and sync status.")
        index = client.get_index(
            endpoint_name=AI_SEARCH_ENDPOINT_NAME,
            index_name=AI_SEARCH_INDEX_NAME,
        )
        current_description = index.describe()
        spec = current_description.get("delta_sync_index_spec") or {}
        actual_source = spec.get("source_table")
        if actual_source != AI_SEARCH_SOURCE_TABLE:
            raise RuntimeError(
                f"Index source mismatch: actual={actual_source!r}, "
                f"expected={AI_SEARCH_SOURCE_TABLE!r}. Nothing was changed."
            )

        if index_sync_is_complete(
            current_description, expected_version, expected_rows
        ):
            logger.info(
                "Existing production index is already ready and current. "
                "Skipping index.sync() to avoid an unnecessary triggered update."
            )
            return current_description

        logger.info(
            "Existing index is not current. Triggering sync for source version %s.",
            expected_version,
        )
        index.sync()
    else:
        columns_to_sync = list(dict.fromkeys([
            PRIMARY_KEY_COLUMN,
            EMBEDDING_SOURCE_COLUMN,
            *PROJECT_PRODUCTION_METADATA_COLUMNS,
        ]))
        logger.info("Creating Unity Catalog AI Search index: %s", AI_SEARCH_INDEX_NAME)
        index = client.create_delta_sync_index(
            endpoint_name=AI_SEARCH_ENDPOINT_NAME,
            source_table_name=AI_SEARCH_SOURCE_TABLE,
            index_name=AI_SEARCH_INDEX_NAME,
            pipeline_type="TRIGGERED",
            primary_key=PRIMARY_KEY_COLUMN,
            embedding_source_column=EMBEDDING_SOURCE_COLUMN,
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT_NAME,
            columns_to_sync=columns_to_sync,
            index_subtype="HYBRID",
        )
        logger.info("Triggering initial index.sync() for %s", AI_SEARCH_INDEX_NAME)
        index.sync()

    # SDK wait is useful for provisioning. The explicit status poll below is the
    # final authority for row count and processed source Delta version.
    index.wait_until_ready(
        verbose=True,
        timeout=timedelta(seconds=1200),
        wait_for_updates=True,
    )

    return wait_for_index_source_version(
        index=index,
        expected_source_version=expected_version,
        expected_rows=expected_rows,
        timeout_seconds=1200,
    )


def validate_index_description(
    description: Dict[str, Any],
    expected_rows: int,
    expected_source_version: int,
) -> Dict[str, Any]:
    status = description.get("status") or {}
    spec = description.get("delta_sync_index_spec") or {}
    if spec.get("source_table") != AI_SEARCH_SOURCE_TABLE:
        raise RuntimeError(f"Index source mismatch: {spec.get('source_table')!r}")

    if not index_sync_is_complete(
        description, expected_source_version, expected_rows
    ):
        raise RuntimeError(
            "Index is not verified as fully synchronized and ready. "
            f"Expected rows={expected_rows}, source version={expected_source_version}; "
            f"actual status={status}"
        )

    update = status.get("triggered_update_status") or {}
    return {
        "endpoint_name": AI_SEARCH_ENDPOINT_NAME,
        "index_name": AI_SEARCH_INDEX_NAME,
        "source_table": spec.get("source_table"),
        "index_state": status.get("detailed_state"),
        "index_ready": status.get("ready") is True,
        "indexed_rows": int(status.get("indexed_row_count")),
        "last_processed_commit_version": int(
            update.get("last_processed_commit_version")
        ),
        "sync_progress_complete": (
            status.get("detailed_state") == "ONLINE_NO_PENDING_UPDATE"
            or _triggered_progress_is_complete(status, expected_rows)
        ),
        "status": "PASS",
    }


def run_retrieval_tests(spark) -> Dict[str, Any]:
    """Run real-runbook retrieval, provenance, metadata filter and no-result tests."""
    cases = [
        {
            "name": "Pipeline failure investigation",
            "query": "The data pipeline failed. How should I investigate the failure?",
        },
        {
            "name": "Data quality and quarantine",
            "query": "How should I investigate data quality failures and quarantined records?",
        },
        {
            "name": "Replay and backfill",
            "query": "What is the safe procedure for replaying or backfilling pipeline data?",
        },
    ]

    all_records: List[Dict[str, Any]] = []
    case_summary = []
    required_fields = [
        "chunk_id", "chunk_text", "document_id", "document_title", "source_uri",
        "document_version", "section_path", "document_status", "evidence_state",
        "project_retrieval_eligible",
    ]

    for case in cases:
        logger.info("Retrieval test: %s", case["name"])
        records = search_runbooks(query=case["query"], top_k=5)
        print(f"\nQUESTION: {case['query']}")
        display_results(records)
        if not records:
            raise AssertionError(f"No runbook results for test: {case['name']}")
        for rank, record in enumerate(records, start=1):
            missing = [field for field in required_fields if record.get(field) is None]
            if missing:
                raise AssertionError(
                    f"Result {rank} missing provenance fields: {missing}"
                )
            if record.get("project_retrieval_eligible") is not True:
                raise AssertionError(
                    f"Ineligible chunk returned in {case['name']}"
                )
        all_records.extend(records)
        case_summary.append({
            "test": case["name"],
            "result_count": len(records),
            "status": "PASS",
        })

    # Restrict results to one known document and ensure no cross-document leak.
    filter_target = (
        spark.table(AI_SEARCH_SOURCE_TABLE)
        .select("document_id", "document_title")
        .dropDuplicates(["document_id"])
        .orderBy("document_id")
        .first()
    )
    filter_document_id = filter_target["document_id"]
    filter_query = filter_target["document_title"] or "operational runbook procedure"
    filtered = search_runbooks(
        query=filter_query,
        top_k=5,
        additional_filters={"document_id": filter_document_id},
    )
    if not filtered:
        raise AssertionError(
            "Metadata filter test returned no results for selected document: "
            f"{filter_document_id}"
        )
    if any(record.get("document_id") != filter_document_id for record in filtered):
        raise AssertionError("Metadata filter leaked results from another document")

    # No-result behavior is expected to return an empty normalized record list.
    no_result = search_runbooks(
        query="How should I investigate a pipeline failure?",
        top_k=3,
        additional_filters={"document_id": "DOCUMENT-DOES-NOT-EXIST"},
    )
    if no_result:
        raise AssertionError(f"Expected zero results, received {len(no_result)}")

    return {
        "query_tests": case_summary,
        "metadata_filter_test": "PASS",
        "no_result_test": "PASS",
        "retrieved_records_total": len(all_records),
        "provenance_validation": "PASS",
        "status": "PASS",
    }


def main() -> Dict[str, Any]:
    spark = SparkSession.builder.getOrCreate()
    logger.info("Starting project runbook AI Search setup")

    source_result = validate_existing_project_source(spark)
    index_description = create_or_sync_project_index(
        spark=spark,
        expected_rows=source_result["row_count"],
    )
    index_result = validate_index_description(
        description=index_description,
        expected_rows=source_result["row_count"],
        expected_source_version=source_result["delta_version"],
    )
    retrieval_result = run_retrieval_tests(spark)

    summary = {
        "source": source_result,
        "index": index_result,
        "retrieval": retrieval_result,
        "status": "PASS",
    }

    print("\n" + "=" * 72)
    print("PROJECT RUNBOOK AI SEARCH SETUP SUMMARY")
    print("=" * 72)
    print(f"Source table: {AI_SEARCH_SOURCE_TABLE}")
    print(f"Source rows: {source_result['row_count']}")
    print(f"Source documents: {source_result['document_count']}")
    print(f"Project-eligible chunks: {source_result['project_eligible_rows']}")
    print(f"Externally approved chunks: {source_result['externally_approved_rows']}")
    print(f"CDF enabled: {source_result['cdf_enabled']}")
    print(f"Row tracking enabled: {source_result['row_tracking_enabled']}")
    print(f"AI Search endpoint: {AI_SEARCH_ENDPOINT_NAME}")
    print(f"Unity Catalog index: {AI_SEARCH_INDEX_NAME}")
    print(f"Index state: {index_result['index_state']}")
    print(f"Index ready: {index_result['index_ready']}")
    print(f"Indexed rows: {index_result['indexed_rows']}")
    print(
        "Last processed source commit: "
        f"{index_result['last_processed_commit_version']}"
    )
    print(f"Sync progress complete: {index_result['sync_progress_complete']}")
    print(f"Retrieval checks: {retrieval_result['status']}")
    print("Source table modified by this script: NO")
    print("Synthetic Day 22 test source/index modified by this script: NO")
    print("=" * 72)
    print("PROJECT RUNBOOK AI SEARCH SETUP: PASS")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
