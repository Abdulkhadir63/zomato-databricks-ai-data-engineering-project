"""Databricks AI Search lifecycle helpers with source-version-aware sync checks."""

import logging
import time
from datetime import timedelta
from typing import Any, Dict, Optional

from databricks.ai_search.client import AISearchClient

try:
    from .config import (
        AI_SEARCH_ENDPOINT_NAME,
        AI_SEARCH_INDEX_NAME,
        AI_SEARCH_SOURCE_TABLE,
        EMBEDDING_MODEL_ENDPOINT_NAME,
        EMBEDDING_SOURCE_COLUMN,
        PRIMARY_KEY_COLUMN,
        PROJECT_PRODUCTION_METADATA_COLUMNS,
        TEST_INDEX_NAME,
        TEST_SOURCE_TABLE,
    )
except ImportError:
    from config import (
        AI_SEARCH_ENDPOINT_NAME,
        AI_SEARCH_INDEX_NAME,
        AI_SEARCH_SOURCE_TABLE,
        EMBEDDING_MODEL_ENDPOINT_NAME,
        EMBEDDING_SOURCE_COLUMN,
        PRIMARY_KEY_COLUMN,
        PROJECT_PRODUCTION_METADATA_COLUMNS,
        TEST_INDEX_NAME,
        TEST_SOURCE_TABLE,
    )

logger = logging.getLogger("zomato_ai_search_index")

SUCCESS_STATE = "ONLINE_NO_PENDING_UPDATE"
FAILURE_STATES = {
    "ONLINE_PIPELINE_FAILED",
    "ONLINE_FAILED",
    "OFFLINE",
    "ERROR",
}


def get_client() -> AISearchClient:
    """Use Databricks notebook authentication in the current workspace."""
    return AISearchClient()


def get_index(index_name: str, client: Optional[AISearchClient] = None):
    if not index_name:
        raise ValueError("index_name is required")
    client = client or get_client()
    logger.info("Connecting to AI Search index: %s", index_name)
    return client.get_index(
        endpoint_name=AI_SEARCH_ENDPOINT_NAME,
        index_name=index_name,
    )


def get_test_index(client: Optional[AISearchClient] = None):
    return get_index(TEST_INDEX_NAME, client)


def get_production_index(client: Optional[AISearchClient] = None):
    return get_index(AI_SEARCH_INDEX_NAME, client)


def latest_delta_version(spark, source_table_name: str) -> int:
    """Read the most recent committed Delta version for the supplied source."""
    row = spark.sql(
        f"DESCRIBE HISTORY {source_table_name} LIMIT 1"
    ).select("version").first()
    if row is None:
        raise RuntimeError(f"No Delta history found for {source_table_name}")
    return int(row["version"])


def wait_for_index_source_version(
    index,
    *,
    expected_source_version: int,
    timeout_seconds: int = 1200,
    poll_seconds: int = 10,
) -> Dict[str, Any]:
    """Wait for a clean index state AND proof that the expected Delta version was read.

    `ready=True` alone is not accepted: the index can be queryable while a triggered
    source update is still running. The method therefore requires the stable
    ONLINE_NO_PENDING_UPDATE state and last_processed_commit_version >= source version.
    """
    deadline = time.monotonic() + timeout_seconds
    last_status: Dict[str, Any] = {}
    stable_observations = 0

    while time.monotonic() < deadline:
        description = index.describe()
        status = description.get("status") or {}
        last_status = status

        state = status.get("detailed_state")
        ready = status.get("ready") is True
        update_status = status.get("triggered_update_status") or {}
        processed_version = update_status.get("last_processed_commit_version")

        try:
            processed_version = int(processed_version)
        except (TypeError, ValueError):
            processed_version = -1

        logger.info(
            "Index sync verification: state=%s, ready=%s, processed_version=%s, expected_version=%s",
            state,
            ready,
            processed_version,
            expected_source_version,
        )

        if state in FAILURE_STATES or (state and "FAILED" in state):
            raise RuntimeError(
                f"AI Search index entered failure state {state}: {status}"
            )

        if (
            state == SUCCESS_STATE
            and ready
            and processed_version >= expected_source_version
        ):
            stable_observations += 1
            if stable_observations >= 2:
                return description
        else:
            stable_observations = 0

        time.sleep(poll_seconds)

    raise TimeoutError(
        "AI Search index did not finish synchronizing within "
        f"{timeout_seconds}s. Expected Delta version {expected_source_version}. "
        f"Last status: {last_status}"
    )


def sync_index(
    index_name: str,
    source_table_name: str,
    spark,
    *,
    timeout_seconds: int = 1200,
) -> Dict[str, Any]:
    """Trigger sync and wait for the index to consume the latest source commit."""
    expected_version = latest_delta_version(spark, source_table_name)
    index = get_index(index_name)

    logger.info(
        "Triggering sync for %s; source=%s; expected_delta_version=%s",
        index_name,
        source_table_name,
        expected_version,
    )
    index.sync()

    # SDK wait is useful, but we independently verify the final state and source
    # commit version afterwards. Databricks SDK supports wait_for_updates=True.
    index.wait_until_ready(
        verbose=True,
        timeout=timedelta(seconds=timeout_seconds),
        wait_for_updates=True,
    )

    description = wait_for_index_source_version(
        index,
        expected_source_version=expected_version,
        timeout_seconds=timeout_seconds,
    )

    status = description.get("status") or {}
    logger.info(
        "Index synchronization verified: state=%s, indexed_rows=%s, processed_commit=%s",
        status.get("detailed_state"),
        status.get("indexed_row_count"),
        (status.get("triggered_update_status") or {}).get(
            "last_processed_commit_version"
        ),
    )
    return description


def sync_test_index(spark, timeout_seconds: int = 1200) -> Dict[str, Any]:
    return sync_index(
        TEST_INDEX_NAME,
        TEST_SOURCE_TABLE,
        spark,
        timeout_seconds=timeout_seconds,
    )


def sync_production_index(spark, timeout_seconds: int = 1200) -> Dict[str, Any]:
    return sync_index(
        AI_SEARCH_INDEX_NAME,
        AI_SEARCH_SOURCE_TABLE,
        spark,
        timeout_seconds=timeout_seconds,
    )


def ensure_project_production_index(spark) -> Dict[str, Any]:
    """Create the production Delta Sync Hybrid index if absent; never replace it.

    Existing indexes are checked for the expected source table. An index with a
    different source is not deleted or silently repointed.
    """
    client = get_client()
    exists = client.index_exists(
        endpoint_name=AI_SEARCH_ENDPOINT_NAME,
        index_name=AI_SEARCH_INDEX_NAME,
    )

    expected_version = latest_delta_version(spark, AI_SEARCH_SOURCE_TABLE)

    if exists:
        index = get_index(AI_SEARCH_INDEX_NAME, client)
        description = index.describe()
        spec = description.get("delta_sync_index_spec") or {}
        actual_source = spec.get("source_table")
        if actual_source != AI_SEARCH_SOURCE_TABLE:
            raise RuntimeError(
                f"Production index already exists but points to {actual_source!r}, "
                f"not {AI_SEARCH_SOURCE_TABLE!r}. No index was deleted or changed."
            )
        # Existing indexes need an explicit triggered refresh.
        index.sync()
        index.wait_until_ready(
            verbose=True,
            timeout=timedelta(minutes=20),
            wait_for_updates=True,
        )
    else:
        columns_to_sync = list(dict.fromkeys([
            PRIMARY_KEY_COLUMN,
            EMBEDDING_SOURCE_COLUMN,
            *PROJECT_PRODUCTION_METADATA_COLUMNS,
        ]))
        logger.info(
            "Creating project-production Hybrid Delta Sync index %s",
            AI_SEARCH_INDEX_NAME,
        )
        client.create_delta_sync_index_and_wait(
            endpoint_name=AI_SEARCH_ENDPOINT_NAME,
            source_table_name=AI_SEARCH_SOURCE_TABLE,
            index_name=AI_SEARCH_INDEX_NAME,
            pipeline_type="TRIGGERED",
            primary_key=PRIMARY_KEY_COLUMN,
            embedding_source_column=EMBEDDING_SOURCE_COLUMN,
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT_NAME,
            columns_to_sync=columns_to_sync,
            index_subtype="HYBRID",
            verbose=True,
            timeout=timedelta(minutes=30),
        )
        index = get_index(AI_SEARCH_INDEX_NAME, client)

    # Do not trigger a second refresh here. Poll the current create/update until
    # the index is stable and has processed the exact source snapshot version.
    return wait_for_index_source_version(
        index,
        expected_source_version=expected_version,
        timeout_seconds=1200,
    )


if __name__ == "__main__":
    raise SystemExit(
        "Import the relevant helper from a Databricks Python file. "
        "Run day_22_test_ai_search.py for regression tests or "
        "project_production_setup.py to publish the six project runbooks."
    )
