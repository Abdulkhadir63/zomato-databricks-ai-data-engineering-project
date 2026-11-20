"""Reusable Hybrid Search retrieval interfaces for the project AI Agent."""

import logging
from typing import Any, Dict, Iterable, List, Optional

try:
    from .config import (
        AI_SEARCH_INDEX_NAME,
        TEST_INDEX_NAME,
        DEFAULT_TOP_K,
        PRODUCTION_RETURN_COLUMNS,
        TEST_RETURN_COLUMNS,
        PRODUCTION_ALLOWED_FILTERS,
        TEST_ALLOWED_FILTERS,
    )
    from .index import get_index
except ImportError:
    from config import (
        AI_SEARCH_INDEX_NAME,
        TEST_INDEX_NAME,
        DEFAULT_TOP_K,
        PRODUCTION_RETURN_COLUMNS,
        TEST_RETURN_COLUMNS,
        PRODUCTION_ALLOWED_FILTERS,
        TEST_ALLOWED_FILTERS,
    )
    from index import get_index

logger = logging.getLogger("zomato_ai_search")


def normalize_results(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(response, dict):
        raise TypeError(f"Unexpected AI Search response type: {type(response)!r}")

    manifest = response.get("manifest") or {}
    result = response.get("result") or {}
    columns_meta = manifest.get("columns") or []
    column_names = [
        item.get("name") if isinstance(item, dict) else str(item)
        for item in columns_meta
    ]

    normalized = []
    for row in result.get("data_array") or []:
        record = dict(zip(column_names, row))
        record.setdefault("score", None)
        normalized.append(record)
    return normalized


def _validate_filters(
    additional_filters: Optional[Dict[str, Any]],
    allowed_filters: Iterable[str],
) -> Dict[str, Any]:
    filters = dict(additional_filters or {})
    unknown = sorted(set(filters) - set(allowed_filters))
    if unknown:
        raise ValueError(f"Unsupported metadata filter(s): {unknown}")
    return filters


def _search(
    *,
    query: str,
    index_name: str,
    top_k: int,
    mandatory_filters: Dict[str, Any],
    additional_filters: Optional[Dict[str, Any]],
    allowed_filters: Iterable[str],
    return_columns: List[str],
    expected_flag: str,
) -> List[Dict[str, Any]]:
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    if not isinstance(top_k, int) or top_k < 1:
        raise ValueError("top_k must be a positive integer")

    filters = _validate_filters(additional_filters, allowed_filters)
    for key, value in mandatory_filters.items():
        if key in filters and filters[key] != value:
            raise ValueError(f"Filter {key!r} is reserved by this retrieval path")
        filters[key] = value

    logger.info("Connecting to index: %s", index_name)
    index = get_index(index_name)
    logger.info("Executing Hybrid retrieval. Top-K=%s", top_k)
    response = index.similarity_search(
        query_text=query,
        columns=return_columns,
        num_results=top_k,
        query_type="HYBRID",
        filters=filters,
    )
    records = normalize_results(response)

    safe_records = [
        row for row in records
        if row.get(expected_flag) is True
    ]
    logger.info("Search completed. Retrieved=%s", len(safe_records))
    return safe_records


def search_runbooks(
    query: str,
    top_k: int = DEFAULT_TOP_K,
    additional_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Search the six project-production runbooks and return source evidence."""
    return _search(
        query=query,
        index_name=AI_SEARCH_INDEX_NAME,
        top_k=top_k,
        mandatory_filters={"project_retrieval_eligible": True},
        additional_filters=additional_filters,
        allowed_filters=PRODUCTION_ALLOWED_FILTERS,
        return_columns=PRODUCTION_RETURN_COLUMNS,
        expected_flag="project_retrieval_eligible",
    )


def search_test_runbooks(
    query: str,
    top_k: int = DEFAULT_TOP_K,
    additional_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Search only synthetic regression-test fixtures."""
    return _search(
        query=query,
        index_name=TEST_INDEX_NAME,
        top_k=top_k,
        mandatory_filters={"is_test_fixture": True},
        additional_filters=additional_filters,
        allowed_filters=TEST_ALLOWED_FILTERS,
        return_columns=TEST_RETURN_COLUMNS,
        expected_flag="is_test_fixture",
    )


def display_results(results: List[Dict[str, Any]]) -> None:
    if not results:
        print("No useful runbook chunks found.")
        return

    for rank, record in enumerate(results, start=1):
        print(f"\nRESULT RANK: {rank}")
        for field in (
            "chunk_id", "document_id", "document_title", "document_version",
            "section_path", "source_file", "source_uri", "source_line_start",
            "source_line_end", "document_status", "evidence_state",
            "project_retrieval_eligible", "score",
        ):
            print(f"{field.upper()}: {record.get(field)}")
        print("CHUNK TEXT:")
        print(record.get("chunk_text", ""))


if __name__ == "__main__":
    raise SystemExit(
        "Import search_runbooks() for project-production retrieval or "
        "search_test_runbooks() for synthetic regression tests."
    )
