"""Safe, opt-in MLflow span helpers.

The helpers are no-ops outside an explicitly enabled tracing session. This
keeps ordinary project runs and existing unit tests independent of MLflow
tracing while allowing nested spans during a traced investigation.
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Mapping, Optional

_TRACING_ENABLED: ContextVar[bool] = ContextVar(
    "zomato_mlflow_tracing_enabled",
    default=False,
)

_ALLOWED_TOOL_ARGUMENTS = {
    "run_id",
    "status_filter",
    "limit",
    "lookback_days",
    "event_limit",
    "dataset",
    "table_name",
    "pipeline_update_id",
    "max_depth",
    "max_results",
    "top_k",
}

_COUNTABLE_OUTPUT_FIELDS = (
    "tasks",
    "downstream_tables",
    "evidence",
    "datasets",
    "history",
    "results",
    "citations",
)


@contextmanager
def tracing_session() -> Iterator[None]:
    """Enable child spans for the current synchronous execution context."""
    token = _TRACING_ENABLED.set(True)
    try:
        yield
    finally:
        _TRACING_ENABLED.reset(token)


def is_tracing_enabled() -> bool:
    return _TRACING_ENABLED.get()


@contextmanager
def child_span(
    *,
    name: str,
    span_type: str,
    inputs: Optional[Mapping[str, Any]] = None,
    attributes: Optional[Mapping[str, Any]] = None,
) -> Iterator[Any]:
    """Create a nested span only inside an explicitly traced agent execution."""
    if not _TRACING_ENABLED.get():
        yield None
        return

    import mlflow

    with mlflow.start_span(name=name, span_type=span_type) as span:
        if inputs:
            span.set_inputs(dict(inputs))
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(str(key), value)
        yield span


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def summarize_tool_inputs(
    tool_name: str,
    arguments: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return an allowlisted argument projection safe for trace storage.

    WorkspaceClient, SparkSession, raw prompts and runbook query text are never
    stored as span inputs. Runbook queries are represented by length + SHA-256.
    """
    safe: Dict[str, Any] = {"tool_name": tool_name}

    for key in _ALLOWED_TOOL_ARGUMENTS:
        value = arguments.get(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            if value is not None:
                safe[key] = value

    query = arguments.get("query")
    if isinstance(query, str):
        safe["query_chars"] = len(query)
        safe["query_sha256"] = _hash_text(query)

    filters = arguments.get("additional_filters")
    if isinstance(filters, Mapping):
        safe["additional_filter_keys"] = sorted(str(key) for key in filters.keys())

    return safe


def summarize_tool_output(
    tool_name: str,
    result: Any,
) -> Dict[str, Any]:
    """Summarize a governed tool response without tracing raw evidence payloads."""
    if not isinstance(result, Mapping):
        return {
            "tool_name": tool_name,
            "status": "MALFORMED_OUTPUT",
            "result_type": type(result).__name__,
        }

    summary: Dict[str, Any] = {
        "tool_name": tool_name,
        "status": str(result.get("status", "UNKNOWN")),
        "returned_keys": sorted(str(key) for key in result.keys()),
    }

    error_type = result.get("error_type")
    if isinstance(error_type, str):
        summary["error_type"] = error_type

    for field in _COUNTABLE_OUTPUT_FIELDS:
        value = result.get(field)
        if isinstance(value, (list, tuple, dict)):
            summary[f"{field}_count"] = len(value)

    evidence = result.get("evidence")
    if isinstance(evidence, list):
        chunk_ids = [
            row.get("chunk_id")
            for row in evidence
            if isinstance(row, Mapping) and isinstance(row.get("chunk_id"), str)
        ]
        document_ids = sorted(
            {
                str(row.get("document_id"))
                for row in evidence
                if isinstance(row, Mapping) and row.get("document_id") is not None
            }
        )
        summary["retrieved_chunk_ids"] = chunk_ids
        summary["retrieved_document_ids"] = document_ids

    return summary


def summarize_investigation_report(report: Mapping[str, Any]) -> Dict[str, Any]:
    """Create a compact, non-content trace output for the final agent report."""
    if not isinstance(report, Mapping):
        return {"report_status": "MALFORMED_OUTPUT"}

    rag = report.get("runbook_analysis")
    rag = rag if isinstance(rag, Mapping) else {}
    grounding = report.get("answer_grounding")
    grounding = grounding if isinstance(grounding, Mapping) else {}
    policy = report.get("execution_policy")
    policy = policy if isinstance(policy, Mapping) else {}

    calls = report.get("tool_calls")
    calls = calls if isinstance(calls, list) else []

    return {
        "incident_id": report.get("incident_id"),
        "run_id": report.get("run_id"),
        "workflow_version": report.get("schema_version"),
        "workflow_execution_status": report.get("workflow_execution_status"),
        "investigation_status": report.get("investigation_status"),
        "evidence_completeness": report.get("evidence_completeness"),
        "termination_reason": report.get("termination_reason"),
        "incident_category": report.get("incident_category"),
        "incident_subtype": report.get("incident_subtype"),
        "selected_tools": report.get("selected_tools", []),
        "tool_call_count": len(calls),
        "tool_statuses": [
            {
                "tool_name": call.get("tool_name"),
                "status": call.get("status"),
            }
            for call in calls
            if isinstance(call, Mapping)
        ],
        "rag_status": rag.get("status"),
        "retrieved_count": rag.get("retrieved_count"),
        "citation_count": len(rag.get("citations", []))
        if isinstance(rag.get("citations"), list)
        else 0,
        "grounding_status": grounding.get("status"),
        "read_only": policy.get("read_only"),
        "automatic_remediation": policy.get("automatic_remediation"),
        "durable_persistence": policy.get("durable_persistence"),
    }
