
"""
Zomato Operations Intelligence Agent - governed tool interface.

This module is the only operational tool surface to register with the agent.

Security boundaries:
- One approved Databricks Job.
- One approved Lakeflow pipeline.
- No arbitrary Job or pipeline identifiers.
- Existing narrow DQ, schema, Delta, AI Search and lineage interfaces.
- Read-only operations only.
- Email principal redaction in agent-facing responses.
- Structured error responses.

The underlying implementation modules remain reusable and unchanged.
Direct workspace permissions must independently enforce least privilege.
"""

import re
from typing import Any, Dict, Optional


# ============================================================
# PROJECT SECURITY CONFIGURATION
# ============================================================

ZOMATO_JOB_ID = 835221340536536

ZOMATO_JOB_NAME = "zomato_lakehouse_orchestration"

ZOMATO_PIPELINE_ID = "a83e1034-e6eb-4b57-acc8-883de68cee58"


# ============================================================
# EXISTING IMPLEMENTATIONS
# ============================================================

from .jobs import (
    list_job_runs as _list_job_runs,
    get_job_run_details as _get_job_run_details,
)

from .investigation import (
    investigate_job_run as _investigate_job_run,
)

from .incident_context import (
    get_incident_context as _get_incident_context,
)

from .pipeline_events import (
    get_pipeline_events as _get_pipeline_events,
)

from .data_quality import (
    get_data_quality_results as _get_data_quality_results,
)

from .schema_inspection import (
    inspect_schema_changes as _inspect_schema_changes,
)

from .delta_history import (
    get_delta_table_history as _get_delta_table_history,
)

from .runbooks import (
    search_runbooks as _search_runbooks,
)

from .lineage import (
    get_downstream_impact as _get_downstream_impact,
)


# ============================================================
# COMMON HELPERS
# ============================================================

_EMAIL_PATTERN = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)


def _read_field(value: Any, field_name: str) -> Any:
    """Read a field from either an SDK object or dictionary."""

    if isinstance(value, dict):
        return value.get(field_name)

    return getattr(value, field_name, None)


def _positive_run_id(run_id: Any) -> bool:
    return (
        isinstance(run_id, int)
        and not isinstance(run_id, bool)
        and run_id > 0
    )


def _sanitize_payload(value: Any) -> Any:
    """Remove email principals recursively from agent-facing evidence."""

    if isinstance(value, dict):
        return {
            key: _sanitize_payload(item)
            for key, item in value.items()
            if key.lower() not in {
                "creator_user_name",
            }
        }

    if isinstance(value, list):
        return [
            _sanitize_payload(item)
            for item in value
        ]

    if isinstance(value, tuple):
        return [
            _sanitize_payload(item)
            for item in value
        ]

    if isinstance(value, str):
        return _EMAIL_PATTERN.sub(
            "[REDACTED_EMAIL]",
            value,
        )

    return value


def _tool_error(
    tool_name: str,
    exc: Exception,
) -> Dict[str, Any]:
    """Produce a normalized agent-facing error."""

    return {
        "status": (
            "TIMEOUT"
            if isinstance(exc, TimeoutError)
            else "ERROR"
        ),
        "tool_name": tool_name,
        "error_type": type(exc).__name__,
        "error": _sanitize_payload(str(exc)[:1000]),
    }


def _validate_response(
    result: Any,
    tool_name: str,
) -> Dict[str, Any]:
    """Validate the outer tool response contract."""

    if not isinstance(result, dict):
        return {
            "status": "MALFORMED_OUTPUT",
            "tool_name": tool_name,
            "error": "The underlying tool returned a non-dictionary response.",
        }

    if not isinstance(result.get("status"), str):
        return {
            "status": "MALFORMED_OUTPUT",
            "tool_name": tool_name,
            "error": "The underlying tool response has no valid status.",
        }

    return _sanitize_payload(result)


def _verify_zomato_job_run(
    workspace_client: Any,
    run_id: int,
) -> Dict[str, Any]:
    """
    Verify that a run belongs to the fixed Zomato orchestration Job.

    Only the parent Job ID is inspected during preflight.
    Run metadata is not returned when the parent Job is unapproved.
    """

    if workspace_client is None:
        return {
            "status": "INVALID_ARGUMENT",
            "error": "workspace_client is required.",
        }

    if not _positive_run_id(run_id):
        return {
            "status": "INVALID_ARGUMENT",
            "error": "run_id must be a positive integer.",
        }

    if not hasattr(workspace_client, "jobs"):
        return {
            "status": "INVALID_ARGUMENT",
            "error": "An authenticated WorkspaceClient is required.",
        }

    try:
        run = workspace_client.jobs.get_run(
            run_id=run_id
        )

        parent_job_id = _read_field(
            run,
            "job_id",
        )

        if (
            isinstance(parent_job_id, bool)
            or not isinstance(parent_job_id, int)
        ):
            return {
                "status": "SCOPE_UNVERIFIABLE",
                "error": (
                    "The Jobs API did not expose a verifiable "
                    "parent Job ID. Access was denied."
                ),
            }

        if parent_job_id != ZOMATO_JOB_ID:
            return {
                "status": "ACCESS_DENIED",
                "error": (
                    "The requested run does not belong to the "
                    "approved Zomato orchestration Job."
                ),
            }

        return {
            "status": "OK",
            "verified_job_id": ZOMATO_JOB_ID,
            "verified_run_id": run_id,
        }

    except Exception as exc:
        return _tool_error(
            "verify_zomato_job_run",
            exc,
        )


# ============================================================
# TOOL 1: LIST APPROVED ZOMATO JOB RUNS
# ============================================================

def list_job_runs(
    workspace_client: Any,
    *,
    status_filter: str = "ALL",
    limit: int = 10,
    lookback_days: int = 30,
) -> Dict[str, Any]:
    """
    List runs from the approved Zomato orchestration Job only.

    The caller cannot provide an arbitrary Job ID or Job name.
    """

    try:
        result = _list_job_runs(
            workspace_client,
            job_id=ZOMATO_JOB_ID,
            job_name=ZOMATO_JOB_NAME,
            status_filter=status_filter,
            limit=limit,
            lookback_days=lookback_days,
        )

        response = _validate_response(
            result,
            "list_job_runs",
        )

        if response.get("status") in {"OK", "PARTIAL"}:
            returned_job_id = (
                response.get("job", {}).get("job_id")
                if isinstance(response.get("job"), dict)
                else None
            )

            if returned_job_id != ZOMATO_JOB_ID:
                return {
                    "status": "MALFORMED_OUTPUT",
                    "tool_name": "list_job_runs",
                    "error": "The returned Job identity did not match the approved Job.",
                }

        return response

    except Exception as exc:
        return _tool_error("list_job_runs", exc)


# ============================================================
# TOOL 2: GET APPROVED ZOMATO JOB RUN DETAILS
# ============================================================

def get_job_run_details(
    workspace_client: Any,
    *,
    run_id: int,
) -> Dict[str, Any]:
    """
    Retrieve details only for runs belonging to the approved Zomato Job.

    Parent Job verification occurs before detailed run metadata
    is returned to the agent.
    """

    verification = _verify_zomato_job_run(
        workspace_client,
        run_id,
    )

    if verification["status"] != "OK":
        return {
            "status": verification["status"],
            "tool_name": "get_job_run_details",
            "error": verification.get("error"),
        }

    try:
        result = _get_job_run_details(
            workspace_client,
            run_id=run_id,
        )

        response = _validate_response(
            result,
            "get_job_run_details",
        )

        if response["status"] != "OK":
            return response

        job_run = response.get("job_run")

        if not isinstance(job_run, dict):
            return {
                "status": "MALFORMED_OUTPUT",
                "tool_name": "get_job_run_details",
                "error": "The Job run response is malformed.",
            }

        if job_run.get("job_id") != ZOMATO_JOB_ID:
            return {
                "status": "ACCESS_DENIED",
                "tool_name": "get_job_run_details",
                "error": "The retrieved run did not belong to the approved Job.",
            }

        return response

    except Exception as exc:
        return _tool_error("get_job_run_details", exc)


# ============================================================
# TOOL 3: GET INCIDENT CONTEXT
# ============================================================

def get_incident_context(
    workspace_client: Any,
    spark: Any,
    *,
    run_id: Optional[int] = None,
    status_filter: str = "FAILED",
    lookback_days: int = 30,
    event_limit: int = 100,
) -> Dict[str, Any]:
    """
    Retrieve incident context exclusively for the approved Job
    and approved Lakeflow pipeline.

    The agent cannot override the Job ID, Job name or pipeline ID.
    """

    if workspace_client is None or spark is None:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_incident_context",
            "error": "workspace_client and spark are required.",
        }

    if run_id is not None:

        verification = _verify_zomato_job_run(
            workspace_client,
            run_id,
        )

        if verification["status"] != "OK":
            return {
                "status": verification["status"],
                "tool_name": "get_incident_context",
                "error": verification.get("error"),
            }

    try:
        result = _get_incident_context(
            workspace_client,
            spark,
            run_id=run_id,
            job_id=ZOMATO_JOB_ID,
            job_name=ZOMATO_JOB_NAME,
            pipeline_id=ZOMATO_PIPELINE_ID,
            status_filter=status_filter,
            lookback_days=lookback_days,
            event_limit=event_limit,
        )

        response = _validate_response(
            result,
            "get_incident_context",
        )

        if response.get("status") in {
            "OK",
            "PARTIAL",
        }:
            returned_job = response.get("job") or {}
            returned_pipeline = response.get("pipeline") or {}

            if returned_job.get("job_id") != ZOMATO_JOB_ID:
                return {
                    "status": "MALFORMED_OUTPUT",
                    "tool_name": "get_incident_context",
                    "error": "Investigation returned an unexpected Job identity.",
                }

            if returned_pipeline.get("pipeline_id") != ZOMATO_PIPELINE_ID:
                return {
                    "status": "MALFORMED_OUTPUT",
                    "tool_name": "get_incident_context",
                    "error": "Investigation returned an unexpected pipeline identity.",
                }

        return response

    except Exception as exc:
        return _tool_error("get_incident_context", exc)


# ============================================================
# TOOL 4: GET DATA QUALITY RESULTS
# ============================================================

def get_data_quality_results(
    spark: Any,
    *,
    dataset: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve approved Zomato dataset-level quarantine evidence."""

    try:
        result = _get_data_quality_results(
            spark,
            dataset=dataset,
        )

        return _validate_response(
            result,
            "get_data_quality_results",
        )

    except Exception as exc:
        return _tool_error("get_data_quality_results", exc)


# ============================================================
# TOOL 5: INSPECT SCHEMA CHANGES
# ============================================================

def inspect_schema_changes(
    spark: Any,
    *,
    dataset: Optional[str] = None,
) -> Dict[str, Any]:
    """Compare approved Zomato Bronze/Silver schemas with YAML contracts."""

    try:
        result = _inspect_schema_changes(
            spark,
            dataset=dataset,
        )

        return _validate_response(
            result,
            "inspect_schema_changes",
        )

    except Exception as exc:
        return _tool_error("inspect_schema_changes", exc)


# ============================================================
# TOOL 6: GET DELTA TABLE HISTORY
# ============================================================

def get_delta_table_history(
    spark: Any,
    *,
    table_name: str,
    limit: int = 20,
) -> Dict[str, Any]:
    """Retrieve bounded history for approved Zomato Delta objects."""

    try:
        result = _get_delta_table_history(
            spark,
            table_name=table_name,
            limit=limit,
        )

        return _validate_response(
            result,
            "get_delta_table_history",
        )

    except Exception as exc:
        return _tool_error("get_delta_table_history", exc)


# ============================================================
# TOOL 7: SEARCH APPROVED RUNBOOKS
# ============================================================

def search_runbooks(
    query: str,
    *,
    top_k: int = 5,
    additional_filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Search the existing production-only AI Search retrieval path."""

    try:
        result = _search_runbooks(
            query,
            top_k=top_k,
            additional_filters=additional_filters,
        )

        return _validate_response(
            result,
            "search_runbooks",
        )

    except Exception as exc:
        return _tool_error("search_runbooks", exc)


# ============================================================
# TOOL 8: CONDITIONAL DOWNSTREAM IMPACT
# ============================================================

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
    Inspect actual Unity Catalog lineage for approved Zomato tables.

    Historical lineage must not be described as incident-specific
    unless an exact pipeline update ID is provided and matched.
    """

    try:
        result = _get_downstream_impact(
            spark,
            table_name=table_name,
            pipeline_update_id=pipeline_update_id,
            max_depth=max_depth,
            lookback_days=lookback_days,
            max_results=max_results,
        )

        return _validate_response(
            result,
            "get_downstream_impact",
        )

    except Exception as exc:
        return _tool_error("get_downstream_impact", exc)


# ============================================================
# AGENT TOOL ALLOWLIST
# ============================================================

AGENT_TOOL_FUNCTIONS = {
    "list_job_runs": list_job_runs,
    "get_job_run_details": get_job_run_details,
    "get_incident_context": get_incident_context,
    "get_data_quality_results": get_data_quality_results,
    "inspect_schema_changes": inspect_schema_changes,
    "get_delta_table_history": get_delta_table_history,
    "search_runbooks": search_runbooks,
    "get_downstream_impact": get_downstream_impact,
}


def get_agent_tool_registry() -> Dict[str, Any]:
    """
    Return the only approved callable registry for agent integration.

    Do not register raw functions from jobs.py, investigation.py,
    pipeline_events.py or other internal modules directly.
    """

    return dict(AGENT_TOOL_FUNCTIONS)


print("Zomato agent tool boundary loaded.")
