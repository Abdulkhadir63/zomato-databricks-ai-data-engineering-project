"""Read-only Lakeflow Jobs inspection tools.

This module exposes bounded, structured access to the Zomato orchestration job.
It does not start, repair, cancel, delete, or modify Databricks jobs or runs.

The Databricks SDK client is injected by the caller. This keeps the module
independently testable and avoids creating credentials or clients at import time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
import re


DEFAULT_ZOMATO_JOB_NAME = "zomato_lakehouse_orchestration"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_RESULT_LIMIT = 10
MAX_RESULT_LIMIT = 20
MAX_SCAN_LIMIT = 500
SDK_PAGE_LIMIT = 20

_FAILURE_STATES = {"FAILED", "TIMEDOUT", "INTERNAL_ERROR", "UPSTREAM_FAILED"}
_FAILURE_TERMINATION_CODES = {
    "INTERNAL_ERROR",
    "DRIVER_ERROR",
    "CLUSTER_ERROR",
    "REPOSITORY_CHECKOUT_FAILED",
    "INVALID_CLUSTER_REQUEST",
    "WORKSPACE_RUN_LIMIT_EXCEEDED",
    "FEATURE_DISABLED",
    "CLUSTER_REQUEST_LIMIT_EXCEEDED",
    "STORAGE_ACCESS_ERROR",
    "RUN_EXECUTION_ERROR",
    "UNAUTHORIZED_ERROR",
    "LIBRARY_INSTALLATION_ERROR",
    "MAX_CONCURRENT_RUNS_EXCEEDED",
    "MAX_SPARK_CONTEXTS_EXCEEDED",
    "RESOURCE_NOT_FOUND",
    "INVALID_RUN_CONFIGURATION",
    "CLOUD_FAILURE",
    "MAX_JOB_QUEUE_SIZE_EXCEEDED",
    "BREAKING_CHANGE",
    "CLUSTER_TERMINATED_BY_USER",
    "BUDGET_POLICY_LIMIT_EXCEEDED",
}
_TERMINATION_RESULT_MAP = {
    "SUCCESS": "SUCCESS",
    "SUCCESS_WITH_FAILURES": "SUCCESS_WITH_FAILURES",
    "RUN_EXECUTION_ERROR": "FAILED",
    "INTERNAL_ERROR": "FAILED",
    "DRIVER_ERROR": "FAILED",
    "CLUSTER_ERROR": "FAILED",
    "REPOSITORY_CHECKOUT_FAILED": "FAILED",
    "INVALID_CLUSTER_REQUEST": "FAILED",
    "WORKSPACE_RUN_LIMIT_EXCEEDED": "FAILED",
    "FEATURE_DISABLED": "FAILED",
    "CLUSTER_REQUEST_LIMIT_EXCEEDED": "FAILED",
    "STORAGE_ACCESS_ERROR": "FAILED",
    "UNAUTHORIZED_ERROR": "FAILED",
    "LIBRARY_INSTALLATION_ERROR": "FAILED",
    "MAX_CONCURRENT_RUNS_EXCEEDED": "FAILED",
    "MAX_SPARK_CONTEXTS_EXCEEDED": "FAILED",
    "RESOURCE_NOT_FOUND": "FAILED",
    "INVALID_RUN_CONFIGURATION": "FAILED",
    "CLOUD_FAILURE": "FAILED",
    "MAX_JOB_QUEUE_SIZE_EXCEEDED": "FAILED",
    "BREAKING_CHANGE": "FAILED",
    "CLUSTER_TERMINATED_BY_USER": "FAILED",
    "BUDGET_POLICY_LIMIT_EXCEEDED": "FAILED",
    "USER_CANCELED": "CANCELED",
    "CANCELED": "CANCELED",
}
_ALLOWED_STATUS_FILTERS = {"ALL", "FAILED", "SUCCESS", "ACTIVE"}
_SECRET_PATTERNS = (
    re.compile(r"(?i)(password|passwd|token|secret|api[_-]?key|client[_-]?secret)\s*([:=])\s*([^\s,;]+)"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/-]+=*"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)


def _value(obj: Any, name: str, default: Any = None) -> Any:
    """Read a field from either a dict or a Databricks SDK model object."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iso_utc(milliseconds: Any) -> Optional[str]:
    timestamp_ms = _as_int(milliseconds)
    if timestamp_ms is None or timestamp_ms <= 0:
        return None
    return datetime.fromtimestamp(
        timestamp_ms / 1000, tz=timezone.utc
    ).isoformat(timespec="seconds")


def _redact_sensitive_text(value: Any, max_length: int = 2000) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    text = _SECRET_PATTERNS[0].sub(r"\1\2[REDACTED]", text)
    text = _SECRET_PATTERNS[1].sub("Bearer [REDACTED]", text)
    text = _SECRET_PATTERNS[2].sub("[REDACTED_AWS_ACCESS_KEY]", text)
    return text[:max_length]


def _enum_text(value: Any) -> Optional[str]:
    """Normalize strings and SDK enum values to stable uppercase names."""
    if value is None:
        return None
    enum_value = _value(value, "value", value)
    text = str(enum_value).strip()
    # Some SDK enum instances stringify as ClassName.MEMBER rather than MEMBER.
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper() or None


def _run_state(run: Any) -> Dict[str, Optional[str]]:
    """Normalize both Jobs API status shapes without discarding legacy results.

    Newer responses expose ``status.state`` and ``status.termination_details``.
    The terminal outcome can still be present in the legacy ``state`` object as
    ``result_state``. Both shapes are retained because TERMINATED alone does not
    distinguish a successful run from a failed one.
    """
    current_status = _value(run, "status")
    legacy_state = _value(run, "state")
    termination_details = _value(current_status, "termination_details")

    status_state = _enum_text(_value(current_status, "state"))
    legacy_lifecycle = _enum_text(_value(legacy_state, "life_cycle_state"))
    legacy_result = _enum_text(_value(legacy_state, "result_state"))
    current_result = _enum_text(_value(current_status, "result_state"))
    termination_code = _enum_text(_value(termination_details, "code"))

    # Newer list-runs responses can omit the deprecated legacy state object.
    # Derive a result only for termination codes with a documented outcome.
    result = (
        current_result
        or legacy_result
        or _TERMINATION_RESULT_MAP.get(termination_code)
    )

    # Prefer the richer legacy lifecycle when available (e.g. INTERNAL_ERROR),
    # while retaining the newer API state separately (e.g. TERMINATED).
    lifecycle = legacy_lifecycle or status_state
    message = (
        _value(legacy_state, "state_message")
        or _value(termination_details, "message")
    )

    return {
        "life_cycle_state": lifecycle,
        "status_state": status_state,
        "result_state": result,
        "state_message": _redact_sensitive_text(message),
        "termination_code": termination_code,
        "termination_type": _enum_text(_value(termination_details, "type")),
    }


def _normalize_run(run: Any) -> Dict[str, Any]:
    start_ms = _as_int(_value(run, "start_time"))
    end_ms = _as_int(_value(run, "end_time"))
    state = _run_state(run)

    duration_ms = None
    if start_ms is not None and end_ms is not None and end_ms >= start_ms:
        duration_ms = end_ms - start_ms

    return {
        "job_id": _as_int(_value(run, "job_id")),
        "run_id": _as_int(_value(run, "run_id")),
        "run_name": _value(run, "run_name"),
        **state,
        "start_time_utc": _iso_utc(start_ms),
        "end_time_utc": _iso_utc(end_ms),
        "duration_ms": duration_ms,
        "run_page_url": _value(run, "run_page_url"),
    }


def _resolve_job(workspace_client: Any, job_id: Optional[int], job_name: str) -> Dict[str, Any]:
    """Resolve a job by explicit ID or exact name; never guess between matches."""
    if job_id is not None:
        if isinstance(job_id, bool) or not isinstance(job_id, int) or job_id <= 0:
            raise ValueError("job_id must be a positive integer.")
        return {"job_id": job_id, "job_name": job_name}

    if not isinstance(job_name, str) or not job_name.strip():
        raise ValueError("job_name must be a non-empty string.")

    matches = list(
        workspace_client.jobs.list(name=job_name.strip(), expand_tasks=False)
    )
    exact_matches = []
    for job in matches:
        settings = _value(job, "settings")
        actual_name = _value(settings, "name") or _value(job, "name")
        if isinstance(actual_name, str) and actual_name.casefold() == job_name.strip().casefold():
            exact_matches.append(job)

    if not exact_matches:
        raise LookupError(f"No Databricks Job matched the exact name {job_name!r}.")
    if len(exact_matches) > 1:
        raise LookupError(
            f"More than one Databricks Job matched {job_name!r}; supply job_id explicitly."
        )

    resolved = exact_matches[0]
    resolved_id = _as_int(_value(resolved, "job_id"))
    if resolved_id is None:
        raise RuntimeError("The matched Job did not contain a valid job_id.")

    settings = _value(resolved, "settings")
    actual_name = _value(settings, "name") or _value(resolved, "name") or job_name
    return {"job_id": resolved_id, "job_name": actual_name}


def _error_result(exc: Exception, *, tool_name: str) -> Dict[str, Any]:
    """Return a structured error without pretending the lookup succeeded."""
    class_name = type(exc).__name__
    message = str(exc).strip() or class_name
    lowered = f"{class_name} {message}".lower()

    if "permission" in lowered or "forbidden" in lowered or "403" in lowered:
        error_code = "PERMISSION_DENIED"
    elif "timeout" in lowered or "timed out" in lowered:
        error_code = "TIMEOUT"
    elif isinstance(exc, LookupError):
        error_code = "NOT_FOUND_OR_AMBIGUOUS"
    elif isinstance(exc, (ValueError, TypeError)):
        error_code = "INVALID_ARGUMENT"
    else:
        error_code = "DATABRICKS_API_ERROR"

    return {
        "status": "ERROR",
        "tool": tool_name,
        "error_code": error_code,
        "error_type": class_name,
        "error_message": message[:1200],
        "evidence": [],
    }


def list_job_runs(
    workspace_client: Any,
    *,
    job_id: Optional[int] = None,
    job_name: str = DEFAULT_ZOMATO_JOB_NAME,
    status_filter: str = "ALL",
    limit: int = DEFAULT_RESULT_LIMIT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, Any]:
    """List recent runs for the Zomato orchestration Job.

    Args:
        workspace_client: An authenticated databricks.sdk.WorkspaceClient.
        job_id: Optional explicit Job ID. Prefer this once confirmed in the project.
        job_name: Exact Job name used only when job_id is not supplied.
        status_filter: ALL, FAILED, SUCCESS, or ACTIVE.
        limit: Maximum matching runs returned (1..20).
        lookback_days: Restrict the search window (1..365).

    Returns a structured evidence object. FAILED includes FAILED, TIMEDOUT,
    INTERNAL_ERROR, and UPSTREAM_FAILED terminal results. It does not classify
    CANCELED or SUCCESS_WITH_FAILURES as FAILED.
    """
    try:
        if workspace_client is None or not hasattr(workspace_client, "jobs"):
            raise TypeError("workspace_client must be an authenticated WorkspaceClient.")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_RESULT_LIMIT:
            raise ValueError(f"limit must be an integer between 1 and {MAX_RESULT_LIMIT}.")
        if (
            isinstance(lookback_days, bool)
            or not isinstance(lookback_days, int)
            or not 1 <= lookback_days <= 365
        ):
            raise ValueError("lookback_days must be an integer between 1 and 365.")

        normalized_filter = str(status_filter).strip().upper()
        if normalized_filter not in _ALLOWED_STATUS_FILTERS:
            raise ValueError(
                f"status_filter must be one of {sorted(_ALLOWED_STATUS_FILTERS)}."
            )

        resolved = _resolve_job(workspace_client, job_id, job_name)
        now = datetime.now(timezone.utc)
        start_time_from = int((now - timedelta(days=lookback_days)).timestamp() * 1000)

        sdk_runs: Iterable[Any] = workspace_client.jobs.list_runs(
            job_id=resolved["job_id"],
            active_only=(normalized_filter == "ACTIVE"),
            completed_only=(normalized_filter in {"FAILED", "SUCCESS"}),
            start_time_from=start_time_from,
            limit=SDK_PAGE_LIMIT,
            expand_tasks=False,
        )

        evidence: List[Dict[str, Any]] = []
        scanned = 0
        scan_limit_reached = False

        for run in sdk_runs:
            if scanned >= MAX_SCAN_LIMIT:
                scan_limit_reached = True
                break
            scanned += 1

            item = _normalize_run(run)
            result_state = item.get("result_state")
            lifecycle_state = item.get("life_cycle_state")
            termination_code = item.get("termination_code")
            is_failed = (
                result_state in _FAILURE_STATES
                or lifecycle_state == "INTERNAL_ERROR"
                or termination_code in _FAILURE_TERMINATION_CODES
            )

            if normalized_filter == "FAILED" and not is_failed:
                continue
            if normalized_filter == "SUCCESS" and result_state != "SUCCESS":
                continue
            if normalized_filter == "ACTIVE" and lifecycle_state not in {
                "QUEUED", "PENDING", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY", "BLOCKED"
            }:
                continue

            evidence.append(item)
            if len(evidence) >= limit:
                break

        return {
            "status": "OK",
            "tool": "list_job_runs",
            "job": resolved,
            "query": {
                "status_filter": normalized_filter,
                "limit": limit,
                "lookback_days": lookback_days,
                "start_time_from_utc": (now - timedelta(days=lookback_days)).isoformat(timespec="seconds"),
            },
            "scanned_runs": scanned,
            "returned_runs": len(evidence),
            "scan_limit_reached": scan_limit_reached,
            "evidence": evidence,
            "limitations": [
                "Only runs returned by the Databricks Jobs API are visible to this tool.",
                "This tool does not inspect Pipeline event details or task stack traces.",
            ],
        }
    except Exception as exc:  # structured tool boundary for the agent
        return _error_result(exc, tool_name="list_job_runs")


def get_job_run_details(
    workspace_client: Any,
    *,
    run_id: int,
) -> Dict[str, Any]:
    """Retrieve one Job run and its task-level status metadata (read-only)."""
    try:
        if workspace_client is None or not hasattr(workspace_client, "jobs"):
            raise TypeError("workspace_client must be an authenticated WorkspaceClient.")
        if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
            raise ValueError("run_id must be a positive integer.")

        run = workspace_client.jobs.get_run(run_id=run_id)
        normalized = _normalize_run(run)

        tasks: List[Dict[str, Any]] = []
        for task in (_value(run, "tasks", []) or []):
            task_state = _run_state(task)
            task_start = _as_int(_value(task, "start_time"))
            task_end = _as_int(_value(task, "end_time"))
            task_duration = None
            if task_start is not None and task_end is not None and task_end >= task_start:
                task_duration = task_end - task_start

            tasks.append(
                {
                    "task_key": _value(task, "task_key"),
                    "run_id": _as_int(_value(task, "run_id")),
                    "attempt_number": _as_int(_value(task, "attempt_number")),
                    **task_state,
                    "start_time_utc": _iso_utc(task_start),
                    "end_time_utc": _iso_utc(task_end),
                    "duration_ms": task_duration,
                    "run_page_url": _value(task, "run_page_url"),
                }
            )

        return {
            "status": "OK",
            "tool": "get_job_run_details",
            "job_run": {
                **normalized,
                "original_attempt_run_id": _as_int(_value(run, "original_attempt_run_id")),
                "run_type": _value(run, "run_type"),
                "trigger": _value(run, "trigger"),
                "creator_user_name": _value(run, "creator_user_name"),
            },
            "task_count": len(tasks),
            "tasks": tasks,
            "limitations": [
                "The Jobs API metadata may not contain the complete task stack trace or Pipeline event details.",
                "Use the Pipeline event-log tool for pipeline-specific execution evidence.",
            ],
        }
    except Exception as exc:  # structured tool boundary for the agent
        return _error_result(exc, tool_name="get_job_run_details")
