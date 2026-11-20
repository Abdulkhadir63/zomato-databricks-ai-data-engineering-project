"""Read-only Job-to-pipeline investigation workflow for the Zomato project.

This module composes the existing operational tools. It does not duplicate
Databricks API or event-log query logic and performs no operational writes.

Typical use in a Databricks notebook::

    from databricks.sdk import WorkspaceClient
    from operations.investigation import investigate_job_run

    ws = WorkspaceClient()
    result = investigate_job_run(ws, spark)

When ``run_id`` is omitted, the workflow discovers the latest run matching
``status_filter`` (FAILED by default). When ``run_id`` is supplied, it
investigates that exact run without performing discovery first.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from .jobs import (
    DEFAULT_ZOMATO_JOB_NAME,
    get_job_run_details,
    list_job_runs,
)
from .pipeline_events import get_pipeline_events


DEFAULT_ZOMATO_JOB_ID = 835221340536536
DEFAULT_ZOMATO_PIPELINE_ID = "a83e1034-e6eb-4b57-acc8-883de68cee58"
DEFAULT_STATUS_FILTER = "FAILED"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_EVENT_LIMIT = 100
MAX_TASKS_TO_CORRELATE = 20

_FAILURE_STATES = {"FAILED", "TIMEDOUT", "INTERNAL_ERROR", "UPSTREAM_FAILED"}


def _json_safe(value: Any) -> Any:
    """Normalize SDK enums, datetimes, and nested objects for tool responses."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "as_dict"):
        try:
            return _json_safe(value.as_dict())
        except Exception:
            pass
    if hasattr(value, "asDict"):
        try:
            return _json_safe(value.asDict(recursive=True))
        except Exception:
            pass
    if hasattr(value, "value"):
        try:
            return _json_safe(value.value)
        except Exception:
            pass
    return str(value)


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be a positive integer.")
    return parsed


def _run_sort_key(run: dict[str, Any]) -> str:
    """ISO-8601 UTC timestamps sort lexicographically when consistently emitted."""
    return str(run.get("start_time_utc") or "")


def _is_failed(item: dict[str, Any]) -> bool:
    result_state = str(item.get("result_state") or "").upper()
    lifecycle = str(item.get("life_cycle_state") or "").upper()
    termination_code = str(item.get("termination_code") or "").upper()
    return (
        result_state in _FAILURE_STATES
        or lifecycle == "INTERNAL_ERROR"
        or termination_code not in {"", "SUCCESS", "USER_CANCELED", "CANCELED"}
        and bool(termination_code)
    )


def _failed_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failed = []
    for task in tasks:
        result_state = str(task.get("result_state") or "").upper()
        lifecycle = str(task.get("life_cycle_state") or "").upper()
        termination_code = str(task.get("termination_code") or "").upper()
        is_failed = (
            result_state in _FAILURE_STATES
            or lifecycle == "INTERNAL_ERROR"
            or (
                bool(termination_code)
                and termination_code not in {"SUCCESS", "USER_CANCELED", "CANCELED"}
            )
        )
        if is_failed and task.get("run_id") is not None:
            failed.append(task)
    return failed


def _exception_evidence(event: dict[str, Any], task: dict[str, Any], update_id: str | None) -> list[dict[str, Any]]:
    """Attach provenance to structured exception summaries from the event tool."""
    extracted = []
    for exception in event.get("error_summary") or []:
        if not isinstance(exception, dict):
            continue
        extracted.append(
            {
                "evidence_type": "pipeline_exception",
                "task_key": task.get("task_key"),
                "task_run_id": task.get("run_id"),
                "pipeline_update_id": update_id or event.get("update_id"),
                "event_timestamp": event.get("timestamp"),
                "event_type": event.get("event_type"),
                "exception_class": exception.get("class_name"),
                "exception_message": exception.get("exception_message")
                or exception.get("message_tail"),
                "traceback_present": exception.get("traceback_present"),
                "stack_present": exception.get("stack_present"),
            }
        )
    return extracted


def investigate_job_run(
    workspace_client: Any,
    spark: Any,
    *,
    run_id: int | str | None = None,
    job_id: int = DEFAULT_ZOMATO_JOB_ID,
    job_name: str = DEFAULT_ZOMATO_JOB_NAME,
    pipeline_id: str = DEFAULT_ZOMATO_PIPELINE_ID,
    status_filter: str = DEFAULT_STATUS_FILTER,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    event_limit: int = DEFAULT_EVENT_LIMIT,
) -> dict[str, Any]:
    """Build one evidence-backed investigation for a Zomato orchestration run.

    Args:
        workspace_client: Authenticated Databricks ``WorkspaceClient``.
        spark: Active Spark session used by ``get_pipeline_events``.
        run_id: Exact Job run ID. If omitted, discover the latest run matching
            ``status_filter`` (FAILED by default).
        job_id: Known Zomato orchestration Job ID.
        job_name: Human-readable Job name included in discovery evidence.
        pipeline_id: Zomato Lakeflow pipeline ID.
        status_filter: Discovery filter: FAILED, SUCCESS, ALL, or ACTIVE.
        lookback_days: Discovery lookback when ``run_id`` is omitted.
        event_limit: Maximum events retrieved per correlated failed task (1..500).

    Returns:
        JSON-friendly dictionary with Job metadata, failed tasks, correlated
        pipeline events, root-cause evidence, missing evidence, and limitations.
        ``status`` is OK only when the selected run was inspected and every
        failed task with a run ID was correlated to readable pipeline evidence.
        Otherwise the response is PARTIAL, NO_MATCH, or ERROR.
    """
    try:
        normalized_run_id = _positive_int(run_id, "run_id") if run_id is not None else None
        normalized_job_id = _positive_int(job_id, "job_id")
        normalized_event_limit = _positive_int(event_limit, "event_limit")
        if normalized_event_limit > 500:
            raise ValueError("event_limit must not exceed 500.")
        if spark is None:
            raise ValueError("spark must be the active Databricks SparkSession.")
        if workspace_client is None:
            raise ValueError("workspace_client must be an authenticated WorkspaceClient.")

        discovery = None
        selected_run = normalized_run_id

        if selected_run is None:
            discovery = list_job_runs(
                workspace_client,
                job_id=normalized_job_id,
                job_name=job_name,
                status_filter=status_filter,
                limit=1,
                lookback_days=lookback_days,
            )
            if discovery.get("status") != "OK":
                return {
                    "status": "ERROR",
                    "tool": "investigate_job_run",
                    "phase": "job_run_discovery",
                    "discovery": _json_safe(discovery),
                    "error": discovery.get("error_message") or discovery.get("error"),
                    "evidence": [],
                    "limitations": ["Investigation stopped because Job run discovery failed."],
                }

            candidates = discovery.get("evidence") or []
            if not candidates:
                return {
                    "status": "NO_MATCH",
                    "tool": "investigate_job_run",
                    "phase": "job_run_discovery",
                    "discovery": _json_safe(discovery),
                    "selected_run_id": None,
                    "evidence": [],
                    "limitations": [
                        f"No Job runs matched status_filter={status_filter!r} within the requested lookback window."
                    ],
                }
            candidates = sorted(candidates, key=_run_sort_key, reverse=True)
            selected_run = _positive_int(candidates[0].get("run_id"), "discovered run_id")

        details = get_job_run_details(workspace_client, run_id=selected_run)
        if details.get("status") != "OK":
            return {
                "status": "ERROR",
                "tool": "investigate_job_run",
                "phase": "job_run_details",
                "selected_run_id": selected_run,
                "discovery": _json_safe(discovery),
                "job_run_details": _json_safe(details),
                "error": details.get("error_message") or details.get("error"),
                "evidence": [],
                "limitations": ["Investigation stopped because Job run details could not be retrieved."],
            }

        job_run = details.get("job_run") or {}
        tasks = details.get("tasks") or []
        failed_tasks = _failed_tasks(tasks)
        overall_failed = _is_failed(job_run)

        pipeline_investigations: list[dict[str, Any]] = []
        root_cause_evidence: list[dict[str, Any]] = []
        missing_evidence: list[str] = []

        # Parent Job result and child task results are independent facts.
        # SUCCESS_WITH_FAILURES is a valid parent result when a task failed
        # but a failure-handler task completed successfully. It must not hide
        # the failed child task or suppress Pipeline event correlation.
        if overall_failed and not failed_tasks:
            missing_evidence.append(
                "The parent Job is in a failure state, but the Jobs API did not expose "
                "a failed task with a task run ID."
            )

        if failed_tasks:
            for task in failed_tasks[:MAX_TASKS_TO_CORRELATE]:
                task_run_id = _positive_int(task["run_id"], "task.run_id")
                event_result = get_pipeline_events(
                    spark,
                    pipeline_id,
                    job_task_run_id=task_run_id,
                    limit=normalized_event_limit,
                )
                matched_ids = event_result.get("matched_update_ids") or []
                events = event_result.get("events") or []

                pipeline_investigations.append(
                    {
                        "task_key": task.get("task_key"),
                        "task_run_id": task_run_id,
                        "status": event_result.get("status"),
                        "matched_update_ids": matched_ids,
                        "event_count": event_result.get("event_count", len(events)),
                        "error_event_count": event_result.get("error_event_count", 0),
                        "events": _json_safe(events),
                        "error": event_result.get("error"),
                        "limitations": _json_safe(event_result.get("limitations") or []),
                    }
                )

                for update_id in matched_ids or [None]:
                    for event in events:
                        if str(event.get("level") or "").upper() == "ERROR" or event.get("error"):
                            root_cause_evidence.extend(_exception_evidence(event, task, update_id))
                            if not event.get("error_summary"):
                                root_cause_evidence.append(
                                    {
                                        "evidence_type": "pipeline_error_event",
                                        "task_key": task.get("task_key"),
                                        "task_run_id": task_run_id,
                                        "pipeline_update_id": update_id or event.get("update_id"),
                                        "event_timestamp": event.get("timestamp"),
                                        "event_type": event.get("event_type"),
                                        "message": event.get("message"),
                                    }
                                )

                if event_result.get("status") != "OK":
                    missing_evidence.append(
                        f"Pipeline event correlation for task {task.get('task_key')!r} / task run {task_run_id} returned {event_result.get('status')}."
                    )
                elif not matched_ids:
                    missing_evidence.append(
                        f"No pipeline update was correlated to task run {task_run_id}."
                    )
                elif not any(
                    (str(event.get("level") or "").upper() == "ERROR") or event.get("error")
                    for event in events
                ):
                    missing_evidence.append(
                        f"Pipeline update {matched_ids} was found, but no error-level event was present in the returned events."
                    )

        if len(failed_tasks) > MAX_TASKS_TO_CORRELATE:
            missing_evidence.append(
                f"Only the first {MAX_TASKS_TO_CORRELATE} failed task runs were correlated to bound tool execution."
            )

        if not failed_tasks and not overall_failed:
            investigation_status = "OK"
            root_cause_status = "NOT_REQUIRED"
            summary = (
                "The parent Job and its exposed tasks contain no failed task requiring "
                "Pipeline failure-event investigation."
            )
        elif pipeline_investigations and not missing_evidence and root_cause_evidence:
            investigation_status = "OK"
            root_cause_status = "EVIDENCE_FOUND"
            summary = (
                "Failed task(s) were correlated to Pipeline event evidence containing "
                "diagnostic exception details. Parent Job result is reported separately."
            )
        else:
            investigation_status = "PARTIAL"
            root_cause_status = (
                "EVIDENCE_FOUND" if root_cause_evidence else "EVIDENCE_INCOMPLETE"
            )
            summary = (
                "A failed task or parent Job failure was identified, but Pipeline "
                "event correlation or diagnostic evidence is incomplete."
            )

        return _json_safe(
            {
                "status": investigation_status,
                "tool": "investigate_job_run",
                "selected_run_id": selected_run,
                "job": {
                    "job_id": normalized_job_id,
                    "job_name": job_name,
                },
                "pipeline": {"pipeline_id": pipeline_id},
                "discovery": discovery,
                "job_run": job_run,
                "parent_job_result_state": str(job_run.get("result_state") or "").upper() or None,
                "parent_job_life_cycle_state": str(job_run.get("life_cycle_state") or "").upper() or None,
                "parent_job_failed": overall_failed,
                "failed_task_count": len(failed_tasks),
                "task_count": details.get("task_count", len(tasks)),
                "failed_tasks": failed_tasks,
                "pipeline_investigations": pipeline_investigations,
                "root_cause_status": root_cause_status,
                "root_cause_evidence": root_cause_evidence,
                "summary": summary,
                "missing_evidence": missing_evidence,
                "limitations": [
                    "The workflow combines read-only Jobs API metadata and the pipeline event log; it does not inspect driver or executor log files outside the event payload.",
                    "An exception message is reported as evidence, not treated as an automatically verified causal explanation beyond what that message states.",
                    "Pipeline update correlation is attempted for failed task run IDs only.",
                    "No retry, restart, cancellation, data mutation, schema change, permission change, or checkpoint operation is performed.",
                ],
            }
        )

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool": "investigate_job_run",
            "error_type": type(exc).__name__,
            "error": str(exc)[:2_000],
            "evidence": [],
            "limitations": ["No operational state or data was modified by this workflow."],
        }


__all__ = [
    "DEFAULT_ZOMATO_JOB_ID",
    "DEFAULT_ZOMATO_PIPELINE_ID",
    "investigate_job_run",
]
