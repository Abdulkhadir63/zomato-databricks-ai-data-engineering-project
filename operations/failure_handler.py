"""Lakeflow Jobs failure-handler entry point for the Zomato agent.

This module is called by a thin Databricks notebook task. It validates the
upstream task context, initializes evidence storage, and runs the existing
registered read-only investigation with durable evidence and report
persistence enabled.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional


DEFAULT_CATALOG = "zomato_dev"
DEFAULT_SCHEMA = "ops_evidence"
REQUEST_SOURCE = "LAKEFLOW_FAILURE_HANDLER"

# Lakeflow task result states that represent a failure or cancellation path.
# The Jobs Run-if condition should still be the primary gate.
HANDLED_RESULT_STATES = {
    "FAILED",
    "CANCELED",
    "CANCELLED",
    "TIMEDOUT",
    "EVICTED",
    "UPSTREAM_FAILED",
    "UPSTREAM_CANCELED",
    "UPSTREAM_EVICTED",
}


def _required_text(parameters: Mapping[str, Any], key: str) -> str:
    value = parameters.get(key)
    if value is None:
        raise ValueError(f"Missing required failure-handler parameter: {key}")
    value = str(value).strip()
    if not value:
        raise ValueError(f"Failure-handler parameter {key!r} cannot be empty.")
    return value


def _positive_int(parameters: Mapping[str, Any], key: str) -> int:
    raw = _required_text(parameters, key)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Failure-handler parameter {key!r} must be an integer.") from exc
    if value <= 0:
        raise ValueError(f"Failure-handler parameter {key!r} must be positive.")
    return value


def _optional_text(parameters: Mapping[str, Any], key: str) -> Optional[str]:
    value = parameters.get(key)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def make_request_id(
    *, job_id: int, job_run_id: int, failed_task_key: str, failed_task_run_id: int
) -> str:
    """Stable request identity for duplicate delivery of one failed task run."""
    safe_task_key = "".join(
        character if character.isalnum() or character in "_-" else "_"
        for character in failed_task_key
    )
    return (
        f"lakeflow-failure-{job_id}-{job_run_id}-"
        f"{safe_task_key}-{failed_task_run_id}"
    )


def run_failure_investigation(
    *,
    spark: Any,
    parameters: Mapping[str, Any],
    workspace_client: Any = None,
    runbook_top_k: int = 5,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, Any]:
    """Execute one validated failure investigation from Lakeflow parameters."""
    if not isinstance(parameters, Mapping):
        raise TypeError("parameters must be a mapping.")

    job_id = _positive_int(parameters, "job_id")
    job_run_id = _positive_int(parameters, "job_run_id")
    failed_task_key = _required_text(parameters, "failed_task_key")
    failed_task_run_id = _positive_int(parameters, "failed_task_run_id")
    result_state = _required_text(parameters, "failed_task_result_state").upper()

    if result_state not in HANDLED_RESULT_STATES:
        raise ValueError(
            "Failure handler received an unexpected upstream task state: "
            f"{result_state!r}. Expected one of {sorted(HANDLED_RESULT_STATES)}."
        )

    pipeline_id = _optional_text(parameters, "pipeline_id")
    pipeline_update_id = _optional_text(parameters, "pipeline_update_id")
    error_code = _optional_text(parameters, "failed_task_error_code")

    request_id = make_request_id(
        job_id=job_id,
        job_run_id=job_run_id,
        failed_task_key=failed_task_key,
        failed_task_run_id=failed_task_run_id,
    )

    # Imports are delayed so parameter validation can run without initializing
    # Databricks SDK, MLflow or the evidence persistence dependencies.
    from databricks.sdk import WorkspaceClient

    from observability.agent_tracing import run_traced_registered_investigation
    from operations.evidence_snapshots import (
        build_evidence_persist_callback,
        ensure_evidence_snapshot_table,
    )

    if workspace_client is None:
        workspace_client = WorkspaceClient()

    # One DDL setup call per handler invocation. The callback itself performs
    # Delta writes only; it does not run DDL for every tool result.
    ensure_evidence_snapshot_table(
        spark,
        catalog=catalog,
        schema=schema,
    )

    job_context = {
        "job_id": job_id,
        "job_run_id": job_run_id,
        "failed_task_key": failed_task_key,
        "failed_task_run_id": failed_task_run_id,
        "failed_task_result_state": result_state,
        "failed_task_error_code": error_code,
        "pipeline_id": pipeline_id,
        "pipeline_update_id": pipeline_update_id,
    }

    evidence_callback = build_evidence_persist_callback(
        spark,
        job_context=job_context,
        catalog=catalog,
        schema=schema,
    )

    report = run_traced_registered_investigation(
        run_id=job_run_id,
        workspace_client=workspace_client,
        spark=spark,
        runbook_top_k=runbook_top_k,
        request_id=request_id,
        request_source=REQUEST_SOURCE,
        persist=True,
        evidence_persist_callback=evidence_callback,
    )

    if not isinstance(report, dict):
        raise TypeError("The traced investigation returned a non-dictionary report.")

    workflow_status = str(report.get("workflow_execution_status", "")).upper()
    investigation_status = str(report.get("investigation_status", "")).upper()

    if workflow_status != "COMPLETED":
        raise RuntimeError(
            "Failure investigation did not complete successfully. "
            f"workflow_execution_status={workflow_status!r}; "
            f"investigation_status={investigation_status!r}; "
            f"request_id={request_id!r}."
        )

    tool_calls = report.get("tool_calls", [])
    if not isinstance(tool_calls, list):
        raise TypeError("Investigation report tool_calls must be a list.")

    failed_checkpoints = []
    for call in tool_calls:
        if not isinstance(call, Mapping):
            failed_checkpoints.append("UNKNOWN_NON_MAPPING_TOOL_CALL")
            continue
        checkpoint = call.get("evidence_persistence")
        if not isinstance(checkpoint, Mapping) or checkpoint.get("persisted") is not True:
            failed_checkpoints.append(str(call.get("tool_name", "UNKNOWN")))
    if failed_checkpoints:
        raise RuntimeError(
            "One or more governed tool results lack durable evidence checkpoints: "
            f"{failed_checkpoints}. request_id={request_id!r}."
        )

    observability = report.get("observability") or {}
    result = {
        "request_id": request_id,
        "incident_id": report.get("incident_id"),
        "job_id": job_id,
        "job_run_id": job_run_id,
        "failed_task_key": failed_task_key,
        "failed_task_run_id": failed_task_run_id,
        "failed_task_result_state": result_state,
        "workflow_version": report.get(
            "workflow_version", report.get("schema_version")
        ),
        "workflow_trace_id": report.get("trace_id"),
        "mlflow_trace_id": observability.get("mlflow_trace_id"),
        "workflow_execution_status": workflow_status,
        "investigation_status": investigation_status,
        "evidence_completeness": report.get("evidence_completeness"),
        "tool_call_count": len(tool_calls),
        "evidence_checkpointing": "PASS",
        "report_persistence": "ENABLED",
        "automatic_remediation": False,
    }
    return result
