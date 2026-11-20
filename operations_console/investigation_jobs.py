# Databricks notebook source

import sys
import json

# --------------------------------------------------
# 1. Add the existing project root to Python path
# --------------------------------------------------

PROJECT_ROOT = (
    "/Workspace/Users/abdulkhadarm86@gmail.com/"
    "zomato_lakehouse_pipeline"
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --------------------------------------------------
# 2. Imports from existing project modules
# --------------------------------------------------

from databricks.sdk import WorkspaceClient

from operations.agent_tools import ZOMATO_JOB_ID

from operations.evidence_snapshots import (
    ensure_evidence_snapshot_table,
    build_evidence_persist_callback,
)

from observability.agent_tracing import (
    run_traced_registered_investigation,
)


# --------------------------------------------------
# 3. Job parameters
# --------------------------------------------------

for name, default in [
    ("target_job_run_id", ""),
    ("request_id", ""),
    ("request_source", "MANUAL"),
]:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


def positive_int(value, field_name):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be a positive integer."
        ) from exc

    if result <= 0:
        raise ValueError(
            f"{field_name} must be a positive integer."
        )

    return result


target_run_id = positive_int(
    dbutils.widgets.get("target_job_run_id"),
    "target_job_run_id",
)

request_id = dbutils.widgets.get("request_id").strip()

request_source = (
    dbutils.widgets.get("request_source").strip().upper()
)

if not request_id:
    raise ValueError("request_id cannot be empty.")

if request_id == "manual:configure-me":
    raise ValueError("Replace the placeholder request_id.")

if len(request_id) > 256:
    raise ValueError("request_id exceeds 256 characters.")

if request_source not in {
    "MANUAL",
    "JOB_FAILURE_EVENT",
}:
    raise ValueError(
        f"Unsupported request_source: {request_source}"
    )


# --------------------------------------------------
# 4. Initialize Databricks clients
# --------------------------------------------------

workspace_client = WorkspaceClient()

# --------------------------------------------------
# 5. Initialize durable evidence storage
# --------------------------------------------------

ensure_evidence_snapshot_table(spark)

job_context = {
    "job_id": ZOMATO_JOB_ID,
    "job_run_id": target_run_id,
}

evidence_callback = build_evidence_persist_callback(
    spark,
    job_context=job_context,
)

# --------------------------------------------------
# 6. Execute the existing registered investigation
# --------------------------------------------------

report = run_traced_registered_investigation(
    run_id=target_run_id,
    workspace_client=workspace_client,
    spark=spark,
    runbook_top_k=5,
    request_id=request_id,
    request_source=request_source,
    persist=True,
    evidence_persist_callback=evidence_callback,
)

# --------------------------------------------------
# 7. Validate execution and evidence persistence
# --------------------------------------------------

if not isinstance(report, dict):
    raise TypeError(
        "Investigation returned an invalid report."
    )

workflow_status = str(
    report.get("workflow_execution_status", "")
).upper()

if workflow_status != "COMPLETED":
    raise RuntimeError(
        "Investigation workflow did not complete. "
        f"Status: {workflow_status}"
    )

tool_calls = report.get("tool_calls", [])

if not isinstance(tool_calls, list):
    raise TypeError("tool_calls must be a list.")

failed_checkpoints = [
    call.get("tool_name", "UNKNOWN")
    for call in tool_calls
    if not isinstance(call, dict)
    or not isinstance(call.get("evidence_persistence"), dict)
    or call["evidence_persistence"].get("persisted") is not True
]

if failed_checkpoints:
    raise RuntimeError(
        "Evidence checkpointing failed for: "
        f"{failed_checkpoints}"
    )

# --------------------------------------------------
# 8. Return a compact Job result
# --------------------------------------------------

result = {
    "request_id": request_id,
    "target_job_run_id": target_run_id,
    "workflow_execution_status": workflow_status,
    "investigation_status": report.get("investigation_status"),
    "evidence_completeness": report.get("evidence_completeness"),
    "workflow_trace_id": report.get("trace_id"),
    "mlflow_trace_id": (
        report.get("observability") or {}
    ).get("mlflow_trace_id"),
    "tool_call_count": len(tool_calls),
    "evidence_checkpointing": "PASS",
    "report_persistence": "ENABLED",
    "automatic_remediation": False,
}

dbutils.notebook.exit(
    json.dumps(result, ensure_ascii=False)
)