# Databricks notebook source
# Dedicated read-only Operations Intelligence investigation task.
# Keep this Job separate from the Zomato ETL orchestration Job.

import json
import sys

PROJECT_ROOT = "/Workspace/Users/abdulkhadarm86@gmail.com/zomato_lakehouse_pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks.sdk import WorkspaceClient
from operations.investigation_workflow import run_registered_investigation

dbutils.widgets.text("target_job_run_id", "")
dbutils.widgets.text("request_id", "")
dbutils.widgets.text("request_source", "MANUAL")

target_text = dbutils.widgets.get("target_job_run_id").strip()
request_id = dbutils.widgets.get("request_id").strip()
request_source = dbutils.widgets.get("request_source").strip() or "MANUAL"

if not target_text.isdigit() or int(target_text) <= 0:
    raise ValueError("target_job_run_id must be a positive integer.")
if not request_id:
    raise ValueError("request_id is required for idempotent workflow persistence.")

target_run_id = int(target_text)
report = run_registered_investigation(
    run_id=target_run_id,
    workspace_client=WorkspaceClient(),
    spark=spark,
    request_id=request_id,
    request_source=request_source,
    persist=True,
)

if not isinstance(report, dict):
    raise TypeError("The registered investigation returned a non-dictionary report.")

# Evidence may be PARTIAL while the investigation execution itself is successful.
summary = {
    "target_job_run_id": target_run_id,
    "request_id": request_id,
    "request_source": request_source,
    "workflow_execution_status": report.get("workflow_execution_status"),
    "investigation_status": report.get("investigation_status"),
    "evidence_completeness": report.get("evidence_completeness"),
    "incident_category": report.get("incident_category"),
    "incident_subtype": report.get("incident_subtype"),
    "trace_id": report.get("trace_id"),
    "read_only": (report.get("execution_policy") or {}).get("read_only"),
    "durable_persistence": (report.get("execution_policy") or {}).get("durable_persistence"),
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
