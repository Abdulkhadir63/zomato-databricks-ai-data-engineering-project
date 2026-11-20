# Databricks notebook source
import sys
import importlib
from pathlib import Path

PROJECT_ROOT = Path(
    "/Workspace/Users/abdulkhadarm86@gmail.com/zomato_lakehouse_pipeline"
)

assert PROJECT_ROOT.exists(), "Project root does not exist."

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

importlib.invalidate_caches()

print("PROJECT ROOT:", PROJECT_ROOT)
print("OPERATIONS DIRECTORY:", (PROJECT_ROOT / "operations").is_dir())

import operations.investigation_workflow

print("OPERATIONS IMPORT: PASS")

# COMMAND ----------

import inspect

from operations import investigation_workflow
from observability import agent_tracing

adapter_signature = inspect.signature(
    agent_tracing.run_traced_registered_investigation
)

workflow_signature = inspect.signature(
    investigation_workflow.run_registered_investigation
)

print(
    "Adapter supports evidence callback:",
    "evidence_persist_callback" in adapter_signature.parameters
)

print(
    "Workflow supports evidence callback:",
    "evidence_persist_callback" in workflow_signature.parameters
)

# COMMAND ----------

import uuid

print(f"manual:{uuid.uuid4().hex}")

# COMMAND ----------
