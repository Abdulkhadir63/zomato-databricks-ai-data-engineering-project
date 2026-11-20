# Databricks notebook source
# MAGIC %pip install databricks-ai-search

# COMMAND ----------


import sys
import json

PROJECT_ROOT = (
    "/Workspace/Users/abdulkhadarm86@gmail.com/"
    "zomato_lakehouse_pipeline"
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# Receive parameters from the Lakeflow Job.
WIDGETS = [
    "job_id",
    "job_run_id",
    "failed_task_key",
    "failed_task_run_id",
    "failed_task_result_state",
    "failed_task_error_code",
    "pipeline_id",
    "pipeline_update_id",
]

for name in WIDGETS:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, "")


parameters = {
    name: dbutils.widgets.get(name)
    for name in WIDGETS
}


# Call the existing failure handler.
from operations.failure_handler import run_failure_investigation

result = run_failure_investigation(
    spark=spark,
    parameters=parameters,
)

print("FAILURE HANDLER COMPLETED")
print(json.dumps(result, indent=2, default=str))
