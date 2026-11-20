"""MLflow tracking configuration for the Zomato Day 29 observability layer."""

from __future__ import annotations

from typing import Any

TRACKING_URI = "databricks"
EXPERIMENT_NAME = (
    "/Users/abdulkhadarm86@gmail.com/"
    "zomato_day29_agent_observability"
)


def configure_experiment() -> Any:
    """Configure Databricks tracking and return the dedicated MLflow experiment.

    This deliberately uses experiment-backed trace storage. The current
    workspace reports MLflow 3.12.0; Unity Catalog trace storage is not enabled
    by this module.
    """
    import mlflow

    mlflow.set_tracking_uri(TRACKING_URI)
    experiment = mlflow.set_experiment(experiment_name=EXPERIMENT_NAME)

    if not getattr(experiment, "experiment_id", None):
        raise RuntimeError(
            "MLflow did not return an experiment ID for the Zomato observability experiment."
        )

    return experiment
