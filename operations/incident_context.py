
"""
Read-only incident context interface.

Reuses the existing investigation workflow.
Does not create persistent incident records or perform recovery actions.
"""

from typing import Any, Dict, Optional

from .investigation import investigate_job_run


ALLOWED_STATUSES = {
    "OK",
    "PARTIAL",
    "NO_MATCH",
    "ERROR",
}


def get_incident_context(
    workspace_client: Any,
    spark: Any,
    *,
    run_id: Optional[int] = None,
    job_id: Optional[int] = None,
    job_name: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    status_filter: Optional[str] = None,
    lookback_days: Optional[int] = None,
    event_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Retrieve incident context from an existing Job investigation.

    Uses the Job run ID as the investigation reference.
    No separate incident registry or persistent incident ID is created.
    """

    if workspace_client is None:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_incident_context",
            "error": "workspace_client is required.",
        }

    if spark is None:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "get_incident_context",
            "error": "spark is required.",
        }

    optional_arguments = {
        "run_id": run_id,
        "job_id": job_id,
        "job_name": job_name,
        "pipeline_id": pipeline_id,
        "status_filter": status_filter,
        "lookback_days": lookback_days,
        "event_limit": event_limit,
    }

    kwargs = {
        name: value
        for name, value in optional_arguments.items()
        if value is not None
    }

    try:
        investigation = investigate_job_run(
            workspace_client,
            spark,
            **kwargs,
        )

        if not isinstance(investigation, dict):
            raise ValueError(
                "Investigation returned a non-dictionary response."
            )

        investigation_status = investigation.get("status")

        if investigation_status not in ALLOWED_STATUSES:
            raise ValueError(
                "Investigation returned an unsupported status."
            )

        pipeline_investigations = (
            investigation.get("pipeline_investigations") or []
        )

        root_cause_evidence = (
            investigation.get("root_cause_evidence") or []
        )

        missing_evidence = (
            investigation.get("missing_evidence") or []
        )

        limitations = investigation.get("limitations") or []
        failed_tasks = investigation.get("failed_tasks") or []

        if not isinstance(pipeline_investigations, list):
            raise ValueError(
                "pipeline_investigations must be a list."
            )

        if not isinstance(root_cause_evidence, list):
            raise ValueError(
                "root_cause_evidence must be a list."
            )

        if not isinstance(missing_evidence, list):
            raise ValueError("missing_evidence must be a list.")

        if not isinstance(limitations, list):
            raise ValueError("limitations must be a list.")

        if not isinstance(failed_tasks, list):
            raise ValueError("failed_tasks must be a list.")

        if not all(isinstance(item, dict) for item in root_cause_evidence):
            raise ValueError(
                "Malformed root-cause evidence returned by investigation."
            )

        if not all(isinstance(item, dict) for item in failed_tasks):
            raise ValueError(
                "Malformed failed-task evidence returned by investigation."
            )

        failed_task_references = []
        pipeline_update_ids = set()
        pipeline_event_references = []

        for task in failed_tasks:
            failed_task_references.append({
                "task_key": task.get("task_key"),
                "task_run_id": task.get("run_id"),
                "result_state": task.get("result_state"),
            })

        for item in pipeline_investigations:

            if not isinstance(item, dict):
                raise ValueError(
                    "Malformed pipeline investigation response."
                )

            task_run_id = item.get("task_run_id")

            for update_id in item.get("matched_update_ids") or []:
                if update_id is not None:
                    pipeline_update_ids.add(str(update_id))

            events = item.get("events") or []

            if not isinstance(events, list):
                raise ValueError(
                    "Pipeline events must be returned as a list."
                )

            for event in events:

                if not isinstance(event, dict):
                    raise ValueError(
                        "Malformed pipeline event returned."
                    )

                pipeline_event_references.append({
                    "task_run_id": task_run_id,
                    "event_id": event.get("event_id") or event.get("id"),
                    "pipeline_update_id": event.get("update_id"),
                    "event_timestamp": event.get("timestamp"),
                    "event_type": event.get("event_type"),
                    "level": event.get("level"),
                })

        return {
            "status": investigation_status,
            "tool_name": "get_incident_context",

            "incident_reference": {
                "job_run_id": investigation.get("selected_run_id"),
                "persistent_incident_record": False,
            },

            "job": investigation.get("job"),
            "pipeline": investigation.get("pipeline"),

            "evidence_references": {
                "failed_tasks": failed_task_references,
                "pipeline_update_ids": sorted(pipeline_update_ids),
                "pipeline_events": pipeline_event_references,
                "root_cause_evidence_count": len(root_cause_evidence),
            },

            "root_cause_status": investigation.get(
                "root_cause_status"
            ),

            "root_cause_evidence": root_cause_evidence,
            "summary": investigation.get("summary"),
            "missing_evidence": missing_evidence,
            "limitations": limitations,

            "persistence": "NOT_PERSISTED",
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool_name": "get_incident_context",
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "persistence": "NOT_PERSISTED",
        }
