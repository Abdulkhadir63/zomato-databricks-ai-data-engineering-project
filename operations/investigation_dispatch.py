"""Shared submitter for the single Zomato investigation execution Job.

Operator-triggered and future event-triggered requests must submit this same
Lakeflow Job. The Job's task calls the existing run_registered_investigation()
entrypoint. This module does not implement incident investigation logic.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from typing import Any

_ALLOWED_REQUEST_SOURCES = {"MANUAL", "JOB_FAILURE_EVENT"}


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def new_manual_request_id() -> str:
    """Create a unique request identity for one operator-initiated attempt."""
    return f"manual:{uuid.uuid4().hex}"


def event_request_id_for_run(run_id: int) -> str:
    """Stable request identity reserved for the future event-triggered stage."""
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer.")
    return f"job-failure:{run_id}"


def jobs_api_idempotency_token(request_id: str) -> str:
    """Return a deterministic 64-character token for Jobs API deduplication."""
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError("request_id must be a non-empty string.")
    return hashlib.sha256(
        f"zomato-ops-investigation:{request_id.strip()}".encode("utf-8")
    ).hexdigest()


def _read_run_id(response: Any) -> int:
    run_id = _field(response, "run_id")
    if run_id is None:
        nested = _field(response, "response")
        run_id = _field(nested, "run_id") if nested is not None else None
    if isinstance(run_id, bool):
        raise RuntimeError("Databricks Jobs API returned an invalid run ID.")
    try:
        parsed = int(run_id)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Databricks Jobs API did not return a readable investigation Job run ID."
        ) from exc
    if parsed <= 0:
        raise RuntimeError("Databricks Jobs API returned a non-positive run ID.")
    return parsed


def submit_investigation_job(
    workspace_client: Any,
    *,
    investigation_job_id: int,
    target_job_run_id: int,
    request_id: str,
    request_source: str = "MANUAL",
) -> int:
    """Submit the common investigation Job and return its Databricks run ID.

    The App and a future event handler call this same function. Both send the
    same parameter contract to the same job task. Backend idempotency remains
    enforced by run_registered_investigation() using request_id.
    """
    for value, name in (
        (investigation_job_id, "investigation_job_id"),
        (target_job_run_id, "target_job_run_id"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer.")

    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError("request_id must be a non-empty string.")

    normalized_source = str(request_source).strip().upper()
    if normalized_source not in _ALLOWED_REQUEST_SOURCES:
        raise ValueError(
            f"request_source must be one of {sorted(_ALLOWED_REQUEST_SOURCES)}."
        )

    if workspace_client is None or not hasattr(workspace_client, "jobs"):
        raise ValueError("An authenticated Databricks WorkspaceClient is required.")

    response = workspace_client.jobs.run_now(
        job_id=investigation_job_id,
        job_parameters={
            "run_id": str(target_job_run_id),
            "request_id": request_id.strip(),
            "request_source": normalized_source,
        },
        idempotency_token=jobs_api_idempotency_token(request_id),
    )
    return _read_run_id(response)
