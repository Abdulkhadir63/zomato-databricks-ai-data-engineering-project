"""Small human-approval workflow for proposed Zomato recovery actions.

This module records an approval decision. It does not execute recovery, issue
SQL, modify pipelines, change schemas, reset checkpoints, or write business
Delta tables. The agent has no recovery executor in its governed tool registry.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Mapping, Optional

from .persistence import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    APPROVAL_REQUESTS_TABLE,
    _fetch_by_key,
    _merge_insert_if_absent,
    _qualified_table,
    ensure_operational_tables,
    normalize_request_id,
    utc_now,
)


ALLOWED_ACTION_TYPES = {
    "CODE_CHANGE",
    "SCHEMA_CHANGE",
    "DATA_REPLAY",
    "DATA_REPAIR",
    "CHECKPOINT_RESET",
    "PIPELINE_CONFIGURATION_CHANGE",
    "OTHER_RECOVERY",
}
ALLOWED_DECISIONS = {"APPROVED", "REJECTED"}
_APPROVAL_ID_PATTERN = re.compile(r"^appr_[0-9a-f]{32}$")


class ApprovalError(RuntimeError):
    """Base approval workflow error."""


class InvalidApprovalTransition(ApprovalError):
    """Raised when an approval decision cannot transition from its state."""


class ApprovalActionMismatch(ApprovalError):
    """Raised when an approval is requested for different action details."""


def action_fingerprint(
    action_type: str,
    recommended_action: str,
    action_details: Mapping[str, Any],
) -> str:
    """Hash the exact action proposal so approval cannot be reused for edits."""
    normalized_type = str(action_type).strip().upper()
    if normalized_type not in ALLOWED_ACTION_TYPES:
        raise ValueError(f"Unsupported action_type: {action_type!r}.")
    if not isinstance(recommended_action, str) or not recommended_action.strip():
        raise ValueError("recommended_action must be a non-empty string.")
    if not isinstance(action_details, Mapping):
        raise ValueError("action_details must be a mapping.")
    canonical = json.dumps(
        {
            "action_type": normalized_type,
            "recommended_action": recommended_action.strip(),
            "action_details": dict(action_details),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_approval_id(
    incident_id: str,
    action_type: str,
    action_fingerprint_value: str,
) -> str:
    """Return a deterministic ID for the same incident/action proposal."""
    if not isinstance(incident_id, str) or not incident_id.strip():
        raise ValueError("incident_id must be a non-empty string.")
    normalized_type = action_type.strip().upper()
    if normalized_type not in ALLOWED_ACTION_TYPES:
        raise ValueError(f"Unsupported action_type: {action_type!r}.")
    if not isinstance(action_fingerprint_value, str) or not re.fullmatch(r"[0-9a-f]{64}", action_fingerprint_value):
        raise ValueError("action_fingerprint_value must be a SHA-256 hex digest.")
    material = "\0".join(
        [incident_id.strip(), normalized_type, action_fingerprint_value]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return f"appr_{digest}"


def create_approval_request(
    spark: Any,
    *,
    incident_id: str,
    run_id: int,
    request_id: str,
    action_type: str,
    recommended_action: str,
    action_details: Mapping[str, Any],
    requested_by: str,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Create or return the same pending approval request idempotently."""
    request_id = normalize_request_id(request_id)
    if not isinstance(incident_id, str) or not incident_id.strip():
        raise ValueError("incident_id must be a non-empty string.")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer.")
    if not isinstance(requested_by, str) or not requested_by.strip():
        raise ValueError("requested_by must identify the requester.")

    normalized_type = str(action_type).strip().upper()
    fingerprint = action_fingerprint(
        normalized_type, recommended_action, action_details
    )
    approval_id = build_approval_id(incident_id, normalized_type, fingerprint)
    details_json = json.dumps(
        dict(action_details),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )

    ensure_operational_tables(spark, catalog=catalog, schema=schema)
    table = _qualified_table(APPROVAL_REQUESTS_TABLE, catalog, schema)
    existing = _fetch_by_key(
        spark, table=table, key_column="approval_id", key_value=approval_id
    )
    if existing:
        if existing.get("action_fingerprint") != fingerprint:
            raise ApprovalActionMismatch("Existing approval ID has different action details.")
        return {"created": False, "approval": existing}

    now = utc_now()
    row = {
        "approval_id": approval_id,
        "incident_id": incident_id.strip(),
        "run_id": int(run_id),
        "request_id": request_id,
        "action_type": normalized_type,
        "recommended_action": recommended_action.strip(),
        "action_details_json": details_json,
        "action_fingerprint": fingerprint,
        "approval_status": "PENDING",
        "requested_by": requested_by.strip(),
        "requested_at": now,
        "reviewed_by": None,
        "reviewed_at": None,
        "review_comment": None,
        "execution_status": "NOT_EXECUTED",
        "execution_reference": None,
        "created_at": now,
        "updated_at": now,
    }
    _merge_insert_if_absent(
        spark, table=table, row=row, key_column="approval_id"
    )
    saved = _fetch_by_key(
        spark, table=table, key_column="approval_id", key_value=approval_id
    )
    if not saved:
        raise ApprovalError("Approval request could not be read after MERGE.")
    return {"created": saved.get("request_id") == request_id, "approval": saved}



def create_approval_request_from_report(
    spark: Any,
    *,
    report: Mapping[str, Any],
    action_type: str,
    recommended_action: str,
    action_details: Mapping[str, Any],
    requested_by: str,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Create an approval request tied to the exact incident/report identity."""
    incident_id = report.get("incident_id")
    run_id = report.get("run_id")
    request_id = report.get("request_id")
    if (
        not isinstance(incident_id, str)
        or isinstance(run_id, bool)
        or not isinstance(run_id, int)
    ):
        raise ValueError("report must contain incident_id and integer run_id.")
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError("report must contain request_id before approval can be requested.")
    return create_approval_request(
        spark,
        incident_id=incident_id,
        run_id=run_id,
        request_id=request_id,
        action_type=action_type,
        recommended_action=recommended_action,
        action_details=action_details,
        requested_by=requested_by,
        catalog=catalog,
        schema=schema,
    )

def get_approval_request(
    spark: Any,
    approval_id: str,
    *,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Optional[Dict[str, Any]]:
    if not isinstance(approval_id, str) or not _APPROVAL_ID_PATTERN.fullmatch(approval_id):
        raise ValueError("approval_id has an invalid format.")
    table = _qualified_table(APPROVAL_REQUESTS_TABLE, catalog, schema)
    return _fetch_by_key(
        spark, table=table, key_column="approval_id", key_value=approval_id
    )


def record_approval_decision(
    spark: Any,
    *,
    approval_id: str,
    decision: str,
    reviewed_by: str,
    review_comment: Optional[str] = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Record a human decision. PENDING can transition once to APPROVED/REJECTED."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    normalized_decision = str(decision).strip().upper()
    if normalized_decision not in ALLOWED_DECISIONS:
        raise ValueError("decision must be APPROVED or REJECTED.")
    if not isinstance(reviewed_by, str) or not reviewed_by.strip():
        raise ValueError("reviewed_by must identify the human reviewer.")
    if review_comment is not None and not isinstance(review_comment, str):
        raise ValueError("review_comment must be a string or None.")

    existing = get_approval_request(
        spark, approval_id, catalog=catalog, schema=schema
    )
    if not existing:
        raise ApprovalError(f"Approval request {approval_id!r} does not exist.")

    current = str(existing.get("approval_status", "")).upper()
    if current == normalized_decision:
        return {"updated": False, "approval": existing, "idempotent_repeat": True}
    if current != "PENDING":
        raise InvalidApprovalTransition(
            f"Cannot change approval from {current} to {normalized_decision}. "
            "Create a new action proposal if the recommended action changes."
        )

    now = utc_now()
    table = _qualified_table(APPROVAL_REQUESTS_TABLE, catalog, schema)
    DeltaTable.forName(spark, table).update(
        condition=(
            (F.col("approval_id") == approval_id)
            & (F.col("approval_status") == "PENDING")
        ),
        set={
            "approval_status": F.lit(normalized_decision),
            "reviewed_by": F.lit(reviewed_by.strip()),
            "reviewed_at": F.lit(now).cast("timestamp"),
            "review_comment": F.lit(review_comment),
            "updated_at": F.lit(now).cast("timestamp"),
        },
    )
    saved = get_approval_request(
        spark, approval_id, catalog=catalog, schema=schema
    )
    if not saved:
        raise ApprovalError("Approval record disappeared after decision update.")
    if saved.get("approval_status") != normalized_decision:
        raise InvalidApprovalTransition(
            "Approval state changed concurrently. Reload the request before proceeding."
        )
    return {"updated": True, "approval": saved, "idempotent_repeat": False}


def check_action_approval(
    spark: Any,
    *,
    approval_id: str,
    action_type: str,
    recommended_action: str,
    action_details: Mapping[str, Any],
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Check approval against the exact action; this function never executes it."""
    record = get_approval_request(
        spark, approval_id, catalog=catalog, schema=schema
    )
    if not record:
        return {"approval_valid": False, "agent_can_execute": False, "reason": "APPROVAL_NOT_FOUND"}

    expected_fingerprint = action_fingerprint(
        action_type, recommended_action, action_details
    )
    if record.get("action_fingerprint") != expected_fingerprint:
        return {"approval_valid": False, "agent_can_execute": False, "reason": "ACTION_DOES_NOT_MATCH_APPROVED_PROPOSAL"}
    if record.get("approval_status") != "APPROVED":
        return {
            "approval_valid": False,
            "agent_can_execute": False,
            "reason": f"APPROVAL_STATUS_{record.get('approval_status', 'UNKNOWN')}",
        }
    return {
        "approval_valid": True,
        "permitted_for_human_execution": True,
        "agent_can_execute": False,
        "reason": "HUMAN_APPROVAL_RECORDED_FOR_EXACT_ACTION",
        "execution_status": "NOT_EXECUTED",
        "approval_id": approval_id,
    }
