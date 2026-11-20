"""Controlled, read-only incident investigation orchestration for the Zomato project.

This module composes the existing Day 25 governed tool registry and the Day 23
RAG response generator. It deliberately does not import or call raw operational
implementations, unrestricted SQL, S3 APIs, or mutation-capable operations.

Execution is serial and read-only. Request-level idempotency is handled by the
registered entrypoint and operations.persistence. Retry and enforced timeout
behavior are intentionally outside the current project scope. The core workflow
is dependency-injectable so routing and evidence handling can be tested without
Databricks access.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .persistence import incident_id_for_run, normalize_request_id


WORKFLOW_VERSION = "1.1.5"
DEFAULT_RAG_MODEL_ENDPOINT = "system.ai.gpt-oss-120b"
DEFAULT_RUNBOOK_TOP_K = 5
DEFAULT_DELTA_HISTORY_LIMIT = 10

RUNBOOK_DOCUMENT_CONTROL_GAP = (
    "Complete the runbook Document Control metadata by assigning a named owner and reviewer "
    "and maintaining review/approval status for any future production-readiness review. "
    "This is documentation governance, not an operational evidence gap or a blocker for "
    "project-authorized retrieval. Human approval is still required before any actual change."
)

EXPECTED_AGENT_TOOLS = {
    "list_job_runs",
    "get_job_run_details",
    "get_incident_context",
    "get_data_quality_results",
    "inspect_schema_changes",
    "get_delta_table_history",
    "search_runbooks",
    "get_downstream_impact",
}

SUCCESS_STATUSES = {"OK", "OK_WITH_WARNINGS"}
USABLE_EVIDENCE_STATUSES = SUCCESS_STATUSES | {"PARTIAL"}
TERMINAL_STATUSES = {
    "ACCESS_DENIED",
    "INVALID_ARGUMENT",
    "MALFORMED_OUTPUT",
    "TIMEOUT",
    "ERROR",
    "EXECUTION_ERROR",
}

ALLOWED_DATASETS = {
    "food",
    "menu",
    "order_items",
    "orders",
    "restaurant",
    "reviews",
    "users",
}

# Do not broaden this list based on model output. It mirrors project schemas.
ALLOWED_TABLE_PATTERN = re.compile(
    r"\bzomato_dev\.(?:bronze|silver|gold|quarantine)\.[a-zA-Z_][a-zA-Z0-9_]*\b",
    re.IGNORECASE,
)
PIPELINE_UPDATE_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

SENSITIVE_KEY_PARTS = (
    "password",
    "secret",
    "token",
    "credential",
    "authorization",
    "api_key",
    "access_key",
)

_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key|authorization)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_AWS_ACCESS_KEY_PATTERN = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def _redact_text(value: str, max_length: int = 1500) -> str:
    """Redact obvious secret assignments and bound text included in reports."""
    text = _SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2[REDACTED]", value)
    text = _AWS_ACCESS_KEY_PATTERN.sub("[REDACTED_AWS_ACCESS_KEY]", text)
    if len(text) > max_length:
        return text[:max_length] + "...[TRUNCATED]"
    return text


def _safe_value(value: Any, key: str = "", depth: int = 0) -> Any:
    """Convert tool output to report-safe Python values and redact secret fields."""
    if any(part in key.lower() for part in SENSITIVE_KEY_PARTS):
        return "[REDACTED]"

    if depth > 12:
        return "[MAX_DEPTH]"

    if hasattr(value, "asDict") and callable(value.asDict):
        try:
            value = value.asDict(recursive=True)
        except Exception:
            return f"[UNSERIALIZABLE:{type(value).__name__}]"

    if isinstance(value, Mapping):
        return {
            str(k): _safe_value(v, key=str(k), depth=depth + 1)
            for k, v in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item, depth=depth + 1) for item in value]

    if isinstance(value, str):
        # Preserve complete runbook chunks and useful generated answers while
        # keeping all other tool/report strings bounded.
        key_lower = key.lower()
        if key_lower == "chunk_text":
            limit = 3500
        elif key_lower == "answer":
            limit = 8000
        else:
            limit = 1500
        return _redact_text(value, max_length=limit)

    if value is None or isinstance(value, (bool, int, float)):
        return value

    return _redact_text(str(value))


def _status(result: Mapping[str, Any]) -> str:
    value = result.get("status")
    if not isinstance(value, str) or not value.strip():
        return "MALFORMED_OUTPUT"
    return value.strip().upper()


def _tool_error(tool_name: str, exc: Exception) -> Dict[str, Any]:
    return {
        "status": "EXECUTION_ERROR",
        "tool_name": tool_name,
        "error_type": type(exc).__name__,
        "error": _redact_text(str(exc), max_length=800),
    }



def _new_trace_id() -> str:
    """Create one unique trace identity for one actual execution."""
    return f"trace-{uuid.uuid4().hex}"


def _new_manual_request_id() -> str:
    """Create an invocation-only key when no event idempotency key is supplied.

    Automatic failure-event handlers must pass a stable event-derived
    request_id. A generated manual key intentionally represents a new request.
    """
    return f"manual:{uuid.uuid4().hex}"


def _tool_call_identity(trace_id: str, sequence: int, tool_name: str) -> str:
    if not trace_id or sequence < 1 or not tool_name:
        raise ValueError("trace_id, positive sequence and tool_name are required.")
    return f"{trace_id}:tool:{sequence:02d}:{tool_name}"


def _validate_primary_evidence_identity(
    *,
    run_id: int,
    context: Mapping[str, Any],
    job_details: Mapping[str, Any],
    expected_job_id: Optional[int] = None,
    expected_pipeline_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Check that returned metadata belongs to the requested approved run.

    Missing identity fields are NOT_VERIFIABLE, not silently treated as a pass.
    Conflicting or mismatched identifiers fail the primary-evidence check.
    """
    checks: List[Dict[str, Any]] = []
    job_run = job_details.get("job_run") if isinstance(job_details.get("job_run"), Mapping) else {}
    context_job = context.get("job") if isinstance(context.get("job"), Mapping) else {}
    pipeline = context.get("pipeline") if isinstance(context.get("pipeline"), Mapping) else {}

    actual_run_id = job_run.get("run_id") or job_run.get("job_run_id")
    if actual_run_id is None:
        checks.append({"check": "job_run_id_matches_request", "status": "NOT_VERIFIABLE", "expected": run_id})
    else:
        checks.append({
            "check": "job_run_id_matches_request",
            "status": "PASS" if str(actual_run_id) == str(run_id) else "FAIL",
            "expected": run_id,
            "observed": actual_run_id,
        })

    incident_reference = context.get("incident_reference") if isinstance(context.get("incident_reference"), Mapping) else {}
    context_run_id = incident_reference.get("run_id") or incident_reference.get("job_run_id")
    if context_run_id is None:
        checks.append({"check": "incident_context_run_id_matches_request", "status": "NOT_VERIFIABLE", "expected": run_id})
    else:
        checks.append({
            "check": "incident_context_run_id_matches_request",
            "status": "PASS" if str(context_run_id) == str(run_id) else "FAIL",
            "expected": run_id,
            "observed": context_run_id,
        })

    details_job_id = job_run.get("job_id")
    context_job_id = context_job.get("job_id")
    present_job_ids = [value for value in (details_job_id, context_job_id) if value is not None]
    if len(present_job_ids) == 2 and str(present_job_ids[0]) != str(present_job_ids[1]):
        checks.append({
            "check": "job_identity_consistent_across_tools",
            "status": "FAIL",
            "observed_job_ids": present_job_ids,
        })
    elif not present_job_ids:
        checks.append({"check": "job_identity_consistent_across_tools", "status": "NOT_VERIFIABLE"})
    else:
        checks.append({
            "check": "job_identity_consistent_across_tools",
            "status": "PASS",
            "observed_job_id": present_job_ids[0],
        })

    if expected_job_id is not None:
        if not present_job_ids:
            checks.append({"check": "approved_job_id_matches", "status": "NOT_VERIFIABLE", "expected": expected_job_id})
        else:
            checks.append({
                "check": "approved_job_id_matches",
                "status": "PASS" if all(str(value) == str(expected_job_id) for value in present_job_ids) else "FAIL",
                "expected": expected_job_id,
                "observed": present_job_ids,
            })

    actual_pipeline_id = pipeline.get("pipeline_id")
    if expected_pipeline_id is not None:
        if actual_pipeline_id is None:
            checks.append({
                "check": "approved_pipeline_id_matches",
                "status": "NOT_VERIFIABLE",
                "expected": expected_pipeline_id,
            })
        else:
            checks.append({
                "check": "approved_pipeline_id_matches",
                "status": "PASS" if str(actual_pipeline_id) == str(expected_pipeline_id) else "FAIL",
                "expected": expected_pipeline_id,
                "observed": actual_pipeline_id,
            })
    elif actual_pipeline_id is not None:
        checks.append({"check": "pipeline_identity_present", "status": "PASS", "observed": actual_pipeline_id})
    else:
        checks.append({"check": "pipeline_identity_present", "status": "NOT_VERIFIABLE"})

    statuses = {item["status"] for item in checks}
    overall = "FAILED" if "FAIL" in statuses else "PARTIAL" if "NOT_VERIFIABLE" in statuses else "PASSED"
    return {
        "status": overall,
        "requested_run_id": run_id,
        "checks": checks,
        "qualification": (
            "This validates consistency of returned metadata. It does not independently audit "
            "Databricks workspace permissions or cloud IAM."
        ),
    }


def _build_evidence_scope(evidence: Mapping[str, Mapping[str, Any]], pipeline_update_id: Optional[str]) -> Dict[str, Any]:
    """Describe the evidentiary scope so metadata is not overstated."""
    scopes: Dict[str, Any] = {}

    if "get_incident_context" in evidence:
        scopes["get_incident_context"] = {
            "scope": "JOB_AND_PIPELINE_INCIDENT_CONTEXT",
            "status": _status(evidence["get_incident_context"]),
            "correlation": "REQUESTED_JOB_RUN",
        }
    if "get_job_run_details" in evidence:
        scopes["get_job_run_details"] = {
            "scope": "JOB_RUN_AND_TASK_METADATA",
            "status": _status(evidence["get_job_run_details"]),
            "correlation": "REQUESTED_JOB_RUN",
        }
    if "inspect_schema_changes" in evidence:
        result = evidence["inspect_schema_changes"]
        scopes["inspect_schema_changes"] = {
            "scope": "SCHEMA_AND_CONTRACT_METADATA",
            "status": _status(result),
            "business_rows_scanned": any(
                item.get("data_values_scanned") is True
                for item in (result.get("datasets") or {}).values()
                if isinstance(item, Mapping)
            ) if isinstance(result.get("datasets"), Mapping) else False,
            "qualification": "Schema metadata differences do not prove actual row-level NULL values or data corruption.",
        }
    if "get_data_quality_results" in evidence:
        result = evidence["get_data_quality_results"]
        correlation = result.get("run_correlation")
        scopes["get_data_quality_results"] = {
            "scope": "DATASET_LEVEL_QUALITY_AND_QUARANTINE",
            "status": _status(result),
            "correlation": correlation or "NOT_ESTABLISHED",
            "incident_correlated": str(correlation).upper() in {"CORRELATED", "RUN_CORRELATED", "PIPELINE_UPDATE_CORRELATED"},
            "qualification": "Dataset-level quality results must not be attributed to this Job run unless run correlation is explicitly established.",
        }
    if "get_delta_table_history" in evidence:
        scopes["get_delta_table_history"] = {
            "scope": "DELTA_TABLE_HISTORY",
            "status": _status(evidence["get_delta_table_history"]),
            "correlation": "TABLE_LEVEL_UNLESS_RUN_METADATA_MATCHED",
            "qualification": "A Delta commit is not attributed to this Job run without independent run/time/operation correlation.",
        }
    if "get_downstream_impact" in evidence:
        result = evidence["get_downstream_impact"]
        observed_update = result.get("pipeline_update_id")
        exact_match = bool(pipeline_update_id and observed_update and str(observed_update) == str(pipeline_update_id))
        scopes["get_downstream_impact"] = {
            "scope": "LINEAGE",
            "status": _status(result),
            "correlation": "EXACT_PIPELINE_UPDATE_MATCH" if exact_match else "HISTORICAL_OR_UNCORRELATED",
            "incident_correlated": exact_match,
            "qualification": "Lineage establishes dependency/access relationships, not data corruption or a required rebuild.",
        }
    if "search_runbooks" in evidence:
        scopes["search_runbooks"] = {
            "scope": "PROJECT_OPERATIONAL_KNOWLEDGE",
            "status": _status(evidence["search_runbooks"]),
            "correlation": "DOCUMENTATION_NOT_LIVE_EXECUTION_EVIDENCE",
            "qualification": "Project retrieval eligibility is not external production approval.",
        }
    return scopes


def _assess_answer_grounding(
    rag_result: Mapping[str, Any], retrieved_rows: Sequence[Mapping[str, Any]]
) -> Dict[str, Any]:
    """Check RAG citations against the actual rows returned by AI Search."""
    retrieved_ids = {
        str(row.get("chunk_id"))
        for row in retrieved_rows
        if row.get("chunk_id") is not None
    }
    citations = rag_result.get("citations")
    citation_ids = {
        str(item.get("chunk_id"))
        for item in citations
        if isinstance(item, Mapping) and item.get("chunk_id") is not None
    } if isinstance(citations, list) else set()
    unknown_ids = sorted(citation_ids - retrieved_ids)
    rag_status = str(rag_result.get("status", "UNKNOWN")).upper()
    answer = rag_result.get("answer")

    if unknown_ids:
        status = "INVALID_CITATION_REFERENCE"
    elif rag_status == "ANSWERED" and isinstance(answer, str) and answer.strip() and citation_ids:
        status = "GROUNDED"
    elif rag_status == "INSUFFICIENT_EVIDENCE":
        status = "SAFE_ABSTENTION"
    elif rag_status == "ANSWERED" and not citation_ids:
        status = "ANSWER_WITHOUT_VERIFIABLE_CITATIONS"
    else:
        status = "NO_GROUNDED_ANSWER"

    return {
        "status": status,
        "rag_status": rag_status,
        "retrieved_chunk_count": len(retrieved_ids),
        "citation_count": len(citation_ids),
        "unknown_chunk_ids": unknown_ids,
        "grounding_rule": "Every citation must resolve to a chunk returned by the current AI Search retrieval call.",
    }

def _execute_tool(
    *,
    registry: Mapping[str, Callable[..., Any]],
    tool_name: str,
    args: Mapping[str, Any],
    evidence: Dict[str, Dict[str, Any]],
    tool_calls: List[Dict[str, Any]],
    run_id: int,
    request_id: str,
    trace_id: str,
    evidence_persist_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Execute one approved tool and durably checkpoint its sanitized result.

    The callback runs after result normalization and identity generation, but
    before this function returns. A callback failure is deliberately allowed
    to propagate so the caller cannot continue to later tools or model work
    without the required evidence checkpoint.
    """
    tool = registry.get(tool_name)
    if not callable(tool):
        result: Dict[str, Any] = {
            "status": "MALFORMED_OUTPUT",
            "tool_name": tool_name,
            "error": "Required tool is absent from the governed registry.",
        }
    else:
        try:
            raw_result = tool(**dict(args))
            if not isinstance(raw_result, dict):
                result = {
                    "status": "MALFORMED_OUTPUT",
                    "tool_name": tool_name,
                    "error": (
                        "Tool returned a non-dictionary response: "
                        f"{type(raw_result).__name__}"
                    ),
                }
            elif not isinstance(raw_result.get("status"), str):
                result = {
                    "status": "MALFORMED_OUTPUT",
                    "tool_name": tool_name,
                    "error": "Tool response omitted a string status field.",
                    "raw_response_type": type(raw_result).__name__,
                }
            else:
                result = _safe_value(raw_result)
        except Exception as exc:  # Defensive boundary; no automatic retry in Day 26.
            result = _tool_error(tool_name, exc)

    call_number = len(tool_calls) + 1
    tool_call_id = _tool_call_identity(trace_id, call_number, tool_name)
    evidence_id = f"{trace_id}:evidence:{call_number:02d}:{tool_name}"
    result_status = _status(result)

    evidence[tool_name] = result
    tool_call_record = {
        "sequence": call_number,
        "request_id": request_id,
        "trace_id": trace_id,
        "tool_call_id": tool_call_id,
        "attempt_number": 1,
        "tool_name": tool_name,
        "status": result_status,
        "evidence_id": evidence_id,
        "arguments": _safe_value(dict(args)),
    }
    tool_calls.append(tool_call_record)

    # Day 30: checkpoint the normalized, redacted result before the next
    # investigation phase can execute. The callback receives no SparkSession,
    # WorkspaceClient, raw prompt, or raw tool arguments.
    if evidence_persist_callback is not None:
        receipt = evidence_persist_callback(
            request_id=request_id,
            incident_id=incident_id_for_run(run_id),
            run_id=run_id,
            trace_id=trace_id,
            tool_call_id=tool_call_id,
            evidence_id=evidence_id,
            tool_name=tool_name,
            evidence_result=result,
        )

        if not isinstance(receipt, Mapping) or receipt.get("persisted") is not True:
            raise RuntimeError(
                f"Durable evidence checkpoint failed for tool {tool_name!r}."
            )

        # Store only a compact persistence receipt in report metadata.
        tool_call_record["evidence_persistence"] = {
            "persisted": True,
            "idempotent_replay": bool(receipt.get("idempotent_replay", False)),
            "evidence_hash": receipt.get("evidence_hash"),
            "table": receipt.get("table"),
        }

    return result


def _walk(value: Any) -> Iterable[Any]:
    """Yield nested values from mappings and sequences."""
    if isinstance(value, Mapping):
        yield value
        for nested in value.values():
            yield from _walk(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _walk(nested)


def _collect_text(value: Any) -> str:
    """Collect only incident/job evidence text, never runbook text, for routing."""
    parts: List[str] = []
    text_keys = {
        "exception_class",
        "exception_message",
        "error_class",
        "error_code",
        "error_message",
        "state_message",
        "termination_code",
        "termination_type",
        "message",
        "task_key",
        "task_name",
        "result_state",
        "event_type",
        "source_file",
        "file_path",
        "path",
    }
    for item in _walk(value):
        if isinstance(item, Mapping):
            for key, nested in item.items():
                if str(key).lower() in text_keys and isinstance(nested, (str, int, float)):
                    parts.append(str(nested))
    return _redact_text(" | ".join(parts), max_length=8000)


def _extract_root_cause_records(context: Mapping[str, Any]) -> List[Dict[str, Any]]:
    raw = context.get("root_cause_evidence")
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        # Some adapters return a single evidence object, others a wrapper.
        for key in ("items", "records", "evidence"):
            nested = raw.get(key)
            if isinstance(nested, list):
                return [dict(item) for item in nested if isinstance(item, Mapping)]
        if raw.get("evidence_type") or raw.get("exception_message"):
            return [dict(raw)]
    return []


def _extract_failed_task(
    context: Mapping[str, Any], job_details: Mapping[str, Any]
) -> Optional[str]:
    tasks = job_details.get("tasks")
    if isinstance(tasks, list):
        for task in tasks:
            if isinstance(task, Mapping) and str(task.get("result_state", "")).upper() == "FAILED":
                value = task.get("task_key") or task.get("task_name")
                if isinstance(value, str) and value.strip():
                    return value.strip()

    references = context.get("evidence_references")
    if isinstance(references, Mapping):
        failed = references.get("failed_tasks")
        if isinstance(failed, list):
            for task in failed:
                if isinstance(task, Mapping):
                    value = task.get("task_key") or task.get("task_name")
                    if isinstance(value, str) and value.strip():
                        return value.strip()
    return None


def _extract_pipeline_update_id(context: Mapping[str, Any]) -> Optional[str]:
    references = context.get("evidence_references")
    candidates: List[str] = []
    if isinstance(references, Mapping):
        update_ids = references.get("pipeline_update_ids")
        if isinstance(update_ids, list):
            candidates.extend(str(item) for item in update_ids if item)
    for item in _walk(context.get("pipeline")):
        if isinstance(item, Mapping):
            for key in ("pipeline_update_id", "update_id"):
                value = item.get(key)
                if isinstance(value, str):
                    candidates.append(value)
    valid = sorted({match for item in candidates for match in PIPELINE_UPDATE_PATTERN.findall(item)})
    return valid[0] if len(valid) == 1 else None


def _extract_dataset(value: Any) -> Optional[str]:
    # Prefer explicit structured dataset fields.
    for item in _walk(value):
        if isinstance(item, Mapping):
            for key in ("dataset", "source_dataset", "entity"):
                candidate = item.get(key)
                if isinstance(candidate, str) and candidate.lower() in ALLOWED_DATASETS:
                    return candidate.lower()

    # Derive a dataset only from a known project raw-source path or an
    # explicitly qualified approved Zomato table name.
    text = _collect_text(value)
    match = re.search(r"raw-source/([a-z_]+)/", text, flags=re.IGNORECASE)
    if match and match.group(1).lower() in ALLOWED_DATASETS:
        return match.group(1).lower()

    table_match = re.search(
        r"\bzomato_dev\.(?:bronze|silver|gold|quarantine)\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        text,
        flags=re.IGNORECASE,
    )
    if table_match and table_match.group(1).lower() in ALLOWED_DATASETS:
        return table_match.group(1).lower()
    return None


def _extract_table_name(value: Any) -> Optional[str]:
    candidates: List[str] = []
    explicit_keys = {"table_name", "affected_table", "target_table", "source_table"}
    for item in _walk(value):
        if isinstance(item, Mapping):
            for key, candidate in item.items():
                if str(key).lower() in explicit_keys and isinstance(candidate, str):
                    candidates.extend(ALLOWED_TABLE_PATTERN.findall(candidate))
    text = _collect_text(value)
    candidates.extend(ALLOWED_TABLE_PATTERN.findall(text))
    normalized = sorted({item.lower() for item in candidates})
    return normalized[0] if len(normalized) == 1 else None


def _extract_missing_columns(text: str) -> List[str]:
    """Extract explicit missing-column names from common validator/error formats.

    Handles serialized mappings such as ``'missing_columns': ['user_id']`` as
    well as human-readable messages such as ``Missing column: user_id``.
    It only extracts identifiers adjacent to an explicit missing-column marker.
    """
    found: List[str] = []

    list_patterns = (
        r"\bmissing_columns?['\"]?\s*[:=]\s*\[([^\]]*)\]",
        r"\bmissing\s+columns?\s*[:=]\s*\[([^\]]*)\]",
    )
    for pattern in list_patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            values_text = match.group(1)
            found.extend(
                re.findall(r"[`'\"]?([A-Za-z_][A-Za-z0-9_]*)[`'\"]?", values_text)
            )

    singular_patterns = (
        r"\bmissing\s+(?:required\s+)?column\s*[:=]\s*[`'\"]?([A-Za-z_][A-Za-z0-9_]*)",
        r"\bmissing\s+required\s+column\s+[`'\"]?([A-Za-z_][A-Za-z0-9_]*)",
    )
    for pattern in singular_patterns:
        found.extend(re.findall(pattern, text, flags=re.IGNORECASE))

    return sorted({name for name in found if name})


def _normalize_gap_text(item: str) -> str:
    """Normalize Unicode dash variants and whitespace before phrase matching."""
    lowered = str(item).casefold()
    lowered = re.sub(r"[\u2010-\u2015\u2212]", "-", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _is_project_governance_gap(item: str) -> bool:
    """Identify future documentation/governance gaps that do not block project RAG use."""
    lowered = _normalize_gap_text(item)
    governance_phrases = (
        "approved production recovery procedure",
        "production-approved recovery procedure",
        "owner and reviewer approval",
        "owner/reviewer approval",
        "owner or reviewer approval",
        "owner/reviewer contact",
        "owner and reviewer contact",
        "approval information",
        "approval workflow for contract changes",
        "contract-change approval workflow",
        "formal contract-change approval workflow",
        "stakeholder notification",
        "notification and escalation",
        "escalation procedure",
        "stakeholder escalation",
        "escalation/notification process",
        "escalation process",
        "notification process",
        "externally approved",
        "external production approval",
        "formal approval status",
        "owner/reviewer signatures",
        "owner and reviewer signatures",
        "owner or reviewer signatures",
        "owner/reviewer sign-off",
        "owner and reviewer sign-off",
        "production approval status",
        "formal runbook approval",
        "runbook approval status",
    )
    return any(phrase in lowered for phrase in governance_phrases)


def _is_runbook_approval_metadata_gap(item: str) -> bool:
    """Recognize runbook owner/reviewer sign-off metadata, not change approval gates."""
    normalized = _normalize_gap_text(item)
    return (
        "owner" in normalized and "reviewer" in normalized
    ) or "runbook approval status" in normalized


def _is_recovery_readiness_gap(item: str) -> bool:
    """Identify information needed to prepare a safe replay/recovery, not to prove cause."""
    lowered = _normalize_gap_text(item)
    recovery_phrases = (
        "replay method",
        "replay procedure",
        "replay/backfill command",
        "replay command",
        "backfill procedure",
        "backfill command",
        "auto loader checkpoint",
        "checkpoint location",
        "checkpoint path",
        "post-recovery validation",
        "post recovery validation",
        "recovery validation steps",
        "success criteria",
        "controlled replay",
        "replay/backfill instructions",
        "replay instructions",
        "backfill instructions",
        "reprocessing instructions",
        "replay/backfill procedure",
        "approved replay/backfill procedure",
        "replay procedure documentation",
        "backfill procedure documentation",
        "recovery validation checklist",
        "post-recovery validation checklist",
        "approved recovery validation checklist",
        "production-approved recovery validation checklist",
    )
    return any(phrase in lowered for phrase in recovery_phrases)


def _is_bronze_identifier_gap(item: str) -> bool:
    """Identify missing-evidence claims about a Bronze table/schema identifier.

    This deliberately does not match checkpoint locations, source file headers,
    discovered schemas, or business-data validation gaps.
    """
    normalized = _normalize_gap_text(item)
    return (
        "bronze" in normalized
        and any(
            phrase in normalized
            for phrase in (
                "table identifier",
                "schema identifier",
                "table name",
                "schema name",
                "table/schema identifier",
                "table or schema identifier",
            )
        )
    )


def _build_evidence_reconciliation(
    *, dataset: Optional[str], schema_result: Mapping[str, Any]
) -> Dict[str, Any]:
    """Reconcile model-generated gap claims with structured schema-tool output.

    A returned table identifier is reported as observed tool output. It is not
    upgraded into a claim that the physical table was independently queried or
    that its business rows were validated.
    """
    schema_datasets = schema_result.get("datasets")
    schema_item: Mapping[str, Any] = {}
    if isinstance(schema_datasets, Mapping) and dataset:
        candidate_item = schema_datasets.get(dataset)
        if isinstance(candidate_item, Mapping):
            schema_item = candidate_item

    schema_status = _status(schema_result)
    reported_dataset = schema_item.get("dataset", dataset)
    dataset_matches = (
        isinstance(dataset, str)
        and bool(dataset.strip())
        and isinstance(reported_dataset, str)
        and reported_dataset.casefold() == dataset.casefold()
    )

    bronze_table_raw = schema_item.get("bronze_table")
    bronze_table = bronze_table_raw.strip() if isinstance(bronze_table_raw, str) else None
    bronze_match = (
        re.fullmatch(
            r"zomato_dev\.bronze\.([A-Za-z_][A-Za-z0-9_]*)",
            bronze_table,
            flags=re.IGNORECASE,
        )
        if bronze_table
        else None
    )
    bronze_dataset_matches = bool(
        bronze_match
        and isinstance(dataset, str)
        and bronze_match.group(1).casefold() == dataset.casefold()
    )
    bronze_returned = bool(
        schema_status in SUCCESS_STATUSES
        and dataset_matches
        and bronze_dataset_matches
    )

    checkpoint_value = None
    for key in (
        "auto_loader_checkpoint_location",
        "checkpoint_location",
        "checkpoint_path",
        "checkpoint_uri",
    ):
        value = schema_item.get(key)
        if isinstance(value, str) and value.strip():
            checkpoint_value = value.strip()
            break

    data_values_scanned = schema_item.get("data_values_scanned")
    null_values_validated = schema_item.get("null_values_validated")
    nullability_mismatches = schema_item.get("nullability_metadata_mismatches")
    if not isinstance(nullability_mismatches, list):
        nullability_mismatches = []

    return {
        "status": "PARTIAL" if bronze_returned or schema_item else "NOT_ESTABLISHED",
        "dataset": dataset,
        "schema_tool_status": schema_status,
        "bronze_table_identifier": {
            "status": "RETURNED_BY_SCHEMA_INSPECTION" if bronze_returned else "NOT_ESTABLISHED",
            "value": bronze_table if bronze_returned else None,
            "source_tool": "inspect_schema_changes",
            "qualification": (
                "The identifier was returned in the current schema-inspection result. "
                "This does not independently prove physical table existence or business-data correctness."
            ),
        },
        "auto_loader_checkpoint_location": {
            "status": "RETURNED_BY_SCHEMA_INSPECTION" if checkpoint_value else "NOT_RETURNED",
            "value": checkpoint_value,
        },
        "business_row_values": {
            "data_values_scanned": data_values_scanned is True,
            "null_values_validated": null_values_validated is True,
            "status": (
                "SCANNED_AND_NULLS_VALIDATED"
                if data_values_scanned is True and null_values_validated is True
                else "SCANNED_NOT_NULL_VALIDATED"
                if data_values_scanned is True
                else "NOT_SCANNED"
                if data_values_scanned is False
                else "NOT_ESTABLISHED"
            ),
        },
        "nullability_metadata_mismatches": nullability_mismatches,
        "qualification": (
            "Schema metadata and table identifiers must not be treated as row-level validation. "
            "Nullability metadata mismatches do not establish actual NULL business values."
        ),
    }


def _apply_evidence_reconciliation(
    *,
    rag_result: Dict[str, Any],
    missing_evidence: List[str],
    reconciliation: Mapping[str, Any],
) -> List[str]:
    """Remove only stale Bronze-identifier gap claims contradicted by tool output."""
    bronze_info = reconciliation.get("bronze_table_identifier")
    if not isinstance(bronze_info, Mapping):
        return missing_evidence
    if bronze_info.get("status") != "RETURNED_BY_SCHEMA_INSPECTION":
        return missing_evidence

    # The free-form model draft is not edited here. Its stale gap claim is
    # removed from the model gap list; the final operator-facing answer is
    # rebuilt deterministically from reconciled evidence later in the workflow.
    rag_missing = rag_result.get("missing_evidence")
    if isinstance(rag_missing, list):
        rag_result["missing_evidence"] = [
            item for item in rag_missing
            if not _is_bronze_identifier_gap(str(item))
        ]

    return [
        item for item in missing_evidence
        if not _is_bronze_identifier_gap(str(item))
    ]


def _build_documentation_reconciliation(
    *,
    retrieved_rows: Sequence[Mapping[str, Any]],
    evidence_reconciliation: Mapping[str, Any],
    runbook_lifecycle: Mapping[str, Any],
) -> Dict[str, Any]:
    """Identify stale documentation statements without changing source documents.

    A stale Bronze pending-validation statement is raised only when the current
    schema-inspection tool returned a matching dataset-specific Bronze table.
    Draft/project-eligible lifecycle metadata is recorded separately; it is not
    converted into a production-approval requirement for project retrieval.
    """
    future_documentation_gaps: List[str] = []
    stale_sources: List[Dict[str, Any]] = []

    # Missing document-control ownership is a documentation-governance item.
    # It does not block project retrieval and is not incident evidence.
    for row in retrieved_rows:
        if not isinstance(row, Mapping):
            continue
        chunk_text = row.get("chunk_text")
        section_path = str(row.get("section_path") or "")
        if not isinstance(chunk_text, str) or not chunk_text.strip():
            continue
        if "document control" not in section_path.casefold():
            continue
        normalized = _normalize_gap_text(chunk_text)
        owner_is_tbd = bool(re.search(r"(?i)\|\s*owner\s*\|\s*(?:\*\*)?tbd(?:\*\*)?\s*\|", normalized))
        reviewer_is_tbd = bool(re.search(r"(?i)\|\s*reviewer\s*\|\s*(?:\*\*)?tbd(?:\*\*)?\s*\|", normalized))
        if owner_is_tbd or reviewer_is_tbd:
            future_documentation_gaps.append(RUNBOOK_DOCUMENT_CONTROL_GAP)
            stale_sources.append(
                {
                    "document_title": row.get("document_title"),
                    "document_id": row.get("document_id"),
                    "section_path": section_path,
                    "stale_statement_type": "DOCUMENT_OWNER_REVIEWER_TBD",
                    "owner_is_tbd": owner_is_tbd,
                    "reviewer_is_tbd": reviewer_is_tbd,
                }
            )

    bronze_info = evidence_reconciliation.get("bronze_table_identifier")
    bronze_value = (
        bronze_info.get("value")
        if isinstance(bronze_info, Mapping)
        and bronze_info.get("status") == "RETURNED_BY_SCHEMA_INSPECTION"
        else None
    )
    dataset = evidence_reconciliation.get("dataset")

    if isinstance(bronze_value, str) and bronze_value.strip():
        for row in retrieved_rows:
            if not isinstance(row, Mapping):
                continue
            chunk_text = row.get("chunk_text")
            if not isinstance(chunk_text, str) or not chunk_text.strip():
                continue

            normalized = _normalize_gap_text(chunk_text)
            has_bronze_inventory_reference = (
                "bronze" in normalized
                and ("schema" in normalized or "identifier" in normalized)
            )
            has_pending_validation_marker = (
                "pending_validation" in normalized
                or "pending validation" in normalized
                or "should be rechecked" in normalized
                or "needs to be rechecked" in normalized
            )
            if not (has_bronze_inventory_reference and has_pending_validation_marker):
                continue

            document_title = str(row.get("document_title") or row.get("document_id") or "Retrieved architecture document")
            section_path = str(row.get("section_path") or "Unspecified section")
            gap = (
                f"Update the Bronze schema entry in {document_title} ({section_path}): "
                f"the source text marks the identifier as pending validation, while the "
                f"current schema-inspection result returned `{bronze_value}` for dataset "
                f"`{dataset}`. Record the newer tool evidence and retain its qualification; "
                "do not claim that this independently proves physical table existence or row-level correctness."
            )
            future_documentation_gaps.append(gap)
            stale_sources.append(
                {
                    "document_title": document_title,
                    "document_id": row.get("document_id"),
                    "section_path": section_path,
                    "stale_statement_type": "BRONZE_SCHEMA_PENDING_VALIDATION",
                    "current_observed_value": bronze_value,
                    "dataset": dataset,
                }
            )

    retrieved_count = int(runbook_lifecycle.get("retrieved_document_count") or 0)
    project_eligible_count = int(runbook_lifecycle.get("project_retrieval_eligible_count") or 0)
    production_eligible_count = int(runbook_lifecycle.get("production_retrieval_eligible_count") or 0)
    statuses = runbook_lifecycle.get("document_statuses") or []

    if retrieved_count and project_eligible_count == retrieved_count:
        lifecycle_note = (
            "Retrieved documents are eligible for this project assistant. Their draft lifecycle "
            "status is preserved, and production_retrieval_eligible=false means they must not be "
            "presented as externally approved production procedures. Draft status does not block "
            "their authorized use as project knowledge and is not an operational evidence gap."
        )
        lifecycle_status = "PROJECT_RETRIEVAL_ELIGIBLE_DRAFT_LIFECYCLE"
    elif retrieved_count and project_eligible_count > 0:
        lifecycle_note = (
            "Retrieved documents have mixed project retrieval eligibility. Preserve each document's "
            "lifecycle metadata and do not present draft or non-production-eligible content as "
            "externally approved production procedures."
        )
        lifecycle_status = "MIXED_PROJECT_RETRIEVAL_ELIGIBILITY"
    elif retrieved_count:
        lifecycle_note = (
            "Project retrieval eligibility was not established for all retrieved documents. "
            "Do not rely on them as project-authorized knowledge until eligibility is verified."
        )
        lifecycle_status = "PROJECT_RETRIEVAL_ELIGIBILITY_NOT_ESTABLISHED"
    else:
        lifecycle_note = "No runbook documents were retrieved; document lifecycle could not be assessed."
        lifecycle_status = "NO_RETRIEVED_DOCUMENTS"

    status = "STALE_DOCUMENTATION_FOUND" if future_documentation_gaps else "NO_STALE_DOCUMENTATION_FOUND"
    return {
        "status": status,
        "future_documentation_gaps": list(dict.fromkeys(future_documentation_gaps)),
        "stale_documentation_sources": stale_sources,
        "document_lifecycle": {
            "status": lifecycle_status,
            "retrieved_document_count": retrieved_count,
            "project_retrieval_eligible_count": project_eligible_count,
            "production_retrieval_eligible_count": production_eligible_count,
            "document_statuses": list(statuses),
            "lifecycle_note": lifecycle_note,
            "production_approval_established": (
                retrieved_count > 0 and production_eligible_count == retrieved_count
            ),
        },
        "qualification": (
            "Documentation maintenance findings are separate from operational missing evidence. "
            "Project retrieval eligibility permits use in this portfolio assistant; it does not "
            "establish external production approval."
        ),
    }


def _apply_documentation_reconciliation(
    *,
    rag_result: Dict[str, Any],
    documentation_reconciliation: Mapping[str, Any],
) -> None:
    """Append deterministic lifecycle and stale-document clarifications to the RAG answer."""
    answer = rag_result.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        return
    if "**documentation reconciliation:**" in answer.casefold():
        return

    additions: List[str] = []
    lifecycle = documentation_reconciliation.get("document_lifecycle")
    if isinstance(lifecycle, Mapping):
        lifecycle_note = lifecycle.get("lifecycle_note")
        if isinstance(lifecycle_note, str) and lifecycle_note.strip():
            additions.append("**Document lifecycle clarification:** " + lifecycle_note.strip())

    gaps = documentation_reconciliation.get("future_documentation_gaps")
    if isinstance(gaps, list) and gaps:
        rendered_gaps = " ".join(f"- {str(item)}" for item in gaps)
        additions.append("**Future documentation maintenance:** " + rendered_gaps)

    if not additions:
        return

    addition = "\n\n**Documentation reconciliation:**\n" + "\n\n".join(additions)
    answer_budget = max(0, 8000 - len(addition) - 20)
    rag_result["answer"] = _redact_text(
        answer[:answer_budget].rstrip() + addition,
        max_length=8000,
    )



def _has_evidence_field(value: Any, candidate_keys: Sequence[str]) -> bool:
    """Return True when structured operational evidence contains a populated field."""
    accepted = {str(key).casefold() for key in candidate_keys}
    for node in _walk(value):
        if not isinstance(node, Mapping):
            continue
        for key, nested in node.items():
            if str(key).casefold() not in accepted:
                continue
            if nested is None:
                continue
            if isinstance(nested, str) and not nested.strip():
                continue
            if isinstance(nested, (list, tuple, dict, set)) and not nested:
                continue
            return True
    return False


def _build_schema_investigation_gaps(
    *,
    category: str,
    dataset: Optional[str],
    source_file: Optional[str],
    operational_evidence: Mapping[str, Any],
    schema_result: Mapping[str, Any],
) -> List[str]:
    """Derive missing schema-investigation artifacts from actual returned evidence."""
    if category != "SCHEMA_DRIFT" or not dataset:
        return []

    gaps: List[str] = []
    schema_datasets = schema_result.get("datasets")
    schema_item = (
        schema_datasets.get(dataset)
        if isinstance(schema_datasets, Mapping)
        else None
    )
    if not isinstance(schema_item, Mapping):
        schema_item = {}

    if not _has_evidence_field(
        operational_evidence,
        ("csv_header", "source_header", "actual_header", "header_columns", "file_header"),
    ):
        file_reference = f" for source object `{source_file}`" if source_file else " for the affected source file"
        gaps.append(
            f"Actual CSV header{file_reference} was not returned by the governed evidence tools; "
            "the Job error identifies the missing field but does not independently show the file header."
        )

    contract_path = schema_item.get("contract_file")
    has_contract_content = _has_evidence_field(
        operational_evidence,
        ("contract_content", "contract_schema", "canonical_schema", "yaml_content", "yaml_text", "schema_yaml"),
    )
    if not has_contract_content:
        path_text = f" (`{contract_path}`)" if isinstance(contract_path, str) and contract_path.strip() else ""
        gaps.append(
            f"Full source-contract YAML content{path_text} for dataset `{dataset}` was not returned; "
            "the schema inspection provided contract metadata/path, not the complete contract definition."
        )

    has_silver_mapping = _has_evidence_field(
        operational_evidence,
        ("silver_mapping", "silver_mapping_yaml", "mapping_content", "mapping_yaml", "silver_schema_mapping"),
    )
    if not has_silver_mapping:
        gaps.append(
            f"The Silver YAML mapping content for dataset `{dataset}` was not returned by the governed evidence tools."
        )

    has_discovered_schema = _has_evidence_field(
        operational_evidence,
        (
            "discovered_dataframe_schema",
            "discovered_schema",
            "dataframe_schema",
            "source_dataframe_schema",
            "bronze_schema_snapshot",
            "auto_loader_schema_snapshot",
        ),
    )
    if not has_discovered_schema:
        gaps.append(
            f"The full discovered Auto Loader DataFrame schema snapshot for dataset `{dataset}` was not returned; "
            "schema counts and metadata comparisons are not a complete runtime schema snapshot."
        )

    return list(dict.fromkeys(gaps))


def _build_recovery_readiness_baseline(
    *,
    dataset: Optional[str],
    evidence_reconciliation: Mapping[str, Any],
    retrieved_rows: Sequence[Mapping[str, Any]],
) -> List[str]:
    """Identify recovery details not established by current tools or retrieved chunks."""
    if not dataset:
        return []

    gaps: List[str] = []
    checkpoint = evidence_reconciliation.get("auto_loader_checkpoint_location")
    if not isinstance(checkpoint, Mapping) or checkpoint.get("status") != "RETURNED_BY_SCHEMA_INSPECTION":
        gaps.append(
            f"Auto Loader checkpoint location for dataset `{dataset}` was not returned by the governed evidence tools."
        )

    chunk_texts = [
        str(row.get("chunk_text") or "").casefold()
        for row in retrieved_rows
        if isinstance(row, Mapping)
    ]
    replay_detail_markers = (
        "replay command",
        "backfill command",
        "replay steps",
        "replay procedure steps",
        "checkpoint reset procedure",
        "replay configuration",
    )
    has_detailed_replay = any(
        marker in text
        for text in chunk_texts
        for marker in replay_detail_markers
    )
    if not has_detailed_replay:
        gaps.append(
            f"Detailed replay/backfill steps and preconditions for affected `{dataset}` source data "
            "were not present in the retrieved project runbook chunks; INC-006 only refers to using an approved replay method."
        )

    validation_detail_markers = (
        "post-recovery validation checklist",
        "post recovery validation checklist",
        "validation acceptance criteria",
        "recovery acceptance criteria",
        "validation sign-off criteria",
    )
    has_validation_checklist = any(
        marker in text
        for text in chunk_texts
        for marker in validation_detail_markers
    )
    if not has_validation_checklist:
        gaps.append(
            f"A detailed post-recovery validation checklist and acceptance criteria for `{dataset}` were not present in the retrieved project runbook chunks."
        )

    return list(dict.fromkeys(gaps))


def _build_retrieval_citations(
    retrieved_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Create citation metadata only for chunks returned by the current retrieval call."""
    citations: List[Dict[str, Any]] = []
    seen: set = set()
    for row in retrieved_rows:
        if not isinstance(row, Mapping):
            continue
        chunk_id = row.get("chunk_id")
        if chunk_id is None or str(chunk_id) in seen:
            continue
        seen.add(str(chunk_id))
        citations.append(
            {
                "citation_number": len(citations) + 1,
                "chunk_id": str(chunk_id),
                "document_id": row.get("document_id"),
                "document_title": row.get("document_title"),
                "document_version": row.get("document_version"),
                "section_path": row.get("section_path"),
                "source_file": row.get("source_file"),
                "source_uri": row.get("source_uri"),
                "source_line_start": row.get("source_line_start"),
                "source_line_end": row.get("source_line_end"),
                "document_status": row.get("document_status"),
                "evidence_state": row.get("evidence_state"),
                "project_retrieval_eligible": row.get("project_retrieval_eligible"),
                "production_retrieval_eligible": row.get("production_retrieval_eligible"),
            }
        )
    return citations


def _build_authoritative_rag_answer(
    *,
    category: str,
    subtype: str,
    failed_task: Optional[str],
    dataset: Optional[str],
    source_file: Optional[str],
    observed_missing_columns: Sequence[str],
    error_text: str,
    retrieved_rows: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
    evidence_reconciliation: Mapping[str, Any],
    documentation_reconciliation: Mapping[str, Any],
    missing_evidence: Sequence[str],
    recovery_readiness_gaps: Sequence[str],
    future_documentation_gaps: Sequence[str],
    safe_next_steps: Sequence[str],
) -> Tuple[str, List[Dict[str, Any]]]:
    """Build the final answer from structured evidence, categories and retrieved source metadata.

    The free-form model draft is not used as the final operator-facing answer because
    its prose can contradict structured tool output or the authoritative gap ledger.
    The model status remains recorded separately by the caller.
    """
    citations = _build_retrieval_citations(retrieved_rows)
    lines: List[str] = ["### Incident assessment"]
    lines.append(f"- Incident category: `{category}`")
    lines.append(f"- Incident subtype: `{subtype}`")
    if failed_task:
        lines.append(f"- Failed task: `{failed_task}`")
    if dataset:
        lines.append(f"- Dataset: `{dataset}`")
    if source_file:
        lines.append(f"- Source object referenced by operational evidence: `{source_file}`")
    if observed_missing_columns:
        lines.append("- Missing column(s) explicitly named by the Job/Pipeline error: " + ", ".join(f"`{c}`" for c in observed_missing_columns))
    if error_text.strip():
        lines.append("- Observed error evidence: " + _redact_text(error_text.strip(), 700))

    lines.append("\n### Current operational evidence")
    bronze = evidence_reconciliation.get("bronze_table_identifier", {})
    if isinstance(bronze, Mapping) and bronze.get("status") == "RETURNED_BY_SCHEMA_INSPECTION":
        lines.append(
            f"- Bronze table identifier returned by the current schema-inspection tool: `{bronze.get('value')}`. "
            "This does not independently prove physical table existence or business-row correctness."
        )
    else:
        lines.append("- A dataset-matched Bronze table identifier was not established by the current schema inspection.")

    checkpoint = evidence_reconciliation.get("auto_loader_checkpoint_location", {})
    if isinstance(checkpoint, Mapping) and checkpoint.get("status") == "RETURNED_BY_SCHEMA_INSPECTION":
        lines.append(f"- Auto Loader checkpoint location returned by schema inspection: `{checkpoint.get('value')}`.")
    else:
        lines.append("- Auto Loader checkpoint location: not returned by the current governed evidence tools.")

    row_values = evidence_reconciliation.get("business_row_values", {})
    if isinstance(row_values, Mapping):
        lines.append(
            f"- Business-row validation status: `{row_values.get('status', 'NOT_ESTABLISHED')}`; "
            f"data_values_scanned={row_values.get('data_values_scanned')}; "
            f"null_values_validated={row_values.get('null_values_validated')}."
        )
    mismatches = evidence_reconciliation.get("nullability_metadata_mismatches") or []
    if mismatches:
        mismatch_names = [str(item.get("column")) for item in mismatches if isinstance(item, Mapping) and item.get("column")]
        if mismatch_names:
            lines.append(
                "- Nullability metadata mismatch(es): " + ", ".join(f"`{name}`" for name in mismatch_names) + ". "
                "These are metadata comparisons and do not prove actual NULL business values or explain a different missing column."
            )

    dq = evidence.get("get_data_quality_results", {})
    if isinstance(dq, Mapping):
        lines.append(
            f"- DQ/quarantine evidence status: `{_status(dq)}`; "
            f"run correlation: `{dq.get('run_correlation', 'NOT_ESTABLISHED')}`. "
            "Dataset-level evidence is not attributed to this incident without explicit run correlation."
        )

    lines.append("\n### Retrieved project knowledge")
    if citations:
        for citation in citations:
            title = citation.get("document_title") or citation.get("document_id") or "Retrieved document"
            section = citation.get("section_path") or "Unspecified section"
            chunk_id = citation["chunk_id"]
            section_lower = str(section).casefold()
            if "inc-006" in section_lower or "required column missing" in section_lower:
                description = (
                    "Directly relevant missing-required-column playbook; it provides investigation "
                    "steps and a recovery outline, but only references an approved replay method."
                )
            elif "document control" in section_lower:
                description = "Runbook lifecycle and document-control metadata."
            elif "platform inventory" in section_lower:
                description = "Architecture inventory; any Bronze pending-validation statement is reconciled against the current schema-tool result below."
            elif "logical architecture" in section_lower:
                description = "Logical source-to-Gold flow and Job/task/Pipeline boundary."
            elif "purpose and scope" in section_lower:
                description = "Runbook scope and read-only/human-approval operating principle."
            else:
                description = "Section returned by the current project AI Search retrieval."
            lines.append(f"- {title} — {section}: {description} (`chunk_id: {chunk_id}`).")
    else:
        lines.append("- No citation-bearing chunks were returned by the current retrieval call.")

    lines.append("\n### Authoritative unresolved operational evidence")
    if missing_evidence:
        lines.extend(f"- {item}" for item in missing_evidence)
    else:
        lines.append("- None identified by the current structured evidence checks.")

    lines.append("\n### Recovery-readiness gaps")
    if recovery_readiness_gaps:
        lines.extend(f"- {item}" for item in recovery_readiness_gaps)
    else:
        lines.append("- None identified from the current retrieved project knowledge and operational evidence.")

    lines.append("\n### Future documentation and governance maintenance")
    if future_documentation_gaps:
        lines.extend(f"- {item}" for item in future_documentation_gaps)
    else:
        lines.append("- None identified by the current documentation reconciliation.")

    lifecycle = documentation_reconciliation.get("document_lifecycle", {})
    if isinstance(lifecycle, Mapping):
        lifecycle_note = lifecycle.get("lifecycle_note")
        if isinstance(lifecycle_note, str) and lifecycle_note.strip():
            lines.append("\n### Document lifecycle")
            lines.append(lifecycle_note.strip())

    lines.append("\n### Safe next steps")
    if safe_next_steps:
        lines.extend(f"{index}. {item}" for index, item in enumerate(safe_next_steps, 1))
    else:
        lines.append("- Continue evidence collection before proposing a change.")

    lines.append(
        "\n### Change-control boundary\n"
        "This investigation is read-only. No code, schema, data, checkpoint, or pipeline change was executed. "
        "A human engineer must approve any proposed change before execution. Project retrieval eligibility "
        "does not establish external production approval."
    )

    answer = "\n".join(lines)
    return _redact_text(answer, max_length=8000), citations


def _gap_deduplication_key(item: str, category: str) -> str:
    """Group differently worded descriptions of the same evidence/recovery gap."""
    text = _normalize_gap_text(item)
    if category == "missing":
        if "csv header" in text or "file header" in text:
            return "csv_header"
        if "contract" in text and ("yaml" in text or ".yml" in text or ".yaml" in text):
            return "source_contract_content"
        if "silver" in text and "mapping" in text:
            return "silver_mapping_content"
        if "dataframe schema" in text or ("discovered" in text and "schema" in text):
            return "discovered_dataframe_schema"
        if "dq" in text and "quarantine" in text and ("run-correlated" in text or "run correlation" in text):
            return "run_correlated_dq_quarantine"
    elif category == "recovery":
        if "checkpoint" in text:
            return "auto_loader_checkpoint"
        if "replay" in text or "backfill" in text or "reprocessing" in text:
            return "replay_backfill_procedure"
        if "validation" in text and ("recovery" in text or "post-recovery" in text):
            return "post_recovery_validation"
    elif category == "future":
        if ("owner" in text and "reviewer" in text) or "runbook approval" in text:
            return "runbook_document_control"
        if "bronze" in text and ("pending validation" in text or "pending_validation" in text):
            return "stale_bronze_inventory"
    return text


def _deduplicate_gap_items(items: Sequence[str], category: str) -> List[str]:
    """Deduplicate semantic duplicates, preferring evidence-specific wording."""
    output: List[str] = []
    positions: Dict[str, int] = {}
    scores: Dict[str, int] = {}
    for raw in items:
        item = _redact_text(str(raw), 700).strip()
        if not item:
            continue
        key = _gap_deduplication_key(item, category)
        score = 2 if any(
            marker in _normalize_gap_text(item)
            for marker in (
                "was not returned",
                "not present in the retrieved",
                "current schema-inspection result returned",
                "current schema inspection returned",
                "retrieved source currently records",
            )
        ) else 1
        if key not in positions:
            positions[key] = len(output)
            scores[key] = score
            output.append(item)
        elif score > scores[key]:
            output[positions[key]] = item
            scores[key] = score
    return output


def _build_report_consistency(
    *,
    missing_evidence: Sequence[str],
    recovery_readiness_gaps: Sequence[str],
    future_documentation_gaps: Sequence[str],
    rag_result: Mapping[str, Any],
    evidence_reconciliation: Mapping[str, Any],
) -> Dict[str, Any]:
    """Check that narrative metadata and the three authoritative gap lists agree."""
    missing_norm = {_normalize_gap_text(item) for item in missing_evidence}
    recovery_norm = {_normalize_gap_text(item) for item in recovery_readiness_gaps}
    future_norm = {_normalize_gap_text(item) for item in future_documentation_gaps}
    overlaps = {
        "missing_vs_recovery": sorted(missing_norm & recovery_norm),
        "missing_vs_future_documentation": sorted(missing_norm & future_norm),
        "recovery_vs_future_documentation": sorted(recovery_norm & future_norm),
    }
    rag_missing = {_normalize_gap_text(item) for item in rag_result.get("missing_evidence", []) if isinstance(item, str)}
    rag_recovery = {_normalize_gap_text(item) for item in rag_result.get("recovery_readiness_gaps", []) if isinstance(item, str)}
    rag_future = {_normalize_gap_text(item) for item in rag_result.get("future_documentation_gaps", []) if isinstance(item, str)}
    category_alignment = (
        rag_missing == missing_norm
        and rag_recovery == recovery_norm
        and rag_future == future_norm
    )
    bronze = evidence_reconciliation.get("bronze_table_identifier", {})
    bronze_reconciled = not (
        isinstance(bronze, Mapping)
        and bronze.get("status") == "RETURNED_BY_SCHEMA_INSPECTION"
        and any(_is_bronze_identifier_gap(item) for item in missing_evidence)
    )
    no_cross_category_overlap = not any(overlaps.values())
    final_answer = rag_result.get("answer")
    answer_text = _normalize_gap_text(final_answer) if isinstance(final_answer, str) else ""
    all_gap_items = list(missing_evidence) + list(recovery_readiness_gaps) + list(future_documentation_gaps)
    answer_contains_all_gaps = all(
        _normalize_gap_text(item) in answer_text
        for item in all_gap_items
    )
    answer_generation = rag_result.get("answer_generation", {})
    deterministic_answer_used = (
        isinstance(answer_generation, Mapping)
        and answer_generation.get("final_answer_method") == "DETERMINISTIC_EVIDENCE_RECONCILED_SYNTHESIS"
        and answer_generation.get("model_freeform_answer_used_as_final") is False
    )
    bronze = evidence_reconciliation.get("bronze_table_identifier", {})
    bronze_value_visible = True
    if isinstance(bronze, Mapping) and bronze.get("status") == "RETURNED_BY_SCHEMA_INSPECTION":
        bronze_value_visible = isinstance(bronze.get("value"), str) and bronze.get("value").casefold() in answer_text

    passed = (
        category_alignment
        and bronze_reconciled
        and no_cross_category_overlap
        and answer_contains_all_gaps
        and deterministic_answer_used
        and bronze_value_visible
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "category_lists_match_rag_metadata": category_alignment,
        "no_cross_category_overlap": no_cross_category_overlap,
        "bronze_identifier_not_reported_missing_when_observed": bronze_reconciled,
        "final_answer_contains_every_structured_gap": answer_contains_all_gaps,
        "deterministic_final_answer_used": deterministic_answer_used,
        "observed_bronze_identifier_visible_in_final_answer": bronze_value_visible,
        "overlaps": overlaps,
        "qualification": "This verifies deterministic report consistency and source-reference membership, not independent truth of every source assertion or automatic claim-level semantic entailment.",
    }

def _classify_incident(text: str) -> Tuple[str, str, str]:
    """Deterministic policy classifier. Returns category, subtype and rationale."""
    lowered = text.lower()

    permission_patterns = (
        r"permission[_ ]denied",
        r"access[_ ]denied",
        r"accessdenied(?:exception)?",
        r"not authorized",
        r"unauthorized",
        r"forbidden",
        r"\b403\b",
        r"insufficient privileges",
        r"does not have .* permission",
        r"missing .* privilege",
    )
    if any(re.search(pattern, lowered) for pattern in permission_patterns):
        return (
            "PERMISSION_DENIED",
            "AUTHORIZATION_OR_ACCESS_FAILURE",
            "Observed error text contains an authorization/access-denial signal.",
        )

    missing_file_patterns = (
        r"file[_ ]not[_ ]found",
        r"filenotfoundexception",
        r"no such file",
        r"path does not exist",
        r"path not found",
        r"nosuchkey",
        r"no such key",
        r"input file .* not found",
        r"missing source file",
        r"source file .* missing",
    )
    if any(re.search(pattern, lowered) for pattern in missing_file_patterns):
        return (
            "MISSING_FILE",
            "SOURCE_OBJECT_NOT_FOUND_AT_EXECUTION",
            "Observed error text reports a missing source object or path.",
        )

    schema_patterns = (
        r"source contract violation",
        r"missing required column",
        r"missing_columns?",
        r"schema drift",
        r"schema mismatch",
        r"incompatible schema",
        r"column .* not found",
        r"cannot resolve column",
        r"unresolved_column",
        r"cannot resolve .* given input columns",
    )
    if any(re.search(pattern, lowered) for pattern in schema_patterns):
        return (
            "SCHEMA_DRIFT",
            "SOURCE_OR_TRANSFORMATION_SCHEMA_CONTRACT_VIOLATION",
            "Observed error text identifies a missing or incompatible column/schema contract.",
        )

    transformation_patterns = (
        r"cast_invalid_input",
        r"analysisexception",
        r"sparkexception",
        r"pythonexception",
        r"divide_by_zero",
        r"task_write_failed",
        r"failed_execute_udf",
        r"executorlostfailure",
        r"numberformatexception",
        r"arithmeticexception",
        r"transformation failed",
        r"failed while executing",
    )
    if any(re.search(pattern, lowered) for pattern in transformation_patterns):
        return (
            "SPARK_TRANSFORMATION_FAILURE",
            "SPARK_EXECUTION_OR_DATA_TRANSFORMATION_ERROR",
            "Observed error text contains a Spark/Python execution or transformation failure signal.",
        )

    return (
        "UNKNOWN",
        "UNCLASSIFIED_FAILURE",
        "No supported failure signature was found in the supplied operational evidence.",
    )


def _build_runbook_query(
    *, category: str, subtype: str, task: Optional[str], error_text: str, dataset: Optional[str]
) -> str:
    """Build a bounded search query from observed operational symptoms."""
    query_parts = [
        f"Zomato incident category {category}",
        f"failure subtype {subtype}",
    ]
    if task:
        query_parts.append(f"failed task {task}")
    if dataset:
        query_parts.append(f"dataset {dataset}")

    # Keep only the first concise observed error segment; avoid sending a full stack.
    concise_error = re.split(r"\n|Traceback", error_text, maxsplit=1)[0].strip()
    if concise_error:
        query_parts.append(f"observed error {concise_error[:350]}")
    return _redact_text(" | ".join(query_parts), max_length=900)


def _runbook_lifecycle_assessment(runbook_result: Mapping[str, Any]) -> Dict[str, Any]:
    rows = runbook_result.get("evidence")
    if not isinstance(rows, list):
        rows = []

    project_eligible = sum(
        1 for row in rows
        if isinstance(row, Mapping) and row.get("project_retrieval_eligible") is True
    )
    production_eligible = sum(
        1 for row in rows
        if isinstance(row, Mapping) and row.get("production_retrieval_eligible") is True
    )
    statuses = sorted({
        str(row.get("document_status"))
        for row in rows
        if isinstance(row, Mapping) and row.get("document_status") is not None
    })
    evidence_states = sorted({
        str(row.get("evidence_state"))
        for row in rows
        if isinstance(row, Mapping) and row.get("evidence_state") is not None
    })

    if rows and project_eligible == len(rows):
        assessment = "PROJECT_ELIGIBLE_NOT_EXTERNALLY_APPROVED"
    elif rows and project_eligible > 0:
        assessment = "MIXED_PROJECT_ELIGIBILITY"
    elif rows:
        assessment = "PROJECT_ELIGIBILITY_NOT_ESTABLISHED"
    else:
        assessment = "NO_RETRIEVED_DOCUMENTS"

    return {
        "assessment": assessment,
        "retrieved_document_count": len(rows),
        "project_retrieval_eligible_count": project_eligible,
        "production_retrieval_eligible_count": production_eligible,
        "document_statuses": statuses,
        "evidence_states": evidence_states,
        "interpretation": (
            "Project-authorized knowledge may be used for this portfolio assistant. "
            "Project retrieval eligibility is not external production approval. "
            "Preserve the source lifecycle metadata and do not present draft content "
            "as externally certified policy."
        ),
    }


def _default_rag_answer(
    *,
    question: str,
    retrieved_rows: Sequence[Dict[str, Any]],
    spark: Any,
    top_k: int,
    model_endpoint: str,
) -> Dict[str, Any]:
    """Call the existing Day 23 RAG function against cached search results.

    The adapter returns the already retrieved rows, so RAG does not make a
    second AI Search request. The existing RAG module validates chunk IDs and
    builds citations from these source rows.
    """
    try:
        from rag.response_generator import answer_question
    except Exception as exc:
        return {
            "status": "RAG_UNAVAILABLE",
            "answer": "The runbook chunks were retrieved, but the existing RAG module could not be imported.",
            "citations": [],
            "missing_evidence": ["Successful import of rag.response_generator.answer_question."],
            "warnings": [_redact_text(f"{type(exc).__name__}: {exc}", 600)],
            "retrieved_count": len(retrieved_rows),
        }

    cached_rows = list(retrieved_rows)

    def cached_search(*, query: str, top_k: int = DEFAULT_RUNBOOK_TOP_K) -> List[Dict[str, Any]]:
        del query  # Retrieval already ran with the incident-specific query.
        return cached_rows[:top_k]

    return answer_question(
        question=question,
        search_fn=cached_search,
        spark=spark,
        top_k=min(top_k, max(1, len(cached_rows))),
        model_endpoint=model_endpoint,
    )


def _runbook_alignment(category: str, retrieved_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Compare observed incident category with retrieved runbook text.

    This is a deterministic routing/coverage signal, not a claim that keyword
    overlap proves a procedure is correct or approved. The existing RAG output
    supplies the semantic explanation and citations.
    """
    term_groups = {
        "MISSING_FILE": ("missing file", "file not found", "source file", "object", "arrival", "s3"),
        "SCHEMA_DRIFT": ("source contract", "required column", "missing column", "schema", "column mapping", "header"),
        "PERMISSION_DENIED": ("permission", "authorization", "access denied", "iam", "external location", "grant"),
        "SPARK_TRANSFORMATION_FAILURE": ("transformation", "try_cast", "cast", "quarantine", "spark", "data type", "expression"),
    }
    target_terms = term_groups.get(category, ())
    aligned: List[Dict[str, Any]] = []

    for row in retrieved_rows:
        chunk_text = str(row.get("chunk_text", ""))
        lowered = chunk_text.lower()
        matched = [term for term in target_terms if term in lowered]
        aligned.append({
            "chunk_id": row.get("chunk_id"),
            "document_id": row.get("document_id"),
            "section_path": row.get("section_path"),
            "alignment": "TERM_MATCH" if matched else "NO_EXPLICIT_TERM_MATCH",
            "matched_terms": matched,
            "document_status": row.get("document_status"),
            "evidence_state": row.get("evidence_state"),
            "project_retrieval_eligible": row.get("project_retrieval_eligible"),
            "production_retrieval_eligible": row.get("production_retrieval_eligible"),
        })

    return {
        "method": "DETERMINISTIC_TERM_COVERAGE_PLUS_EXISTING_RAG",
        "category": category,
        "retrieved_chunk_count": len(retrieved_rows),
        "term_matched_chunk_count": sum(1 for row in aligned if row["alignment"] == "TERM_MATCH"),
        "chunks": aligned,
        "qualification": "Term overlap is only a retrieval-coverage signal. It does not prove factual correctness, document approval, or applicability of a recovery procedure.",
    }


def _extract_missing_evidence(context: Mapping[str, Any]) -> List[str]:
    missing = context.get("missing_evidence")
    if isinstance(missing, list):
        return [_redact_text(str(item), 500) for item in missing if item]
    if isinstance(missing, Mapping):
        items = missing.get("items") or missing.get("records")
        if isinstance(items, list):
            return [_redact_text(str(item), 500) for item in items if item]
    return []


def _diagnosis(
    *, category: str, subtype: str, error_text: str, source_file: Optional[str], table_name: Optional[str]
) -> Dict[str, Any]:
    has_explicit_error = bool(error_text.strip())

    descriptions = {
        "MISSING_FILE": (
            "The execution evidence reports that a source file or object was not found at the time of the failure."
        ),
        "SCHEMA_DRIFT": (
            "The execution evidence reports a missing or incompatible source/transformation schema element."
        ),
        "PERMISSION_DENIED": (
            "The execution evidence reports an authorization or access-denial failure."
        ),
        "SPARK_TRANSFORMATION_FAILURE": (
            "The execution evidence reports a Spark/Python transformation or execution failure."
        ),
        "UNKNOWN": (
            "The available evidence does not support a specific incident category."
        ),
    }

    alternatives = {
        "MISSING_FILE": [
            "Upstream delivery may have been delayed or omitted.",
            "The configured object path or partition may be incorrect.",
            "The expected object may have been moved or replaced before execution.",
        ],
        "SCHEMA_DRIFT": [
            "An upstream producer may have changed the file header or field mapping.",
            "The failed execution may have consumed an unintended file version or format.",
            "A malformed delimiter/header may have changed how columns were parsed.",
        ],
        "PERMISSION_DENIED": [
            "The execution identity may lack the required resource permission.",
            "A credential, role mapping, or external-location authorization may be misconfigured.",
            "A policy or key-management restriction may have denied access.",
        ],
        "SPARK_TRANSFORMATION_FAILURE": [
            "An input value may violate the transformation's expected data type or domain.",
            "A transformation expression or UDF may contain a logic defect.",
            "An upstream data-shape change may have exposed an unhandled transformation case.",
        ],
        "UNKNOWN": [
            "The current evidence may omit the failing operation or full exception context."
        ],
    }

    confirmed = []
    if has_explicit_error and category != "UNKNOWN":
        confirmed.append(
            "The supplied Job/Pipeline evidence contains an error signature consistent with "
            f"{category}; this confirms the observed failure mechanism, not necessarily the upstream reason it arose."
        )
    if source_file:
        confirmed.append(f"The operational evidence references source object: {source_file}.")
    if table_name:
        confirmed.append(f"The operational evidence explicitly identifies table: {table_name}.")

    return {
        "assessment": "OBSERVED_FAILURE_MECHANISM" if confirmed else "UNDETERMINED",
        "category": category,
        "subtype": subtype,
        "description": descriptions[category],
        "confirmed_facts": confirmed,
        "likely_causes": (
            [{"cause": descriptions[category], "confidence": "HIGH", "basis": "Observed Job/Pipeline error evidence."}]
            if has_explicit_error and category != "UNKNOWN"
            else []
        ),
        "alternative_causes": alternatives[category],
        "qualification": (
            "The upstream reason behind the observed failure mechanism remains unproven unless the collected evidence directly establishes it."
        ),
    }


def _selected_evidence_tools(
    *, category: str, dataset: Optional[str], table_name: Optional[str], pipeline_update_id: Optional[str]
) -> Tuple[List[str], List[str]]:
    selected: List[str] = []
    rationale: List[str] = []

    if category == "SCHEMA_DRIFT":
        if dataset:
            selected.append("inspect_schema_changes")
            rationale.append("Dataset is known and the failure signature concerns a schema/column contract.")
            selected.append("get_data_quality_results")
            rationale.append("Dataset-level quarantine evidence may add context, but is not assumed to be run-correlated.")
        else:
            rationale.append("Schema/DQ inspection skipped because no approved dataset could be identified.")

    elif category == "SPARK_TRANSFORMATION_FAILURE":
        if dataset:
            selected.append("get_data_quality_results")
            rationale.append("Dataset-level DQ/quarantine evidence may contextualize the transformation failure.")
        else:
            rationale.append("DQ inspection skipped because no approved dataset could be identified.")

        if table_name:
            selected.append("get_delta_table_history")
            rationale.append("An explicitly evidenced approved table is available for Delta history inspection.")
        else:
            rationale.append("Delta history skipped because the incident evidence did not identify one unambiguous approved table.")

        if table_name and pipeline_update_id:
            selected.append("get_downstream_impact")
            rationale.append("Both an explicitly evidenced table and exact Pipeline update ID are available for scoped lineage inspection.")
        else:
            rationale.append("Downstream impact skipped because an exact table/update pair is not established.")

    elif category == "MISSING_FILE":
        rationale.append(
            "Schema, DQ, Delta and lineage tools are skipped: the current registry has no independent source-object existence tool, and unrelated table checks could imply unsupported conclusions."
        )

    elif category == "PERMISSION_DENIED":
        rationale.append(
            "Schema, DQ, Delta and lineage tools are skipped: investigate the authorization boundary first; do not retry with elevated privileges."
        )

    else:
        rationale.append("Category-specific evidence tools skipped because the failure category is unknown.")

    return selected, rationale


def _report_status(
    *,
    tool_calls: Sequence[Mapping[str, Any]],
    rag_result: Mapping[str, Any],
    context: Mapping[str, Any],
    job_details: Mapping[str, Any],
) -> str:
    required_statuses = []
    for name in ("get_incident_context", "get_job_run_details"):
        matching = [call.get("status") for call in tool_calls if call.get("tool_name") == name]
        required_statuses.extend(matching[:1])

    if len(required_statuses) < 2 or any(status not in SUCCESS_STATUSES for status in required_statuses):
        return "INSUFFICIENT_EVIDENCE"

    optional_problem = any(
        call.get("status") not in SUCCESS_STATUSES
        for call in tool_calls
        if call.get("tool_name") not in {"get_incident_context", "get_job_run_details"}
    )
    rag_status = str(rag_result.get("status", "RAG_UNAVAILABLE")).upper()
    if optional_problem or rag_status not in {"ANSWERED"}:
        return "PARTIAL" if rag_status not in {"RAG_UNAVAILABLE", "ERROR"} else "PARTIAL"

    return "COMPLETED"


def run_investigation(
    *,
    run_id: int,
    registry: Mapping[str, Callable[..., Any]],
    workspace_client: Any,
    spark: Any,
    rag_answer_fn: Optional[Callable[..., Dict[str, Any]]] = None,
    runbook_top_k: int = DEFAULT_RUNBOOK_TOP_K,
    rag_model_endpoint: str = DEFAULT_RAG_MODEL_ENDPOINT,
    delta_history_limit: int = DEFAULT_DELTA_HISTORY_LIMIT,
    request_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    expected_job_id: Optional[int] = None,
    expected_pipeline_id: Optional[str] = None,
    evidence_persist_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Run one bounded incident investigation using only governed tools.

    Args:
        run_id: Stable approved Zomato Job run ID.
        registry: Exact mapping returned by operations.agent_tools.get_agent_tool_registry().
        workspace_client: Databricks WorkspaceClient used by the existing wrappers.
        spark: SparkSession used by the existing wrappers and RAG implementation.
        rag_answer_fn: Optional replacement for the existing RAG function, useful for tests.

    Returns:
        A structured, JSON-serializable incident investigation report.

    Notes:
        - All calls are serial and read-only.
        - Request identity is carried into tool-call and evidence identifiers.
        - Read-only evidence scope and primary metadata identity are reported.
        - It never registers or invokes raw operations outside the governed registry.
    """
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer.")
    request_id = normalize_request_id(request_id or _new_manual_request_id())
    trace_id = trace_id or _new_trace_id()
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise ValueError("trace_id must be a non-empty string.")
    if not isinstance(registry, Mapping):
        raise TypeError("registry must be the mapping returned by get_agent_tool_registry().")
    if evidence_persist_callback is not None and not callable(evidence_persist_callback):
        raise TypeError("evidence_persist_callback must be callable when supplied.")
    if not isinstance(runbook_top_k, int) or isinstance(runbook_top_k, bool) or not 1 <= runbook_top_k <= 10:
        raise ValueError("runbook_top_k must be an integer between 1 and 10.")
    if not isinstance(delta_history_limit, int) or isinstance(delta_history_limit, bool) or not 1 <= delta_history_limit <= 50:
        raise ValueError("delta_history_limit must be an integer between 1 and 50.")

    missing_tools = sorted(EXPECTED_AGENT_TOOLS - set(registry.keys()))
    if missing_tools:
        raise ValueError(f"Governed registry is missing required tools: {missing_tools}")

    evidence: Dict[str, Dict[str, Any]] = {}
    tool_calls: List[Dict[str, Any]] = []
    selection_rationale: List[str] = []
    missing_evidence: List[str] = []
    warnings: List[str] = [
        "Execution is serial and read-only. Retry orchestration and enforced timeouts are outside the agreed Day 27/28 implementation scope."
    ]

    # Phase 1: stable run identity and context.
    context = _execute_tool(
        registry=registry,
        tool_name="get_incident_context",
        args={"workspace_client": workspace_client, "spark": spark, "run_id": run_id, "event_limit": 100},
        evidence=evidence,
        tool_calls=tool_calls,
        run_id=run_id,
        request_id=request_id,
        trace_id=trace_id,
        evidence_persist_callback=evidence_persist_callback,
    )
    context_status = _status(context)

    job_details = _execute_tool(
        registry=registry,
        tool_name="get_job_run_details",
        args={"workspace_client": workspace_client, "run_id": run_id},
        evidence=evidence,
        tool_calls=tool_calls,
        run_id=run_id,
        request_id=request_id,
        trace_id=trace_id,
        evidence_persist_callback=evidence_persist_callback,
    )
    job_status = _status(job_details)

    primary_identity_validation = _validate_primary_evidence_identity(
        run_id=run_id,
        context=context,
        job_details=job_details,
        expected_job_id=expected_job_id,
        expected_pipeline_id=expected_pipeline_id,
    )
    if primary_identity_validation["status"] == "FAILED":
        return {
            "schema_version": WORKFLOW_VERSION,
            "incident_id": incident_id_for_run(run_id),
            "request_id": request_id,
            "trace_id": trace_id,
            "run_id": run_id,
            "workflow_execution_status": "COMPLETED",
            "investigation_status": "INSUFFICIENT_EVIDENCE",
            "evidence_completeness": "INSUFFICIENT",
            "termination_reason": "PRIMARY_EVIDENCE_IDENTITY_MISMATCH",
            "confirmed_facts": [],
            "likely_causes": [],
            "alternative_causes": [],
            "missing_evidence": ["Consistent Job run, Job and Pipeline identity across the primary evidence tools."],
            "tool_calls": tool_calls,
            "selected_tools": ["get_incident_context", "get_job_run_details"],
            "evidence_references": [
                {"evidence_id": c["evidence_id"], "tool_call_id": c["tool_call_id"], "tool_name": c["tool_name"], "status": c["status"]}
                for c in tool_calls
            ],
            "evidence_validation": primary_identity_validation,
            "evidence_reconciliation": {"status": "NOT_EVALUATED", "reason": "Primary evidence identity validation failed before schema inspection."},
            "evidence_scope": _build_evidence_scope(evidence, None),
            "tool_failures": [c for c in tool_calls if c.get("status") not in SUCCESS_STATUSES],
            "human_approval": {
                "required_before_any_change": True,
                "status": "NOT_REQUESTED",
                "agent_can_execute_recovery": False,
                "execution_status": "NOT_EXECUTED",
            },
            "evidence": evidence,
            "execution_policy": {
                "execution_mode": "SERIAL",
                "tool_boundary": "DAY25_GOVERNED_REGISTRY_ONLY",
                "read_only": True,
                "automatic_remediation": False,
                "retries_enabled": False,
                "enforced_timeouts": False,
            "durable_persistence": False,
            "durable_evidence_persistence": evidence_persist_callback is not None,
            },
        }

    context_has_payload = bool(context.get("incident_reference") or context.get("root_cause_evidence") or context.get("evidence_references"))
    job_has_payload = bool(job_details.get("job_run") or job_details.get("tasks"))

    if not context_has_payload and not job_has_payload:
        missing_evidence.append("Neither incident context nor Job details returned usable incident payload.")
        return {
            "schema_version": WORKFLOW_VERSION,
            "incident_id": incident_id_for_run(run_id),
            "request_id": request_id,
            "trace_id": trace_id,
            "run_id": run_id,
            "workflow_execution_status": "COMPLETED",
            "investigation_status": "INSUFFICIENT_EVIDENCE",
            "evidence_completeness": "INSUFFICIENT",
            "status_semantics": {
                "workflow_execution_status": "The orchestrator finished its selected execution path without an unhandled exception.",
                "investigation_status": "The required incident/job context and runbook synthesis completed according to the workflow policy; it does not assert that every possible evidence source exists.",
                "evidence_completeness": "INSUFFICIENT means required incident or Job evidence was unavailable.",
                "termination_reason": "Required incident or Job evidence was not usable, so no further investigation was attempted.",
            },
            "termination_reason": "REQUIRED_INCIDENT_OR_JOB_EVIDENCE_UNAVAILABLE",
            "incident_category": "UNKNOWN",
            "tool_calls": tool_calls,
            "selected_tools": ["get_incident_context", "get_job_run_details"],
            "evidence_references": [
                {"evidence_id": c["evidence_id"], "tool_call_id": c["tool_call_id"], "tool_name": c["tool_name"], "status": c["status"]}
                for c in tool_calls
            ],
            "confirmed_facts": [],
            "likely_causes": [],
            "alternative_causes": [],
            "downstream_impact": {"status": "NOT_EVALUATED"},
            "runbook_analysis": {"status": "NOT_RUN"},
            "missing_evidence": missing_evidence,
            "warnings": warnings,
            "evidence_validation": primary_identity_validation,
            "evidence_reconciliation": {"status": "NOT_EVALUATED", "reason": "Required incident or Job payload was unavailable before schema inspection."},
            "evidence_scope": _build_evidence_scope(evidence, None),
            "tool_failures": [c for c in tool_calls if c.get("status") not in SUCCESS_STATUSES],
            "human_approval": {"required_before_any_change": True, "status": "NOT_REQUESTED", "agent_can_execute_recovery": False},
            "evidence": evidence,
        }

    root_records = _extract_root_cause_records(context)
    failed_task = _extract_failed_task(context, job_details)
    error_text = _collect_text({"context": context, "job_details": job_details})
    category, subtype, category_rationale = _classify_incident(error_text)
    observed_missing_columns = _extract_missing_columns(error_text)
    if category == "SCHEMA_DRIFT" and observed_missing_columns:
        subtype = "SOURCE_CONTRACT_VIOLATION_MISSING_REQUIRED_COLUMN"
        category_rationale = (
            "Observed source/schema contract failure explicitly identifies missing column(s): "
            + ", ".join(observed_missing_columns)
            + "."
        )
    selection_rationale.append(category_rationale)

    dataset = _extract_dataset({"context": context, "job_details": job_details, "root_records": root_records})
    table_name = _extract_table_name({"context": context, "job_details": job_details, "root_records": root_records})
    pipeline_update_id = _extract_pipeline_update_id(context)

    source_file: Optional[str] = None
    for item in _walk({"context": context, "job_details": job_details, "root_records": root_records}):
        if isinstance(item, Mapping):
            for key in ("source_file", "file_path", "path", "file"):
                value = item.get(key)
                if isinstance(value, str) and ("s3://" in value or "/" in value):
                    source_file = _redact_text(value, 500)
                    break
        if source_file:
            break
    if source_file is None:
        source_match = re.search(r"s3://[^\s'\"\],}]+", error_text)
        if source_match:
            # Error messages commonly place a sentence-ending full stop directly
            # after the URI. Remove prose punctuation, not URI query/path content.
            source_file = _redact_text(source_match.group(0).rstrip(".,;:"), 500)

    selected_evidence_tools, evidence_rationale = _selected_evidence_tools(
        category=category,
        dataset=dataset,
        table_name=table_name,
        pipeline_update_id=pipeline_update_id,
    )
    selection_rationale.extend(evidence_rationale)

    # Phase 2: invoke only selected tools, always serially.
    for tool_name in selected_evidence_tools:
        if tool_name == "inspect_schema_changes":
            args = {"spark": spark, "dataset": dataset}
        elif tool_name == "get_data_quality_results":
            args = {"spark": spark, "dataset": dataset}
        elif tool_name == "get_delta_table_history":
            args = {"spark": spark, "table_name": table_name, "limit": delta_history_limit}
        elif tool_name == "get_downstream_impact":
            args = {
                "spark": spark,
                "table_name": table_name,
                "pipeline_update_id": pipeline_update_id,
                "max_depth": 3,
                "max_results": 100,
            }
        else:  # Defensive; selection function only returns the allowed names above.
            continue

        _execute_tool(
            registry=registry,
            tool_name=tool_name,
            args=args,
            evidence=evidence,
            tool_calls=tool_calls,
            run_id=run_id,
            request_id=request_id,
            trace_id=trace_id,
            evidence_persist_callback=evidence_persist_callback,
        )

    # Phase 3: retrieve the project's runbooks based on observed symptoms.
    runbook_query = _build_runbook_query(
        category=category,
        subtype=subtype,
        task=failed_task,
        error_text=error_text,
        dataset=dataset,
    )
    runbook_result = _execute_tool(
        registry=registry,
        tool_name="search_runbooks",
        args={"query": runbook_query, "top_k": runbook_top_k},
        evidence=evidence,
        tool_calls=tool_calls,
        run_id=run_id,
        request_id=request_id,
        trace_id=trace_id,
        evidence_persist_callback=evidence_persist_callback,
    )

    retrieved_rows = runbook_result.get("evidence")
    if not isinstance(retrieved_rows, list):
        retrieved_rows = []
    retrieved_rows = [dict(row) for row in retrieved_rows if isinstance(row, Mapping)]

    lifecycle = _runbook_lifecycle_assessment(runbook_result)
    runbook_alignment = _runbook_alignment(category, retrieved_rows)
    if lifecycle["assessment"] in {
        "PROJECT_ELIGIBLE_NOT_EXTERNALLY_APPROVED",
        "MIXED_PROJECT_ELIGIBILITY",
    }:
        warnings.append(
            "Retrieved runbooks are usable project knowledge, but their project retrieval eligibility does not establish external production approval."
        )

    # Phase 4: compare evidence with the retrieved runbooks using the existing RAG function.
    if retrieved_rows and _status(runbook_result) in USABLE_EVIDENCE_STATUSES:
        operational_context = {
            "incident_category": category,
            "incident_subtype": subtype,
            "failed_task": failed_task,
            "source_dataset": dataset,
            "source_file": source_file,
            "affected_table": table_name,
            "pipeline_update_id": pipeline_update_id,
            "incident_context_status": context_status,
            "job_details_status": job_status,
            "selected_evidence": {
                name: {
                    "status": _status(result),
                    "summary": result.get("summary"),
                    "datasets": result.get("datasets"),
                    "job_run": result.get("job_run"),
                    "tasks": result.get("tasks"),
                    "history": result.get("history"),
                    "downstream_tables": result.get("downstream_tables"),
                }
                for name, result in evidence.items()
                if name not in {"search_runbooks"}
            },
        }
        operational_context_json = str(_safe_value(operational_context))[:5000]
        question = (
    "You are performing evidence-aware runbook analysis for a data platform incident.\n\n"

    "STRICT EVIDENCE RULES:\n"
    "1. Separate observed operational evidence from documented runbook guidance.\n"
    "2. Operational tool results are authoritative for what was actually inspected.\n"
    "3. Retrieved runbook chunks establish documented procedures, not live system state.\n"
    "4. Before identifying something as missing, compare it with the supplied operational evidence.\n"
    "5. Never report an identifier as unknown if an operational tool explicitly returned it.\n"
    "6. If a Bronze table is returned by schema inspection, treat that table identifier as known.\n"
    "7. A known Bronze table does not establish the Auto Loader checkpoint location. "
    "Report the checkpoint path separately as missing only when it was not returned.\n"
    "8. Metadata nullability differences do not prove actual NULL business values.\n"
    "9. Never conflate different columns, such as customer_id and user_id.\n"
    "10. Distinguish a documented recovery outline from a validated, production-approved procedure.\n"
    "11. Separate missing operational evidence, recovery-readiness gaps, and future documentation gaps.\n"
    "12. Do not recommend a remediation that is not supported by the retrieved chunks.\n\n"

    "TASK:\n"
    "Identify the runbook procedures relevant to the observed incident. "
    "Explain the investigation and recovery guidance actually documented. "
    "Identify genuine evidence gaps only after checking the operational results. "
    "Identify recovery-readiness and documentation gaps separately. "
    "Do not invent missing information or repeat stale documentation statements "
    "when current operational evidence provides a more recent value.\n\n"

    f"INCIDENT EVIDENCE CONTEXT (from read-only tools): "
    f"{operational_context_json}\n\n"

    f"OBSERVED ERROR: {error_text[:1200]}"
)
        rag_fn = rag_answer_fn
        if rag_fn is None:
            rag_result = _default_rag_answer(
                question=question,
                retrieved_rows=retrieved_rows,
                spark=spark,
                top_k=runbook_top_k,
                model_endpoint=rag_model_endpoint,
            )
        else:
            def cached_search(*, query: str, top_k: int = runbook_top_k) -> List[Dict[str, Any]]:
                del query
                return retrieved_rows[:top_k]
            try:
                rag_result = rag_fn(
                    question=question,
                    search_fn=cached_search,
                    spark=spark,
                    top_k=min(runbook_top_k, len(retrieved_rows)),
                    model_endpoint=rag_model_endpoint,
                )
                if not isinstance(rag_result, dict):
                    rag_result = {
                        "status": "MALFORMED_OUTPUT",
                        "answer": "RAG returned a non-dictionary response.",
                        "citations": [],
                        "missing_evidence": ["A valid structured RAG response."],
                        "warnings": [],
                    }
            except Exception as exc:
                rag_result = {
                    "status": "ERROR",
                    "answer": "Runbook retrieval succeeded, but grounded response generation failed. No model-generated procedure is presented.",
                    "citations": [],
                    "missing_evidence": ["A successful grounded RAG generation response."],
                    "warnings": [_redact_text(f"{type(exc).__name__}: {exc}", 800)],
                }
    else:
        rag_result = {
            "status": "INSUFFICIENT_EVIDENCE",
            "answer": "No usable runbook chunks were returned for this incident query.",
            "citations": [],
            "missing_evidence": ["Relevant runbook chunks for the observed incident symptoms."],
            "warnings": [],
            "retrieved_count": 0,
        }

    rag_result = _safe_value(rag_result)
    answer_grounding = _assess_answer_grounding(rag_result, retrieved_rows)
    grounding_fallback_reason = None
    if answer_grounding["status"] in {
        "INVALID_CITATION_REFERENCE",
        "ANSWER_WITHOUT_VERIFIABLE_CITATIONS",
    }:
        grounding_fallback_reason = answer_grounding["status"]
        # Fail closed: do not expose an answer as operational guidance when
        # its source reference cannot be tied to this exact retrieval result.
        rag_result = dict(rag_result)
        rag_result["status"] = "INSUFFICIENT_EVIDENCE"
        rag_result["answer"] = (
            "The generated answer could not be linked to verifiable runbook "
            "citations from the current retrieval result. The assistant is "
            "abstaining rather than presenting unsupported operational guidance."
        )
        rag_result["citations"] = []
        missing_items = rag_result.get("missing_evidence")
        if not isinstance(missing_items, list):
            missing_items = []
        missing_items.append("At least one valid citation from the current retrieved runbook chunks.")
        rag_result["missing_evidence"] = list(dict.fromkeys(missing_items))
        answer_grounding = _assess_answer_grounding(rag_result, retrieved_rows)
        answer_grounding["fallback_reason"] = grounding_fallback_reason

    # Phase 5: assemble evidence-aware diagnosis, missing evidence and impact.
    diagnosis = _diagnosis(
        category=category,
        subtype=subtype,
        error_text=error_text,
        source_file=source_file,
        table_name=table_name,
    )
    diagnosis["observed_missing_columns"] = observed_missing_columns
    if observed_missing_columns:
        diagnosis["confirmed_facts"].append(
            "The execution error explicitly identifies missing column(s): "
            + ", ".join(observed_missing_columns)
            + ". This is validator/error evidence; the current S3 object was not independently inspected by this workflow."
        )

    missing_evidence.extend(_extract_missing_evidence(context))
    if primary_identity_validation["status"] == "PARTIAL":
        missing_evidence.append("One or more primary Job/Pipeline identity fields were not returned and could not be independently verified.")
    if grounding_fallback_reason == "INVALID_CITATION_REFERENCE":
        warnings.append("RAG returned a citation identifier that is not present in the current AI Search retrieval result; the answer was replaced with a safe abstention.")
    elif grounding_fallback_reason == "ANSWER_WITHOUT_VERIFIABLE_CITATIONS":
        warnings.append("RAG returned an answer without a verifiable citation; the answer was replaced with a safe abstention.")
    if category == "MISSING_FILE":
        missing_evidence.append(
            "Independent current S3 object-existence/metadata evidence; no such source-object tool is registered in the current governed registry."
        )
    if category == "PERMISSION_DENIED":
        missing_evidence.append(
            "Authoritative authorization decision for the exact execution identity and resource, obtained through the platform's approved access-review process."
        )
    if category == "SPARK_TRANSFORMATION_FAILURE" and not table_name:
        missing_evidence.append("The exact affected Delta table, needed before inspecting Delta history or downstream lineage.")

    downstream = {"status": "NOT_EVALUATED", "reason": "No incident-specific table/update pair was selected by the evidence policy."}
    impact_result = evidence.get("get_downstream_impact")
    if isinstance(impact_result, Mapping):
        downstream = {
            "status": _status(impact_result),
            "source_table": impact_result.get("source_table"),
            "pipeline_update_id": pipeline_update_id,
            "summary": impact_result.get("summary"),
            "downstream_tables": impact_result.get("downstream_tables", []),
            "lineage_edge_count": len(impact_result.get("lineage_edges") or [])
            if isinstance(impact_result.get("lineage_edges"), list) else None,
            "limitations": impact_result.get("limitations", []),
        }
        if downstream["status"] == "NO_MATCH":
            missing_evidence.append(
                "Incident-specific lineage returned NO_MATCH. This does not prove that no downstream dependency exists."
            )
        elif downstream["status"] == "PARTIAL":
            missing_evidence.append("Complete incident-specific lineage results; the downstream tool returned PARTIAL.")

    # DQ tool explicitly says its quarantine evidence is not run-correlated.
    dq_result = evidence.get("get_data_quality_results")
    if isinstance(dq_result, Mapping) and dq_result.get("run_correlation") == "NOT_AVAILABLE":
        warning = (
            "DQ/quarantine evidence is dataset-level and cannot be attributed to this Job run or Pipeline update using the current quarantine tables."
        )
        warnings.append(warning)
        missing_evidence.append("Run-correlated DQ/quarantine evidence for this specific Job run or Pipeline update.")

    delta_result = evidence.get("get_delta_table_history")
    if isinstance(delta_result, Mapping):
        warnings.append(
            "Delta history is table-level context. Do not attribute a commit to this Job run unless timestamps and available operation/job metadata are independently correlated."
        )

    schema_result = evidence.get("inspect_schema_changes")
    if isinstance(schema_result, Mapping):
        schema_datasets = schema_result.get("datasets")
        for schema_item in schema_datasets.values() if isinstance(schema_datasets, Mapping) else []:
            if isinstance(schema_item, Mapping):
                if schema_item.get("data_values_scanned") is False:
                    warnings.append("Schema inspection compared metadata/contracts but did not scan business row values.")
                if schema_item.get("nullability_metadata_mismatches"):
                    warnings.append("Nullability differences are metadata comparisons and do not establish that actual business rows contain NULL values.")

    if category == "MISSING_FILE":
        safe_next_steps = [
            "Verify the expected object and exact prefix using an approved source-storage inspection mechanism.",
            "Confirm the upstream producer's delivery status and expected arrival/partition contract.",
            "Compare the configured source URI with the producer contract before any replay is considered.",
        ]
    elif category == "SCHEMA_DRIFT":
        safe_next_steps = [
            "Compare the failing file's actual header with the approved project source contract.",
            "Confirm with the source owner whether the missing/changed field is an intentional contract change.",
            "Propose a reviewed contract or transformation change; do not auto-alter schemas or rewrite data.",
        ]
    elif category == "PERMISSION_DENIED":
        safe_next_steps = [
            "Identify the exact execution identity and resource from authoritative platform evidence.",
            "Ask the resource owner/platform administrator to review the least-privilege grants and trust/role mapping.",
            "Do not retry as another user, broaden permissions automatically, or expose credentials in logs/prompts.",
        ]
    elif category == "SPARK_TRANSFORMATION_FAILURE":
        safe_next_steps = [
            "Inspect the exact failing transformation expression and redacted input shape/record evidence.",
            "Use the returned DQ, schema and Delta history evidence only within their stated correlation scope.",
            "Prepare a code change for review and validate it against a controlled fixture before deployment.",
        ]
    else:
        safe_next_steps = [
            "Collect the exact failing task, error class, exception message and relevant pipeline event before recommending remediation."
        ]

    for item in rag_result.get("missing_evidence", []) if isinstance(rag_result.get("missing_evidence"), list) else []:
        if item:
            missing_evidence.append(_redact_text(str(item), 500))

    for item in rag_result.get("warnings", []) if isinstance(rag_result.get("warnings"), list) else []:
        if item:
            warnings.append(_redact_text(str(item), 500))

    # Reconcile model-generated Bronze identifier gaps against current,
    # structured schema-inspection output before classifying report gaps.
    schema_result_for_reconciliation = evidence.get("inspect_schema_changes", {})
    if not isinstance(schema_result_for_reconciliation, Mapping):
        schema_result_for_reconciliation = {}

    evidence_reconciliation = _build_evidence_reconciliation(
        dataset=dataset,
        schema_result=schema_result_for_reconciliation,
    )
    missing_evidence.extend(
        _build_schema_investigation_gaps(
            category=category,
            dataset=dataset,
            source_file=source_file,
            operational_evidence={
                "context": context,
                "job_details": job_details,
                "schema_inspection": schema_result_for_reconciliation,
            },
            schema_result=schema_result_for_reconciliation,
        )
    )
    missing_evidence = _apply_evidence_reconciliation(
        rag_result=rag_result,
        missing_evidence=missing_evidence,
        reconciliation=evidence_reconciliation,
    )

    if evidence_reconciliation["bronze_table_identifier"]["status"] == "RETURNED_BY_SCHEMA_INSPECTION":
        warnings.append(
            "Evidence reconciliation: the Bronze table identifier was returned by "
            "the current schema-inspection tool. Checkpoint location and business-row "
            "validation remain separate evidence requirements."
        )

    documentation_reconciliation = _build_documentation_reconciliation(
        retrieved_rows=retrieved_rows,
        evidence_reconciliation=evidence_reconciliation,
        runbook_lifecycle=lifecycle,
    )
    missing_evidence.extend(
        _build_recovery_readiness_baseline(
            dataset=dataset,
            evidence_reconciliation=evidence_reconciliation,
            retrieved_rows=retrieved_rows,
        )
    )
    # Project governance gaps (external approval/owner-review metadata) are not
    # operational evidence blockers for this portfolio assistant. Preserve them
    # separately instead of silently discarding them.
    future_documentation_gaps: List[str] = []
    recovery_readiness_gaps: List[str] = []
    operational_missing_evidence: List[str] = []
    for item in missing_evidence:
        cleaned = _redact_text(str(item), 500).strip()
        if not cleaned:
            continue
        if _is_project_governance_gap(cleaned):
            if _is_runbook_approval_metadata_gap(cleaned):
                future_documentation_gaps.append(RUNBOOK_DOCUMENT_CONTROL_GAP)
            else:
                future_documentation_gaps.append(cleaned)
        elif _is_recovery_readiness_gap(cleaned):
            recovery_readiness_gaps.append(cleaned)
        else:
            operational_missing_evidence.append(cleaned)

    # Stale source-document statements are documentation-maintenance gaps, not
    # operational evidence gaps. Draft lifecycle metadata remains separate.
    future_documentation_gaps.extend(
        documentation_reconciliation.get("future_documentation_gaps", [])
    )

    # Keep all lists deterministic and de-duplicated. Recovery-readiness and
    # documentation gaps are not evidence that changes the incident diagnosis.
    missing_evidence = _deduplicate_gap_items(operational_missing_evidence, "missing")
    recovery_readiness_gaps = _deduplicate_gap_items(recovery_readiness_gaps, "recovery")
    future_documentation_gaps = _deduplicate_gap_items(future_documentation_gaps, "future")
    warnings = list(dict.fromkeys(warnings))

    # The structured report is authoritative. Never expose free-form model prose
    # that can contradict live tool output or the final gap categories.
    rag_model_status = str(rag_result.get("status", "UNKNOWN")).upper()
    rag_result["missing_evidence"] = list(missing_evidence)
    rag_result["recovery_readiness_gaps"] = list(recovery_readiness_gaps)
    rag_result["future_documentation_gaps"] = list(future_documentation_gaps)
    rag_result["evidence_reconciliation"] = evidence_reconciliation
    rag_result["documentation_reconciliation"] = documentation_reconciliation
    rag_result["answer_generation"] = {
        "model_draft_status": rag_model_status,
        "final_answer_method": "DETERMINISTIC_EVIDENCE_RECONCILED_SYNTHESIS",
        "model_freeform_answer_used_as_final": False,
        "reason": (
            "Free-form model prose is not authoritative for live operational facts or gap categories. "
            "The final answer is rebuilt from governed tool results, retrieved source metadata, "
            "and the reconciled structured gap lists."
        ),
    }
    if rag_model_status == "ANSWERED" and retrieved_rows:
        authoritative_answer, authoritative_citations = _build_authoritative_rag_answer(
            category=category,
            subtype=subtype,
            failed_task=failed_task,
            dataset=dataset,
            source_file=source_file,
            observed_missing_columns=observed_missing_columns,
            error_text=error_text,
            retrieved_rows=retrieved_rows,
            evidence=evidence,
            evidence_reconciliation=evidence_reconciliation,
            documentation_reconciliation=documentation_reconciliation,
            missing_evidence=missing_evidence,
            recovery_readiness_gaps=recovery_readiness_gaps,
            future_documentation_gaps=future_documentation_gaps,
            safe_next_steps=safe_next_steps,
        )
        rag_result["answer"] = authoritative_answer
        rag_result["citations"] = authoritative_citations
    rag_result["answer_grounding"] = _assess_answer_grounding(rag_result, retrieved_rows)
    rag_result["answer_grounding"]["claim_level_semantic_entailment"] = "NOT_AUTOMATICALLY_VERIFIED"
    rag_result["answer_grounding"]["qualification"] = (
        "Citation membership is checked against the current retrieval result. "
        "This does not by itself prove semantic entailment; the operator-facing final answer "
        "is a deterministic synthesis of structured evidence and retrieved source metadata."
    )
    answer_grounding = rag_result["answer_grounding"]

    report_consistency = _build_report_consistency(
        missing_evidence=missing_evidence,
        recovery_readiness_gaps=recovery_readiness_gaps,
        future_documentation_gaps=future_documentation_gaps,
        rag_result=rag_result,
        evidence_reconciliation=evidence_reconciliation,
    )

    selected_tools = ["get_incident_context", "get_job_run_details"] + selected_evidence_tools + ["search_runbooks"]
    investigation_status = _report_status(
        tool_calls=tool_calls,
        rag_result=rag_result,
        context=context,
        job_details=job_details,
    )
    if investigation_status == "INSUFFICIENT_EVIDENCE":
        termination_reason = "REQUIRED_INCIDENT_OR_JOB_EVIDENCE_UNAVAILABLE"
    elif str(rag_result.get("status", "")).upper() != "ANSWERED":
        termination_reason = "RUNBOOK_EVIDENCE_INSUFFICIENT_OR_RAG_UNAVAILABLE"
    elif any(call.get("status") == "PARTIAL" for call in tool_calls):
        termination_reason = "OPTIONAL_EVIDENCE_PARTIAL"
    else:
        termination_reason = "CATEGORY_POLICY_COMPLETED"

    return {
        "schema_version": WORKFLOW_VERSION,
        "incident_id": incident_id_for_run(run_id),
        "request_id": request_id,
        "trace_id": trace_id,
        "run_id": run_id,
        "job_id": (job_details.get("job_run") or {}).get("job_id") if isinstance(job_details.get("job_run"), Mapping) else None,
        "job_name": (context.get("job") or {}).get("job_name") if isinstance(context.get("job"), Mapping) else (job_details.get("job_run") or {}).get("run_name") if isinstance(job_details.get("job_run"), Mapping) else None,
        "pipeline_id": (context.get("pipeline") or {}).get("pipeline_id") if isinstance(context.get("pipeline"), Mapping) else None,
        "workflow_execution_status": "COMPLETED",
        "investigation_status": investigation_status,
        "evidence_completeness": (
            "INSUFFICIENT"
            if investigation_status == "INSUFFICIENT_EVIDENCE"
            else "PARTIAL" if missing_evidence else "COMPLETE"
        ),
        "status_semantics": {
            "workflow_execution_status": "The orchestrator finished its selected execution path without an unhandled exception.",
            "investigation_status": "The required incident/job context and runbook synthesis completed according to the workflow policy; it does not assert that every possible evidence source exists.",
            "evidence_completeness": "COMPLETE means no unresolved operational evidence gaps were recorded; PARTIAL means the missing_evidence list is non-empty. Recovery-readiness and future documentation gaps are tracked separately.",
            "recovery_readiness_gaps": "Information needed to prepare a controlled recovery or replay; these gaps do not independently establish the cause of the incident.",
            "future_documentation_gaps": "Stale or incomplete source-document statements and project documentation improvements. Draft lifecycle metadata is reported separately and does not block project-eligible retrieval.",
            "termination_reason": "CATEGORY_POLICY_COMPLETED means all tools selected by the deterministic category policy were executed; it does not mean every conceivable tool was called.",
        },
        "termination_reason": termination_reason,
        "incident_category": category,
        "incident_subtype": subtype,
        "failed_task": failed_task,
        "error_evidence": {
            "summary": _redact_text(error_text, 1800),
            "root_cause_evidence_count": len(root_records),
            "root_cause_status": context.get("root_cause_status"),
        },
        "source_dataset": dataset,
        "source_file": source_file,
        "affected_table": table_name,
        "pipeline_update_id": pipeline_update_id,
        "selected_tools": selected_tools,
        "selection_rationale": selection_rationale,
        "tool_calls": tool_calls,
        "evidence_references": [
            {"evidence_id": call["evidence_id"], "tool_call_id": call["tool_call_id"], "request_id": request_id, "trace_id": trace_id, "tool_name": call["tool_name"], "status": call["status"]}
            for call in tool_calls
        ],
        "diagnosis": diagnosis,
        "confirmed_facts": diagnosis["confirmed_facts"],
        "likely_causes": diagnosis["likely_causes"],
        "alternative_causes": diagnosis["alternative_causes"],
        "runbook_analysis": rag_result,
        "answer_grounding": answer_grounding,
        "evidence_reconciliation": evidence_reconciliation,
        "documentation_reconciliation": documentation_reconciliation,
        "report_consistency": report_consistency,
        "runbook_alignment": runbook_alignment,
        "runbook_lifecycle": lifecycle,
        "downstream_impact": downstream,
        "missing_evidence": missing_evidence,
        "recovery_readiness_gaps": recovery_readiness_gaps,
        "future_documentation_gaps": future_documentation_gaps,
        "safe_next_steps": safe_next_steps,
        "warnings": warnings,
        "evidence_validation": primary_identity_validation,
        "evidence_scope": _build_evidence_scope(evidence, pipeline_update_id),
        "tool_failures": [call for call in tool_calls if call.get("status") not in SUCCESS_STATUSES],
        "human_approval": {
            "required_before_any_change": True,
            "status": "NOT_REQUESTED",
            "approval_records_supported": True,
            "agent_can_execute_recovery": False,
            "execution_status": "NOT_EXECUTED",
        },
        "evidence": evidence,
        "execution_policy": {
            "execution_mode": "SERIAL",
            "tool_boundary": "DAY25_GOVERNED_REGISTRY_ONLY",
            "read_only": True,
            "automatic_remediation": False,
            "retries_enabled": False,
            "enforced_timeouts": False,
            "durable_persistence": False,
                "durable_evidence_persistence": evidence_persist_callback is not None,
        },
    }


def run_registered_investigation(
    *,
    run_id: int,
    workspace_client: Any,
    spark: Any,
    runbook_top_k: int = DEFAULT_RUNBOOK_TOP_K,
    request_id: Optional[str] = None,
    request_source: str = "MANUAL",
    persist: bool = True,
    tool_callable_wrapper: Optional[
        Callable[[str, Callable[..., Any]], Callable[..., Any]]
    ] = None,
    evidence_persist_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Execute through the Day 25 registry with request-level idempotency.

    For duplicate event delivery, callers MUST pass the same stable request_id
    for each redelivery (for example ``job-failure:{run_id}``). When omitted,
    a new manual request ID is generated, intentionally creating a new attempt.

    Persistence uses only ``zomato_dev.ops_evidence``. Set persist=False only
    for controlled tests or an explicitly non-persistent diagnostic run.
    """
    from operations.agent_tools import (
        ZOMATO_JOB_ID,
        ZOMATO_PIPELINE_ID,
        get_agent_tool_registry,
    )

    from .persistence import (
        complete_investigation,
        get_investigation_request,
        mark_investigation_failed,
        repair_current_incident_from_completed_request,
        report_from_request_record,
        reserve_investigation,
    )

    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer.")
    if evidence_persist_callback is not None and not callable(evidence_persist_callback):
        raise TypeError("evidence_persist_callback must be callable when supplied.")
    effective_request_id = normalize_request_id(request_id or _new_manual_request_id())
    effective_trace_id = _new_trace_id()
    started_at = datetime.now(timezone.utc)
    registry = get_agent_tool_registry()

    # Optional observability hook: wrap only the existing governed callables.
    # The registry key set is preserved exactly; no new tool can be registered.
    if tool_callable_wrapper is not None:
        if not callable(tool_callable_wrapper):
            raise TypeError("tool_callable_wrapper must be callable.")

        wrapped_registry: Dict[str, Callable[..., Any]] = {}
        for tool_name, tool_function in registry.items():
            wrapped_tool = tool_callable_wrapper(tool_name, tool_function)
            if not callable(wrapped_tool):
                raise TypeError(
                    f"Observability wrapper returned a non-callable for {tool_name!r}."
                )
            wrapped_registry[tool_name] = wrapped_tool

        if set(wrapped_registry) != set(registry):
            raise RuntimeError(
                "Observability wrapping changed the governed tool registry keys."
            )

        registry = wrapped_registry

    if not persist:
        report = run_investigation(
            run_id=run_id,
            registry=registry,
            workspace_client=workspace_client,
            spark=spark,
            runbook_top_k=runbook_top_k,
            request_id=effective_request_id,
            trace_id=effective_trace_id,
            expected_job_id=ZOMATO_JOB_ID,
            expected_pipeline_id=ZOMATO_PIPELINE_ID,
            evidence_persist_callback=evidence_persist_callback,
        )
        report["execution_policy"]["durable_persistence"] = False
        report["execution_policy"]["durable_evidence_persistence"] = (
            evidence_persist_callback is not None
        )
        report["request_source"] = request_source
        return report

    reservation = reserve_investigation(
        spark,
        request_id=effective_request_id,
        incident_id=incident_id_for_run(run_id),
        run_id=run_id,
        trace_id=effective_trace_id,
        workflow_version=WORKFLOW_VERSION,
        request_source=request_source,
    )

    if not reservation["created"]:
        existing = reservation["record"]
        existing_status = str(existing.get("request_status", "UNKNOWN")).upper()
        if existing_status == "COMPLETED":
            # Recover the current-incident projection if the prior two-table
            # commit was interrupted after execution history was committed.
            repair_current_incident_from_completed_request(spark, record=existing)
            prior_report = report_from_request_record(existing) or {}
            prior_report["idempotency"] = {
                "request_id": effective_request_id,
                "duplicate_request": True,
                "replayed_existing_result": True,
                "new_investigation_executed": False,
            }
            return prior_report

        return {
            "schema_version": WORKFLOW_VERSION,
            "incident_id": incident_id_for_run(run_id),
            "request_id": effective_request_id,
            "trace_id": existing.get("trace_id"),
            "run_id": run_id,
            "workflow_execution_status": "NOT_STARTED",
            "investigation_status": "REQUEST_NOT_REEXECUTED",
            "evidence_completeness": "UNKNOWN",
            "termination_reason": "DUPLICATE_REQUEST_KEY",
            "request_status": existing_status,
            "idempotency": {
                "duplicate_request": True,
                "replayed_existing_result": False,
                "new_investigation_executed": False,
                "message": "This request_id already exists. Use a new request_id for an intentional new investigation.",
            },
            "human_approval": {
                "required_before_any_change": True,
                "status": "NOT_REQUESTED",
                "agent_can_execute_recovery": False,
            },
        }

    try:
        report = run_investigation(
            run_id=run_id,
            registry=registry,
            workspace_client=workspace_client,
            spark=spark,
            runbook_top_k=runbook_top_k,
            request_id=effective_request_id,
            trace_id=effective_trace_id,
            expected_job_id=ZOMATO_JOB_ID,
            expected_pipeline_id=ZOMATO_PIPELINE_ID,
            evidence_persist_callback=evidence_persist_callback,
        )
    except Exception as exc:
        mark_investigation_failed(
            spark,
            request_id=effective_request_id,
            trace_id=effective_trace_id,
            error=exc,
        )
        raise

    report["request_source"] = request_source
    report["execution_policy"]["durable_persistence"] = True
    report["execution_policy"]["durable_evidence_persistence"] = (
        evidence_persist_callback is not None
    )
    report["execution_policy"]["request_idempotency"] = "REQUEST_ID_KEYED_DELTA_RESERVATION"
    report["execution_policy"]["recovery_execution"] = "HUMAN_ONLY_NOT_AVAILABLE_TO_AGENT"
    report["idempotency"] = {
        "request_id": effective_request_id,
        "duplicate_request": False,
        "replayed_existing_result": False,
        "new_investigation_executed": True,
    }
    report["timestamps"] = {"started_at": started_at.isoformat(), "completed_at": datetime.now(timezone.utc).isoformat()}

    # A completed investigation is persisted to execution history first, then
    # projected into current incident state. The method is replay-repairable.
    complete_investigation(
        spark,
        request_id=effective_request_id,
        trace_id=effective_trace_id,
        report=report,
    )
    return report
