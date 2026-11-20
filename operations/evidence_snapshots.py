from __future__ import annotations

import hashlib
import json
import re

from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

from pyspark.sql import SparkSession
from pyspark.sql import types as T


DEFAULT_CATALOG = "zomato_dev"
DEFAULT_SCHEMA = "ops_evidence"
TABLE_NAME = "incident_evidence_snapshots"

MAX_EVIDENCE_BYTES = 1_000_000


class EvidencePersistenceError(RuntimeError):
    """Evidence could not be durably persisted or verified."""


class EvidenceConflictError(EvidencePersistenceError):
    """An existing evidence identity contains different content."""


def _validate_identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", value
    ):
        raise ValueError(f"Invalid SQL identifier: {value!r}")

    return value


def _qualified_table(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> str:
    catalog = _validate_identifier(catalog)
    schema = _validate_identifier(schema)
    return f"`{catalog}`.`{schema}`.`{TABLE_NAME}`"


def _optional_int(value: Any, field_name: str) -> Optional[int]:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        raise ValueError(f"{field_name} cannot be boolean.")

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc


def _optional_text(value: Any, field_name: str) -> Optional[str]:
    if value is None or value == "":
        return None

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")

    return value[:500]


def _trigger_context(
    context: Optional[Mapping[str, Any]],
    *,
    mlflow_trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Allowlist only non-sensitive Job/task and trace-correlation metadata."""
    if context is None:
        context = {}

    if not isinstance(context, Mapping):
        raise TypeError("job_context must be a mapping.")

    context_trace_id = _optional_text(
        context.get("mlflow_trace_id"), "job_context.mlflow_trace_id"
    )
    supplied_trace_id = _optional_text(
        mlflow_trace_id, "mlflow_trace_id"
    )
    if (
        context_trace_id is not None
        and supplied_trace_id is not None
        and context_trace_id != supplied_trace_id
    ):
        raise EvidenceConflictError(
            "Conflicting MLflow trace IDs were supplied in checkpoint context."
        )

    return {
        "job_id": _optional_int(context.get("job_id"), "job_id"),
        "job_run_id": _optional_int(
            context.get("job_run_id"), "job_run_id"
        ),
        "failed_task_key": _optional_text(
            context.get("failed_task_key"), "failed_task_key"
        ),
        "failed_task_run_id": _optional_int(
            context.get("failed_task_run_id"), "failed_task_run_id"
        ),
        "failed_task_result_state": _optional_text(
            context.get("failed_task_result_state"),
            "failed_task_result_state",
        ),
        "failed_task_error_code": _optional_text(
            context.get("failed_task_error_code"),
            "failed_task_error_code",
        ),
        "pipeline_id": _optional_text(
            context.get("pipeline_id"), "pipeline_id"
        ),
        "pipeline_update_id": _optional_text(
            context.get("pipeline_update_id"), "pipeline_update_id"
        ),
        # Stored inside trigger_context_json, so no Delta schema alteration
        # is required for this trace-correlation field.
        "mlflow_trace_id": supplied_trace_id or context_trace_id,
    }


def ensure_evidence_snapshot_table(
    spark: SparkSession,
    *,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    """Create the evidence table once during handler/setup initialization.

    Call this before starting an investigation. ``persist_tool_evidence`` does
    not run DDL; it assumes this setup step has already succeeded.
    """
    table = _qualified_table(catalog, schema)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            evidence_id STRING NOT NULL,
            tool_call_id STRING NOT NULL,
            request_id STRING NOT NULL,
            incident_id STRING NOT NULL,
            run_id BIGINT NOT NULL,
            trace_id STRING NOT NULL,
            tool_name STRING NOT NULL,
            evidence_status STRING NOT NULL,
            evidence_json STRING NOT NULL,
            evidence_hash STRING NOT NULL,

            job_id BIGINT,
            job_run_id BIGINT,
            failed_task_key STRING,
            failed_task_run_id BIGINT,
            failed_task_result_state STRING,
            failed_task_error_code STRING,
            pipeline_id STRING,
            pipeline_update_id STRING,

            trigger_context_json STRING,
            captured_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )


def persist_tool_evidence(
    spark: SparkSession,
    *,
    request_id: str,
    incident_id: str,
    run_id: int,
    trace_id: str,
    tool_call_id: str,
    evidence_id: str,
    tool_name: str,
    evidence_result: Mapping[str, Any],
    job_context: Optional[Mapping[str, Any]] = None,
    mlflow_trace_id: Optional[str] = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, Any]:
    """Persist one already-sanitized governed tool result.

    Setup contract:
        ``ensure_evidence_snapshot_table`` must be called before this method.

    Merge key:
        ``tool_call_id``

    Existing records are immutable. Identical retries are accepted. Reusing a
    tool_call_id with different identity or content is rejected.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from .persistence import incident_id_for_run

    # ------------------------------------------------
    # 1. Validate and normalize identity
    # ------------------------------------------------
    required_strings = {
        "request_id": request_id,
        "incident_id": incident_id,
        "trace_id": trace_id,
        "tool_call_id": tool_call_id,
        "evidence_id": evidence_id,
        "tool_name": tool_name,
    }

    for field, value in required_strings.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string.")

    request_id = request_id.strip()
    incident_id = incident_id.strip()
    trace_id = trace_id.strip()
    tool_call_id = tool_call_id.strip()
    evidence_id = evidence_id.strip()
    tool_name = tool_name.strip()

    if (
        isinstance(run_id, bool)
        or not isinstance(run_id, int)
        or run_id <= 0
    ):
        raise ValueError("run_id must be a positive integer.")

    expected_incident = incident_id_for_run(run_id)
    if incident_id != expected_incident:
        raise EvidenceConflictError(
            "Incident ID does not match the Job run identity."
        )

    if not isinstance(evidence_result, Mapping):
        raise TypeError("evidence_result must be a sanitized mapping.")

    # ------------------------------------------------
    # 2. Normalize and fingerprint evidence
    # ------------------------------------------------
    try:
        payload = json.dumps(
            dict(evidence_result),
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise EvidencePersistenceError(
            "Evidence payload is not valid canonical JSON."
        ) from exc

    payload_size = len(payload.encode("utf-8"))
    if payload_size > MAX_EVIDENCE_BYTES:
        raise EvidencePersistenceError(
            f"Evidence payload exceeds the {MAX_EVIDENCE_BYTES}-byte limit."
        )

    evidence_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    status = evidence_result.get("status", "UNKNOWN")
    if not isinstance(status, str) or not status.strip():
        raise ValueError("Evidence status must be a non-empty string.")

    context = _trigger_context(
        job_context,
        mlflow_trace_id=mlflow_trace_id,
    )
    if (
        context["job_run_id"] is not None
        and context["job_run_id"] != run_id
    ):
        raise EvidenceConflictError(
            "Job run context does not match evidence run_id."
        )

    captured_at = datetime.now(timezone.utc)

    # ------------------------------------------------
    # 3. Use the explicitly qualified pre-created Delta table
    # ------------------------------------------------
    table_name = (
        f"{_validate_identifier(catalog)}."
        f"{_validate_identifier(schema)}."
        f"{TABLE_NAME}"
    )

    # ------------------------------------------------
    # 4. Construct an explicit Spark schema
    # ------------------------------------------------
    source_schema = T.StructType([
        T.StructField("evidence_id", T.StringType(), False),
        T.StructField("tool_call_id", T.StringType(), False),
        T.StructField("request_id", T.StringType(), False),
        T.StructField("incident_id", T.StringType(), False),
        T.StructField("run_id", T.LongType(), False),
        T.StructField("trace_id", T.StringType(), False),
        T.StructField("tool_name", T.StringType(), False),
        T.StructField("evidence_status", T.StringType(), False),
        T.StructField("evidence_json", T.StringType(), False),
        T.StructField("evidence_hash", T.StringType(), False),
        T.StructField("job_id", T.LongType(), True),
        T.StructField("job_run_id", T.LongType(), True),
        T.StructField("failed_task_key", T.StringType(), True),
        T.StructField("failed_task_run_id", T.LongType(), True),
        T.StructField("failed_task_result_state", T.StringType(), True),
        T.StructField("failed_task_error_code", T.StringType(), True),
        T.StructField("pipeline_id", T.StringType(), True),
        T.StructField("pipeline_update_id", T.StringType(), True),
        T.StructField("trigger_context_json", T.StringType(), True),
        T.StructField("captured_at", T.TimestampType(), False),
        T.StructField("created_at", T.TimestampType(), False),
    ])

    row = {
        "evidence_id": evidence_id,
        "tool_call_id": tool_call_id,
        "request_id": request_id,
        "incident_id": incident_id,
        "run_id": run_id,
        "trace_id": trace_id,
        "tool_name": tool_name,
        "evidence_status": status.strip().upper(),
        "evidence_json": payload,
        "evidence_hash": evidence_hash,
        "job_id": context["job_id"],
        "job_run_id": context["job_run_id"],
        "failed_task_key": context["failed_task_key"],
        "failed_task_run_id": context["failed_task_run_id"],
        "failed_task_result_state": context["failed_task_result_state"],
        "failed_task_error_code": context["failed_task_error_code"],
        "pipeline_id": context["pipeline_id"],
        "pipeline_update_id": context["pipeline_update_id"],
        "trigger_context_json": json.dumps(
            context,
            sort_keys=True,
            ensure_ascii=False,
        ),
        "captured_at": captured_at.replace(tzinfo=None),
        "created_at": captured_at.replace(tzinfo=None),
    }

    source_df = spark.createDataFrame([row], schema=source_schema)

    # ------------------------------------------------
    # 5. Immutable idempotency check and Delta MERGE
    # ------------------------------------------------
    existing_rows = (
        spark.table(table_name)
        .where(F.col("tool_call_id") == tool_call_id)
        .select(
            "evidence_id",
            "request_id",
            "incident_id",
            "run_id",
            "trace_id",
            "tool_name",
            "evidence_hash",
            "trigger_context_json",
        )
        .limit(2)
        .collect()
    )

    # Delta does not enforce a primary-key constraint. Detect an already
    # duplicated merge key instead of silently treating one row as canonical.
    if len(existing_rows) > 1:
        raise EvidenceConflictError(
            "Multiple rows already exist for this tool_call_id."
        )

    existing = existing_rows[0] if existing_rows else None

    if existing is not None:
        identity_matches = (
            existing["evidence_id"] == evidence_id
            and existing["request_id"] == request_id
            and existing["incident_id"] == incident_id
            and existing["run_id"] == run_id
            and existing["trace_id"] == trace_id
            and existing["tool_name"] == tool_name
            and existing["trigger_context_json"] == json.dumps(
                context,
                sort_keys=True,
                ensure_ascii=False,
            )
        )
        if not identity_matches:
            raise EvidenceConflictError(
                "Existing tool_call_id belongs to a different evidence identity."
            )

        if existing["evidence_hash"] != evidence_hash:
            raise EvidenceConflictError(
                "Existing evidence identity contains different content."
            )

        return {
            "persisted": True,
            "idempotent_replay": True,
            "tool_call_id": tool_call_id,
            "evidence_hash": evidence_hash,
            "table": table_name,
        }

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            "target.tool_call_id = source.tool_call_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # ------------------------------------------------
    # 6. Read-after-write verification
    # ------------------------------------------------
    saved_rows = (
        spark.table(table_name)
        .where(F.col("tool_call_id") == tool_call_id)
        .select(
            "evidence_id",
            "request_id",
            "incident_id",
            "run_id",
            "trace_id",
            "tool_name",
            "evidence_hash",
            "trigger_context_json",
        )
        .limit(2)
        .collect()
    )

    if not saved_rows:
        raise EvidencePersistenceError(
            "Evidence MERGE completed but record was not found."
        )
    if len(saved_rows) > 1:
        raise EvidenceConflictError(
            "Evidence MERGE resulted in multiple rows for one tool_call_id."
        )
    saved = saved_rows[0]

    verified = (
        saved["evidence_id"] == evidence_id
        and saved["request_id"] == request_id
        and saved["incident_id"] == incident_id
        and saved["run_id"] == run_id
        and saved["trace_id"] == trace_id
        and saved["tool_name"] == tool_name
        and saved["evidence_hash"] == evidence_hash
        and saved["trigger_context_json"] == json.dumps(
            context,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    if not verified:
        raise EvidenceConflictError(
            "Evidence read-after-write verification failed."
        )

    return {
        "persisted": True,
        "idempotent_replay": False,
        "tool_call_id": tool_call_id,
        "evidence_hash": evidence_hash,
        "table": table_name,
    }


def build_evidence_persist_callback(
    spark: SparkSession,
    *,
    job_context: Optional[Mapping[str, Any]] = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Callable[..., dict[str, Any]]:
    """Build the workflow callback after the evidence table is initialized.

    Expected setup sequence:
        1. ensure_evidence_snapshot_table(spark, catalog=..., schema=...)
        2. callback = build_evidence_persist_callback(spark, job_context=...)
        3. pass callback to run_traced_registered_investigation(...)

    The tracing adapter adds ``mlflow_trace_id`` to each callback invocation.
    """
    if job_context is not None and not isinstance(job_context, Mapping):
        raise TypeError("job_context must be a mapping when supplied.")

    def callback(**kwargs: Any) -> dict[str, Any]:
        return persist_tool_evidence(
            spark,
            job_context=job_context,
            catalog=catalog,
            schema=schema,
            **kwargs,
        )

    return callback
