"""Idempotent Delta persistence for the Zomato Operations Intelligence Agent.

This module persists investigation execution history and the latest incident
report. It uses the existing Unity Catalog schema ``zomato_dev.ops_evidence``.

Important operating constraint:
- Delta tables do not enforce primary-key uniqueness.
- The caller must serialize writers for a given request_id/incident_id.
- MERGE provides idempotent upsert behavior for sequential executions; it is
  not a substitute for a distributed uniqueness/locking service.

No operational data, pipeline state, checkpoints, or business tables are
modified by this module. It writes only to the three ops_evidence tables.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional


DEFAULT_CATALOG = "zomato_dev"
DEFAULT_SCHEMA = "ops_evidence"

INVESTIGATION_RUNS_TABLE = "investigation_runs"
CURRENT_INCIDENTS_TABLE = "incident_reports_current"
APPROVAL_REQUESTS_TABLE = "approval_requests"

_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")


class PersistenceError(RuntimeError):
    """Base error for operational evidence persistence."""


class IdempotencyConflict(PersistenceError):
    """A request_id was reused for a different incident/run identity."""


class DuplicateKeyError(PersistenceError):
    """The target Delta table already contains duplicate logical keys."""


class MissingReservationError(PersistenceError):
    """Completion was attempted without an existing request reservation."""


def utc_now() -> datetime:
    """Return a UTC timestamp suitable for Spark TimestampType."""
    return datetime.now(timezone.utc)


def normalize_request_id(request_id: str) -> str:
    """Validate a caller-supplied idempotency key."""
    if not isinstance(request_id, str):
        raise ValueError("request_id must be a string.")
    value = request_id.strip()
    if not _REQUEST_ID_PATTERN.fullmatch(value):
        raise ValueError(
            "request_id must be 1-200 characters and contain only letters, "
            "numbers, dot, underscore, colon, slash or hyphen."
        )
    return value


def incident_id_for_run(run_id: int) -> str:
    """Return the existing stable project identity for a Databricks Job run."""
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer.")
    return f"zomato-jobrun:{run_id}"


def report_json_and_hash(report: Mapping[str, Any]) -> tuple[str, str]:
    """Serialize a JSON-compatible report deterministically and hash it."""
    payload = json.dumps(
        dict(report),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _qualified_table(name: str, catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{name}"


def ensure_operational_tables(
    spark: Any,
    *,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    """Create only the three project operational tables when absent."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    statements = [
        f"""CREATE TABLE IF NOT EXISTS {_qualified_table(INVESTIGATION_RUNS_TABLE, catalog, schema)} (
            request_id STRING,
            incident_id STRING,
            run_id BIGINT,
            trace_id STRING,
            workflow_version STRING,
            request_source STRING,
            request_status STRING,
            workflow_execution_status STRING,
            investigation_status STRING,
            evidence_completeness STRING,
            termination_reason STRING,
            report_json STRING,
            report_hash STRING,
            error_type STRING,
            error_message STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {_qualified_table(CURRENT_INCIDENTS_TABLE, catalog, schema)} (
            incident_id STRING,
            run_id BIGINT,
            job_id BIGINT,
            job_name STRING,
            pipeline_id STRING,
            incident_category STRING,
            incident_subtype STRING,
            workflow_execution_status STRING,
            investigation_status STRING,
            evidence_completeness STRING,
            latest_request_id STRING,
            latest_trace_id STRING,
            workflow_version STRING,
            report_json STRING,
            report_hash STRING,
            first_seen_at TIMESTAMP,
            last_investigated_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {_qualified_table(APPROVAL_REQUESTS_TABLE, catalog, schema)} (
            approval_id STRING,
            incident_id STRING,
            run_id BIGINT,
            request_id STRING,
            action_type STRING,
            recommended_action STRING,
            action_details_json STRING,
            action_fingerprint STRING,
            approval_status STRING,
            requested_by STRING,
            requested_at TIMESTAMP,
            reviewed_by STRING,
            reviewed_at TIMESTAMP,
            review_comment STRING,
            execution_status STRING,
            execution_reference STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA""",
    ]
    for statement in statements:
        spark.sql(statement)


def _fetch_by_key(
    spark: Any,
    *,
    table: str,
    key_column: str,
    key_value: str,
) -> Optional[Dict[str, Any]]:
    from pyspark.sql import functions as F

    rows = (
        spark.table(table)
        .where(F.col(key_column) == key_value)
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise DuplicateKeyError(
            f"{table} contains multiple rows for {key_column}={key_value!r}. "
            "Stop writes and repair the duplicate keys before continuing."
        )
    return rows[0].asDict(recursive=True) if rows else None


def get_investigation_request(
    spark: Any,
    request_id: str,
    *,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Optional[Dict[str, Any]]:
    request_id = normalize_request_id(request_id)
    table = _qualified_table(INVESTIGATION_RUNS_TABLE, catalog, schema)
    return _fetch_by_key(
        spark, table=table, key_column="request_id", key_value=request_id
    )


def _merge_insert_if_absent(
    spark: Any,
    *,
    table: str,
    row: Mapping[str, Any],
    key_column: str,
) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # Explicit schema is required because the first row contains nullable fields.
    schema_by_table = {
        INVESTIGATION_RUNS_TABLE: StructType([
            StructField("request_id", StringType(), True),
            StructField("incident_id", StringType(), True),
            StructField("run_id", LongType(), True),
            StructField("trace_id", StringType(), True),
            StructField("workflow_version", StringType(), True),
            StructField("request_source", StringType(), True),
            StructField("request_status", StringType(), True),
            StructField("workflow_execution_status", StringType(), True),
            StructField("investigation_status", StringType(), True),
            StructField("evidence_completeness", StringType(), True),
            StructField("termination_reason", StringType(), True),
            StructField("report_json", StringType(), True),
            StructField("report_hash", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]),
        CURRENT_INCIDENTS_TABLE: StructType([
            StructField("incident_id", StringType(), True),
            StructField("run_id", LongType(), True),
            StructField("job_id", LongType(), True),
            StructField("job_name", StringType(), True),
            StructField("pipeline_id", StringType(), True),
            StructField("incident_category", StringType(), True),
            StructField("incident_subtype", StringType(), True),
            StructField("workflow_execution_status", StringType(), True),
            StructField("investigation_status", StringType(), True),
            StructField("evidence_completeness", StringType(), True),
            StructField("latest_request_id", StringType(), True),
            StructField("latest_trace_id", StringType(), True),
            StructField("workflow_version", StringType(), True),
            StructField("report_json", StringType(), True),
            StructField("report_hash", StringType(), True),
            StructField("first_seen_at", TimestampType(), True),
            StructField("last_investigated_at", TimestampType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]),
        APPROVAL_REQUESTS_TABLE: StructType([
            StructField("approval_id", StringType(), True),
            StructField("incident_id", StringType(), True),
            StructField("run_id", LongType(), True),
            StructField("request_id", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("recommended_action", StringType(), True),
            StructField("action_details_json", StringType(), True),
            StructField("action_fingerprint", StringType(), True),
            StructField("approval_status", StringType(), True),
            StructField("requested_by", StringType(), True),
            StructField("requested_at", TimestampType(), True),
            StructField("reviewed_by", StringType(), True),
            StructField("reviewed_at", TimestampType(), True),
            StructField("review_comment", StringType(), True),
            StructField("execution_status", StringType(), True),
            StructField("execution_reference", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]),
    }
    table_name = table.rsplit(".", 1)[-1]
    source_df = spark.createDataFrame([dict(row)], schema=schema_by_table[table_name])
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(source_df.alias("source"), f"target.{key_column} = source.{key_column}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def reserve_investigation(
    spark: Any,
    *,
    request_id: str,
    incident_id: str,
    run_id: int,
    trace_id: str,
    workflow_version: str,
    request_source: str = "MANUAL",
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Reserve a logical request before executing any investigation tools.

    Repeated request_id returns the existing record and must not execute tools
    again. A new request_id creates a new execution-history row.
    """
    request_id = normalize_request_id(request_id)
    expected_incident_id = incident_id_for_run(run_id)
    if incident_id != expected_incident_id:
        raise IdempotencyConflict(
            "incident_id does not match the established Job-run identity."
        )
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise ValueError("trace_id must be a non-empty string.")

    ensure_operational_tables(spark, catalog=catalog, schema=schema)
    table = _qualified_table(INVESTIGATION_RUNS_TABLE, catalog, schema)
    existing = _fetch_by_key(
        spark, table=table, key_column="request_id", key_value=request_id
    )
    if existing:
        if existing.get("incident_id") != incident_id or existing.get("run_id") != run_id:
            raise IdempotencyConflict(
                "request_id was previously used for a different incident/run. "
                "Use a new request_id for a new logical request."
            )
        return {"created": False, "record": existing}

    now = utc_now()
    row = {
        "request_id": request_id,
        "incident_id": incident_id,
        "run_id": int(run_id),
        "trace_id": trace_id,
        "workflow_version": workflow_version,
        "request_source": request_source,
        "request_status": "IN_PROGRESS",
        "workflow_execution_status": "RUNNING",
        "investigation_status": "NOT_STARTED",
        "evidence_completeness": "UNKNOWN",
        "termination_reason": None,
        "report_json": None,
        "report_hash": None,
        "error_type": None,
        "error_message": None,
        "started_at": now,
        "completed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    _merge_insert_if_absent(
        spark, table=table, row=row, key_column="request_id"
    )
    saved = _fetch_by_key(
        spark, table=table, key_column="request_id", key_value=request_id
    )
    if not saved:
        raise PersistenceError("Request reservation could not be read after MERGE.")
    if saved.get("incident_id") != incident_id or saved.get("run_id") != run_id:
        raise IdempotencyConflict("A concurrent writer reserved request_id for another run.")
    return {"created": saved.get("trace_id") == trace_id, "record": saved}


def _current_incident_row(
    report: Mapping[str, Any],
    *,
    request_id: str,
    trace_id: str,
    completed_at: datetime,
    existing_created_at: Optional[datetime],
) -> Dict[str, Any]:
    report_payload, report_hash = report_json_and_hash(report)
    return {
        "incident_id": report["incident_id"],
        "run_id": int(report["run_id"]),
        "job_id": report.get("job_id"),
        "job_name": report.get("job_name"),
        "pipeline_id": report.get("pipeline_id"),
        "incident_category": report.get("incident_category"),
        "incident_subtype": report.get("incident_subtype"),
        "workflow_execution_status": report.get("workflow_execution_status"),
        "investigation_status": report.get("investigation_status"),
        "evidence_completeness": report.get("evidence_completeness"),
        "latest_request_id": request_id,
        "latest_trace_id": trace_id,
        "workflow_version": report.get("schema_version"),
        "report_json": report_payload,
        "report_hash": report_hash,
        "first_seen_at": existing_created_at or completed_at,
        "last_investigated_at": completed_at,
        "created_at": existing_created_at or completed_at,
        "updated_at": completed_at,
    }


def _upsert_current_incident(
    spark: Any,
    *,
    report: Mapping[str, Any],
    request_id: str,
    trace_id: str,
    completed_at: datetime,
    catalog: str,
    schema: str,
) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    table = _qualified_table(CURRENT_INCIDENTS_TABLE, catalog, schema)
    existing = _fetch_by_key(
        spark, table=table, key_column="incident_id", key_value=str(report["incident_id"])
    )
    row = _current_incident_row(
        report,
        request_id=request_id,
        trace_id=trace_id,
        completed_at=completed_at,
        existing_created_at=existing.get("created_at") if existing else None,
    )
    schema_def = StructType([
        StructField("incident_id", StringType(), True),
        StructField("run_id", LongType(), True),
        StructField("job_id", LongType(), True),
        StructField("job_name", StringType(), True),
        StructField("pipeline_id", StringType(), True),
        StructField("incident_category", StringType(), True),
        StructField("incident_subtype", StringType(), True),
        StructField("workflow_execution_status", StringType(), True),
        StructField("investigation_status", StringType(), True),
        StructField("evidence_completeness", StringType(), True),
        StructField("latest_request_id", StringType(), True),
        StructField("latest_trace_id", StringType(), True),
        StructField("workflow_version", StringType(), True),
        StructField("report_json", StringType(), True),
        StructField("report_hash", StringType(), True),
        StructField("first_seen_at", TimestampType(), True),
        StructField("last_investigated_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    source_df = spark.createDataFrame([row], schema=schema_def)
    update_columns = [c for c in row if c not in {"incident_id", "first_seen_at", "created_at"}]
    update_set = {column: f"source.{column}" for column in update_columns}
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(source_df.alias("source"), "target.incident_id = source.incident_id")
        .whenMatchedUpdate(
            condition="source.last_investigated_at >= target.last_investigated_at",
            set=update_set,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def complete_investigation(
    spark: Any,
    *,
    request_id: str,
    trace_id: str,
    report: Mapping[str, Any],
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> Dict[str, Any]:
    """Persist a completed investigation without allowing report overwrites.

    Identical repeated completion is idempotent.
    A different report for an already completed request is rejected.
    The current-incident projection is repaired on identical replay.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    request_id = normalize_request_id(request_id)
    incident_id = str(report.get("incident_id") or "")
    run_id = report.get("run_id")

    if not incident_id or isinstance(run_id, bool) or not isinstance(run_id, int):
        raise ValueError(
            "Completed report must contain incident_id and integer run_id."
        )

    if incident_id != incident_id_for_run(run_id):
        raise IdempotencyConflict(
            "Completed report incident_id/run_id mismatch."
        )

    ensure_operational_tables(spark, catalog=catalog, schema=schema)

    history_table = _qualified_table(
        INVESTIGATION_RUNS_TABLE, catalog, schema
    )

    existing = _fetch_by_key(
        spark,
        table=history_table,
        key_column="request_id",
        key_value=request_id,
    )

    if not existing:
        raise MissingReservationError(
            "Cannot complete an investigation without its request reservation."
        )

    if existing.get("trace_id") != trace_id:
        raise IdempotencyConflict(
            "trace_id does not match the reserved request."
        )

    if (
        existing.get("incident_id") != incident_id
        or existing.get("run_id") != run_id
    ):
        raise IdempotencyConflict(
            "Completed report does not match the reserved incident/run."
        )

    report_payload, report_hash = report_json_and_hash(report)

    existing_status = str(
        existing.get("request_status", "")
    ).upper()

    # Identical completed replay: preserve history and repair projection.
    if existing_status == "COMPLETED":
        if existing.get("report_hash") != report_hash:
            raise IdempotencyConflict(
                "Completed request already contains a different report. "
                "The stored report cannot be overwritten."
            )

        completed_at = (
            existing.get("completed_at")
            or existing.get("updated_at")
            or utc_now()
        )

        _upsert_current_incident(
            spark,
            report=report,
            request_id=request_id,
            trace_id=trace_id,
            completed_at=completed_at,
            catalog=catalog,
            schema=schema,
        )

        return {
            "request_status": "COMPLETED",
            "report_hash": report_hash,
            "completed_at": (
                completed_at.isoformat()
                if hasattr(completed_at, "isoformat")
                else str(completed_at)
            ),
            "idempotent_repeat": True,
        }

    # Only an in-progress reservation can transition to completed.
    if existing_status != "IN_PROGRESS":
        raise IdempotencyConflict(
            f"Cannot complete request from state {existing_status!r}. "
            "Only IN_PROGRESS requests can be completed."
        )

    completed_at = utc_now()

    update_values = {
        "request_status": F.lit("COMPLETED"),
        "workflow_execution_status": F.lit(
            str(report.get("workflow_execution_status", "COMPLETED"))
        ),
        "investigation_status": F.lit(
            str(report.get("investigation_status", "UNKNOWN"))
        ),
        "evidence_completeness": F.lit(
            str(report.get("evidence_completeness", "UNKNOWN"))
        ),
        "termination_reason": F.lit(report.get("termination_reason")),
        "report_json": F.lit(report_payload),
        "report_hash": F.lit(report_hash),
        "error_type": F.lit(None).cast("string"),
        "error_message": F.lit(None).cast("string"),
        "completed_at": F.lit(completed_at).cast("timestamp"),
        "updated_at": F.lit(completed_at).cast("timestamp"),
    }

    # Conditional state transition protects against stale writers.
    DeltaTable.forName(spark, history_table).update(
        condition=(
            (F.col("request_id") == request_id)
            & (F.col("trace_id") == trace_id)
            & (F.col("request_status") == "IN_PROGRESS")
        ),
        set=update_values,
    )

    # Verify the committed state before updating the current projection.
    saved = _fetch_by_key(
        spark,
        table=history_table,
        key_column="request_id",
        key_value=request_id,
    )

    if not saved:
        raise PersistenceError(
            "Investigation record disappeared after completion update."
        )

    saved_status = str(
        saved.get("request_status", "")
    ).upper()

    if saved_status == "COMPLETED" and saved.get("report_hash") != report_hash:
        raise IdempotencyConflict(
            "Another completion committed a different report for this request."
        )

    if (
        saved_status != "COMPLETED"
        or saved.get("report_hash") != report_hash
    ):
        raise PersistenceError(
            "Investigation completion could not be verified."
        )

    _upsert_current_incident(
        spark,
        report=report,
        request_id=request_id,
        trace_id=trace_id,
        completed_at=completed_at,
        catalog=catalog,
        schema=schema,
    )

    return {
        "request_status": "COMPLETED",
        "report_hash": report_hash,
        "completed_at": completed_at.isoformat() + "Z",
        "idempotent_repeat": False,
    }


def mark_investigation_failed(
    spark: Any,
    *,
    request_id: str,
    trace_id: str,
    error: Exception,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    """Mark a reserved request failed without overwriting incident reports."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    request_id = normalize_request_id(request_id)
    table = _qualified_table(INVESTIGATION_RUNS_TABLE, catalog, schema)
    existing = _fetch_by_key(
        spark, table=table, key_column="request_id", key_value=request_id
    )
    if not existing:
        raise MissingReservationError("Cannot mark an unreserved request as failed.")
    if existing.get("trace_id") != trace_id:
        raise IdempotencyConflict("trace_id does not match the reserved request.")
    now = utc_now()
    DeltaTable.forName(spark, table).update(
        condition=F.col("request_id") == request_id,
        set={
            "request_status": F.lit("FAILED"),
            "workflow_execution_status": F.lit("FAILED"),
            "error_type": F.lit(type(error).__name__),
            "error_message": F.lit(str(error)[:1000]),
            "completed_at": F.lit(now).cast("timestamp"),
            "updated_at": F.lit(now).cast("timestamp"),
        },
    )


def report_from_request_record(record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    payload = record.get("report_json")
    if not isinstance(payload, str) or not payload.strip():
        return None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise PersistenceError("Stored investigation report_json is invalid.") from exc
    if not isinstance(value, dict):
        raise PersistenceError("Stored investigation report is not a JSON object.")
    return value


def repair_current_incident_from_completed_request(
    spark: Any,
    *,
    record: Mapping[str, Any],
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> bool:
    """Repair current-incident projection after a partial two-table commit."""
    report = report_from_request_record(record)
    if not report:
        return False
    completed_at = record.get("completed_at") or record.get("updated_at") or utc_now()
    _upsert_current_incident(
        spark,
        report=report,
        request_id=str(record["request_id"]),
        trace_id=str(record["trace_id"]),
        completed_at=completed_at,
        catalog=catalog,
        schema=schema,
    )
    return True
