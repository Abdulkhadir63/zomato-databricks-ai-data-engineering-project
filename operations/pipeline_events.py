"""Read-only Lakeflow pipeline event-log tools for the Zomato operations agent.

This module queries the Databricks event_log('<pipeline_id>') table function.
It does not start, stop, update, repair, or otherwise mutate a pipeline.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from typing import Any, Iterable


_TOOL_NAME = "get_pipeline_events"
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
_REQUIRED_COLUMNS = {
    "id",
    "sequence",
    "origin",
    "timestamp",
    "message",
    "level",
    "maturity_level",
    "error",
    "details",
    "event_type",
}
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 500
_MAX_TEXT_CHARS = 12_000


def _validate_uuid(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _UUID_RE.fullmatch(value.strip()):
        raise ValueError(f"{field_name} must be a valid UUID string.")
    return value.strip().lower()


def _parse_aware_datetime(value: str | datetime | None, field_name: str):
    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be ISO-8601, including a timezone offset; "
                "example: 2026-11-09T06:38:47Z."
            ) from exc
    else:
        raise TypeError(f"{field_name} must be an ISO-8601 string or datetime.")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            f"{field_name} must include a timezone offset. Naive timestamps are rejected."
        )
    return parsed.astimezone(timezone.utc)


def _epoch_micros(value: datetime) -> int:
    """Convert an aware datetime to an integer for timestamp_micros()."""
    return int(value.timestamp() * 1_000_000)


def _truncate_text(value: str, max_chars: int = _MAX_TEXT_CHARS) -> str:
    if len(value) <= max_chars:
        return value
    head_chars = max(1_000, max_chars // 4)
    tail_chars = max_chars - head_chars
    omitted = len(value) - max_chars
    return (
        value[:head_chars]
        + f"\n...[TRUNCATED {omitted} CHARACTERS; MIDDLE OMITTED]...\n"
        + value[-tail_chars:]
    )


def _native(value: Any) -> Any:
    """Convert Spark Rows and nested values into JSON-friendly Python values."""
    if value is None:
        return None
    if hasattr(value, "asDict"):
        return _native(value.asDict(recursive=True))
    if isinstance(value, dict):
        converted = {}
        for key, item in value.items():
            # Pipeline create_update events can contain a very large runtime
            # configuration map. It is not needed to investigate a failure.
            if key == "configuration" and isinstance(item, dict):
                converted[key] = {
                    "_omitted": "Large runtime configuration omitted from tool output."
                }
            else:
                converted[key] = _native(item)
        return converted
    if isinstance(value, (list, tuple)):
        return [_native(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return _truncate_text(value)
    # Spark Decimal, primitive values, and other JSON-compatible scalars.
    if isinstance(value, (bool, int, float)):
        return value
    return str(value)


def _parse_details(value: Any) -> Any:
    """Parse the event-log details JSON string when possible."""
    if isinstance(value, str):
        try:
            return _native(json.loads(value))
        except (json.JSONDecodeError, TypeError):
            return _native(value)
    return _native(value)


def _preserve_strings(value: Any) -> Any:
    """Convert Spark values to Python values without truncating diagnostic text."""
    if value is None:
        return None
    if hasattr(value, "asDict"):
        return _preserve_strings(value.asDict(recursive=True))
    if isinstance(value, dict):
        return {key: _preserve_strings(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_preserve_strings(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _extract_terminal_exception_message(message: str, class_name: str | None) -> str:
    """Extract the final exception line, excluding noisy encoded notebook source."""
    normalized = message.replace("\r\n", "\n").replace("\r", "\n")
    matches: list[str] = []

    if class_name:
        pattern = re.compile(
            rf"(?:^|\n)\s*{re.escape(str(class_name))}:\s*([^\n]+)",
            flags=re.MULTILINE,
        )
        matches = [match.group(1).strip() for match in pattern.finditer(normalized)]

    if not matches:
        generic = re.compile(
            r"(?:^|\n)\s*([A-Za-z_][\w.]*(?:Error|Exception)):\s*([^\n]+)",
            flags=re.MULTILINE,
        )
        generic_matches = list(generic.finditer(normalized))
        if generic_matches:
            matches = [generic_matches[-1].group(2).strip()]

    if matches:
        return _truncate_text(matches[-1], 2_000)

    meaningful_lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    fallback = meaningful_lines[-1] if meaningful_lines else normalized.strip()
    return _truncate_text(fallback, 2_000)


def _parse_error_payload(error_payload: Any) -> Any:
    """Normalize event error payloads across Spark Struct, mapping, list and JSON text.

    Invalid JSON strings are preserved as bounded text. They are not treated as
    structured exceptions, so the caller cannot accidentally invent a cause.
    """
    error_data = _preserve_strings(error_payload)

    if isinstance(error_data, str):
        normalized = error_data.strip()
        if not normalized:
            return None
        try:
            parsed = json.loads(normalized)
        except (json.JSONDecodeError, TypeError):
            return {"unparsed_error_text": _truncate_text(normalized, 2_000)}
        return _preserve_strings(parsed)

    if isinstance(error_data, list):
        return {"exceptions": error_data}

    if isinstance(error_data, dict):
        # Some event payloads may wrap the exception array in a list or
        # place the exception object itself at the top level.
        if "exceptions" not in error_data and (
            "class_name" in error_data or "message" in error_data
        ):
            return {"exceptions": [error_data]}
        return error_data

    return error_data


def _exception_summaries(error_payload: Any) -> list[dict[str, Any]]:
    """Return concise exception details, not notebook source or encoded traceback noise."""
    error_data = _parse_error_payload(error_payload)
    if isinstance(error_data, list):
        exceptions = error_data
    elif isinstance(error_data, dict):
        exceptions = error_data.get("exceptions") or []
    else:
        return []

    if isinstance(exceptions, dict):
        exceptions = [exceptions]
    if not isinstance(exceptions, list):
        return []

    summaries: list[dict[str, Any]] = []

    for exception in exceptions:
        if not isinstance(exception, dict):
            continue
        message = exception.get("message")
        class_name = exception.get("class_name")
        if isinstance(message, str):
            concise_message = _extract_terminal_exception_message(message, class_name)
            traceback_present = "Traceback (most recent call last)" in message
        else:
            concise_message = None
            traceback_present = False
        summaries.append(
            {
                "class_name": class_name,
                "exception_message": concise_message,
                # Retain this key for compatibility with the first module version.
                "message_tail": concise_message,
                "traceback_present": traceback_present,
                # Databricks may place the traceback in `message` and leave `stack` null.
                "stack_present": bool(exception.get("stack")),
            }
        )
    return summaries


def _normalized_error_payload(error_payload: Any) -> Any:
    """Expose compact, safe error evidence instead of returning encoded source text."""
    error_data = _parse_error_payload(error_payload)
    if error_data is None:
        return None
    if not isinstance(error_data, dict):
        return _native(error_data)

    normalized = {
        "fatal": error_data.get("fatal"),
        "exceptions": _exception_summaries(error_data),
    }
    if error_data.get("error_code") is not None:
        normalized["error_code"] = _native(error_data.get("error_code"))
    if error_data.get("unparsed_error_text"):
        normalized["unparsed_error_text"] = _truncate_text(
            str(error_data["unparsed_error_text"]), 2_000
        )
    return normalized


def _validate_event_types(event_types: Iterable[str] | None) -> list[str] | None:
    if event_types is None:
        return None
    if isinstance(event_types, str):
        raise TypeError("event_types must be a sequence of strings, not one string.")
    normalized = []
    for item in event_types:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("Each event_types entry must be a non-empty string.")
        normalized.append(item.strip())
    return normalized or None


def get_pipeline_events(
    spark,
    pipeline_id: str,
    *,
    update_id: str | None = None,
    job_task_run_id: int | str | None = None,
    start_time: str | datetime | None = None,
    end_time: str | datetime | None = None,
    event_types: Iterable[str] | None = None,
    level: str | None = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Retrieve read-only Lakeflow event evidence for one pipeline.

    Filters:
      - update_id: return events for one exact pipeline update.
      - job_task_run_id: correlate a Databricks Job task run to its pipeline
        update using details.create_update.job_task.run_id, then return all
        events for the correlated update(s).
      - start_time/end_time: optional inclusive-start, exclusive-end window.
      - event_types/level: optional event type and severity filters.

    Timestamp inputs must be timezone-aware ISO-8601 values or aware datetimes.
    The result is a dictionary suitable for an agent tool response.
    """
    pipeline_id = _validate_uuid(pipeline_id, "pipeline_id")
    if update_id is not None:
        update_id = _validate_uuid(update_id, "update_id")

    if job_task_run_id is not None:
        try:
            job_task_run_id = int(job_task_run_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("job_task_run_id must be a positive integer.") from exc
        if job_task_run_id <= 0:
            raise ValueError("job_task_run_id must be a positive integer.")

    start_dt = _parse_aware_datetime(start_time, "start_time")
    end_dt = _parse_aware_datetime(end_time, "end_time")
    if start_dt and end_dt and start_dt >= end_dt:
        raise ValueError("start_time must be earlier than end_time.")

    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError(f"limit must be an integer between 1 and {_MAX_LIMIT}.")

    normalized_event_types = _validate_event_types(event_types)
    normalized_level = level.strip().upper() if isinstance(level, str) else None
    if level is not None and (not normalized_level or len(normalized_level) > 32):
        raise ValueError("level must be a non-empty string of at most 32 characters.")

    try:
        from pyspark.sql import functions as F
    except ImportError as exc:  # pragma: no cover - only relevant outside Databricks
        raise RuntimeError("PySpark is required to query the Lakeflow event log.") from exc

    try:
        # pipeline_id is strictly UUID-validated above, so it cannot inject SQL.
        events_df = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")
        available_columns = set(events_df.columns)
        missing_columns = sorted(_REQUIRED_COLUMNS - available_columns)
        if missing_columns:
            return {
                "status": "ERROR",
                "tool": _TOOL_NAME,
                "pipeline": {"pipeline_id": pipeline_id},
                "error": "Unexpected Lakeflow event-log schema.",
                "missing_columns": missing_columns,
                "available_columns": sorted(available_columns),
            }

        matched_update_ids: list[str] = []
        if job_task_run_id is not None:
            # The Job task run ID is recorded on the create_update event. Resolve
            # that event first, then fetch every event bearing the update_id.
            # In this workspace, the event-log schema stores `details` as a
            # JSON STRING, not a STRUCT. Calling to_json(details) therefore
            # raises DATATYPE_MISMATCH. Read the JSON string directly. The
            # run_id appears under config.job_task in the observed event payload;
            # the other paths are compatibility fallbacks for alternate event
            # payload layouts.
            details_type = events_df.schema["details"].dataType
            from pyspark.sql.types import StringType

            if isinstance(details_type, StringType):
                details_json = F.col("details")
            else:
                details_json = F.to_json(F.col("details"))

            run_id_json = F.coalesce(
                F.get_json_object(
                    details_json, "$.create_update.config.job_task.run_id"
                ),
                F.get_json_object(
                    details_json,
                    "$.create_update.update_cause_details.job_details.run_id",
                ),
                F.get_json_object(
                    details_json, "$.create_update.job_task.run_id"
                ),
            )
            matches = (
                events_df
                .where(run_id_json == F.lit(str(job_task_run_id)))
                .select(F.col("origin.update_id").alias("update_id"))
                .where(F.col("update_id").isNotNull())
                .distinct()
                .limit(20)
                .collect()
            )
            matched_update_ids = sorted(
                {str(row["update_id"]).lower() for row in matches if row["update_id"]}
            )
            if not matched_update_ids:
                return {
                    "status": "NO_MATCH",
                    "tool": _TOOL_NAME,
                    "pipeline": {"pipeline_id": pipeline_id},
                    "query": {
                        "job_task_run_id": job_task_run_id,
                        "update_id": update_id,
                        "start_time": start_dt.isoformat() if start_dt else None,
                        "end_time": end_dt.isoformat() if end_dt else None,
                    },
                    "matched_update_ids": [],
                    "event_count": 0,
                    "error_event_count": 0,
                    "events": [],
                    "limitations": [
                        "No create_update event matched this Job task run ID."
                    ],
                }

        filtered = events_df
        if update_id is not None:
            filtered = filtered.where(F.col("origin.update_id") == F.lit(update_id))
        if matched_update_ids:
            filtered = filtered.where(F.col("origin.update_id").isin(matched_update_ids))
        if start_dt is not None:
            filtered = filtered.where(
                F.col("timestamp") >= F.expr(f"timestamp_micros({_epoch_micros(start_dt)})")
            )
        if end_dt is not None:
            filtered = filtered.where(
                F.col("timestamp") < F.expr(f"timestamp_micros({_epoch_micros(end_dt)})")
            )
        if normalized_event_types:
            filtered = filtered.where(F.col("event_type").isin(normalized_event_types))
        if normalized_level:
            filtered = filtered.where(F.upper(F.col("level")) == F.lit(normalized_level))

        rows = (
            filtered
            .orderBy(F.col("timestamp").desc())
            .limit(limit)
            .collect()
        )
        rows = sorted(
            rows,
            key=lambda row: (
                row["timestamp"] is None,
                row["timestamp"].timestamp() if row["timestamp"] is not None else 0,
            ),
        )

        event_records: list[dict[str, Any]] = []
        error_event_count = 0
        for row in rows:
            record = row.asDict(recursive=True)
            error_payload = record.get("error")
            if record.get("level") and str(record["level"]).upper() == "ERROR":
                error_event_count += 1
            elif error_payload is not None:
                error_event_count += 1

            event_records.append(
                {
                    "event_id": _native(record.get("id")),
                    "timestamp": _native(record.get("timestamp")),
                    "event_type": _native(record.get("event_type")),
                    "level": _native(record.get("level")),
                    "maturity_level": _native(record.get("maturity_level")),
                    "message": _native(record.get("message")),
                    "update_id": _native((record.get("origin") or {}).get("update_id")),
                    "origin": _native(record.get("origin")),
                    "error_summary": _exception_summaries(error_payload),
                    "error": _normalized_error_payload(error_payload),
                    "details": _parse_details(record.get("details")),
                    "sequence": _native(record.get("sequence")),
                }
            )

        try:
            session_timezone = spark.conf.get("spark.sql.session.timeZone")
        except Exception:
            session_timezone = "unknown"

        resolved_update_ids = matched_update_ids
        if update_id is not None and not resolved_update_ids:
            resolved_update_ids = [update_id]

        return {
            "status": "OK" if event_records else "NO_MATCH",
            "tool": _TOOL_NAME,
            "pipeline": {"pipeline_id": pipeline_id},
            "query": {
                "update_id": update_id,
                "job_task_run_id": job_task_run_id,
                "start_time": start_dt.isoformat() if start_dt else None,
                "end_time": end_dt.isoformat() if end_dt else None,
                "event_types": normalized_event_types,
                "level": normalized_level,
                "limit": limit,
                "timestamp_session_timezone": session_timezone,
            },
            "matched_update_ids": resolved_update_ids,
            "event_count": len(event_records),
            "limit_reached": len(event_records) == limit,
            "error_event_count": error_event_count,
            "events": event_records,
            "limitations": [
                "Results are limited to events returned by this pipeline's event_log table function.",
                "The tool is read-only and does not inspect external executor logs beyond the event payload.",
                "Large runtime configuration maps are omitted; long exception text is truncated while preserving its beginning and ending.",
                "If more than the requested limit matches, older matching events are not returned.",
            ],
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool": _TOOL_NAME,
            "pipeline": {"pipeline_id": pipeline_id},
            "query": {
                "update_id": update_id,
                "job_task_run_id": job_task_run_id,
                "start_time": start_dt.isoformat() if start_dt else None,
                "end_time": end_dt.isoformat() if end_dt else None,
            },
            "matched_update_ids": [],
            "event_count": 0,
            "error_event_count": 0,
            "events": [],
            "error": f"{type(exc).__name__}: {_truncate_text(str(exc), 4_000)}",
            "limitations": [
                "No pipeline state or data was modified by this tool.",
            ],
        }


__all__ = ["get_pipeline_events"]
