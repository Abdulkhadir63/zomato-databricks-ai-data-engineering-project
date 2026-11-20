"""Day 19 PDF parsing validation for the Zomato Operations Knowledge Base.

This is an isolated validation job. It reads synthetic/test PDFs from a dedicated
S3 prefix and writes to parsed_pdf_document_elements, never to the Markdown
operational/evaluation tables. It uses Databricks ai_parse_document schema 2.0.

Prerequisites:
- Databricks serverless environment version 3+ (Free Edition is serverless-only).
- Workspace region supports ai_parse_document v2.
- The test PDF is uploaded to PDF_SOURCE_PREFIX.
- The executing principal can read that S3 prefix and create/write the Delta table.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

# ---------------------------------------------------------------------------
# Isolated test configuration. Do not point this at the Markdown source prefix.
# ---------------------------------------------------------------------------
PDF_SOURCE_PREFIX = (
    "s3://databricks-zomato-ai-datalake/"
    "knowledge-base/pdf-validation/"
)
CATALOG = "zomato_dev"
SCHEMA = "ops_knowledge"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.parsed_pdf_document_elements"
MAX_PDF_BYTES = 100 * 1024 * 1024  # ai_parse_document documented maximum
PARSER_VERSION = "2.0"
PARSER_NAME = "databricks.ai_parse_document"

logger = logging.getLogger("zomato_day19_pdf_validation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

ELEMENT_SCHEMA = StructType([
    StructField("element_id", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("document_version", StringType(), True),
    StructField("document_type", StringType(), True),
    StructField("component", StringType(), True),
    StructField("document_status", StringType(), True),
    StructField("evidence_state", StringType(), True),
    StructField("production_retrieval_eligible", BooleanType(), False),
    StructField("element_type", StringType(), False),
    StructField("heading_level", IntegerType(), True),
    StructField("heading_text", StringType(), True),
    StructField("section_path", StringType(), True),
    StructField("content", StringType(), False),
    StructField("structured_content", StringType(), True),
    StructField("code_language", StringType(), True),
    StructField("source_uri", StringType(), False),
    StructField("source_sha256", StringType(), False),
    StructField("source_line_start", LongType(), True),
    StructField("source_line_end", LongType(), True),
    StructField("page_number", LongType(), True),
    StructField("parser_name", StringType(), False),
    StructField("parser_version", StringType(), False),
    StructField("parser_library_version", StringType(), False),
    StructField("manifest_record_sha256", StringType(), False),
    StructField("ingestion_run_id", StringType(), False),
    StructField("parsed_at", TimestampType(), False),
])


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_hash(value: Dict[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_target_table(spark: SparkSession) -> None:
    ddl = """(
        element_id STRING NOT NULL, document_id STRING NOT NULL,
        source_file STRING NOT NULL, document_version STRING,
        document_type STRING, component STRING, document_status STRING,
        evidence_state STRING, production_retrieval_eligible BOOLEAN NOT NULL,
        element_type STRING NOT NULL, heading_level INT, heading_text STRING,
        section_path STRING, content STRING NOT NULL, structured_content STRING,
        code_language STRING, source_uri STRING NOT NULL, source_sha256 STRING NOT NULL,
        source_line_start BIGINT, source_line_end BIGINT, page_number BIGINT,
        parser_name STRING NOT NULL, parser_version STRING NOT NULL,
        parser_library_version STRING NOT NULL, manifest_record_sha256 STRING NOT NULL,
        ingestion_run_id STRING NOT NULL, parsed_at TIMESTAMP NOT NULL
    ) USING DELTA"""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} {ddl}")


def make_element_records(
    *,
    source_uri: str,
    source_file: str,
    source_sha256: str,
    parsed_payload: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    document_id = "ZOMATO-PDF-TEST-" + sha256_text(source_file)[:12].upper()
    document_version = "test-1.0"
    manifest_stub = {
        "document_id": document_id,
        "file": source_file,
        "version": document_version,
        "document_type": "pdf_validation_fixture",
        "source_sha256": source_sha256,
        "parser_version": PARSER_VERSION,
    }
    manifest_hash = canonical_hash(manifest_stub)
    parsed_at = datetime.now(timezone.utc)

    document = parsed_payload.get("document") or {}
    elements = document.get("elements") or []
    records: List[Dict[str, Any]] = []

    for fallback_index, element in enumerate(elements):
        if not isinstance(element, dict):
            continue
        element_index = element.get("id", fallback_index)
        element_type = str(element.get("type") or "unknown")
        raw_content = element.get("content")
        description = element.get("description")
        content = raw_content or description or f"[{element_type} element without extracted text]"

        bbox = element.get("bbox") or []
        page_ids = [
            int(box["page_id"])
            for box in bbox
            if isinstance(box, dict) and box.get("page_id") is not None
        ]
        # ai_parse_document bbox.page_id is 0-based; store human-readable pages 1-based.
        page_number = min(page_ids) + 1 if page_ids else None

        structured = {
            "source_element_id": element_index,
            "confidence": element.get("confidence"),
            "bbox": bbox,
            "description": description,
        }
        structured_json = json.dumps(structured, ensure_ascii=False, separators=(",", ":"))
        element_identity = {
            "document_id": document_id,
            "source_sha256": source_sha256,
            "parser_version": PARSER_VERSION,
            "element_index": element_index,
            "element_type": element_type,
            "page_number": page_number,
            "content": content,
        }
        element_id = canonical_hash(element_identity)

        records.append({
            "element_id": element_id,
            "document_id": document_id,
            "source_file": source_file,
            "document_version": document_version,
            "document_type": "pdf_validation_fixture",
            "component": "knowledge_ingestion_validation",
            "document_status": "TEST_ONLY",
            "evidence_state": "SYNTHETIC_FIXTURE",
            "production_retrieval_eligible": False,
            "element_type": element_type,
            "heading_level": None,
            "heading_text": None,
            "section_path": None,
            "content": str(content),
            "structured_content": structured_json,
            "code_language": None,
            "source_uri": source_uri,
            "source_sha256": source_sha256,
            "source_line_start": None,
            "source_line_end": None,
            "page_number": page_number,
            "parser_name": PARSER_NAME,
            "parser_version": PARSER_VERSION,
            "parser_library_version": "Databricks-managed",
            "manifest_record_sha256": manifest_hash,
            "ingestion_run_id": run_id,
            "parsed_at": parsed_at,
        })
    return records


def run(spark: SparkSession) -> Dict[str, Any]:
    run_id = str(uuid.uuid4())
    logger.info("Starting isolated PDF validation run_id=%s", run_id)
    ensure_target_table(spark)

    try:
        binary = (
            spark.read.format("binaryFile")
            .load(PDF_SOURCE_PREFIX + "*.pdf")
            .select("path", "length", "content")
        )
        source_rows = binary.select("path", "length").collect()
        discovered = len(source_rows)
    except Exception as exc:
        raise RuntimeError(
            "Could not read the PDF test prefix. Check S3 path and Unity Catalog access. "
            f"Source prefix: {PDF_SOURCE_PREFIX}. Original error: {type(exc).__name__}: {exc}"
        ) from exc

    if discovered == 0:
        raise RuntimeError(
            "No PDF files found. Upload day19_pdf_validation_fixture.pdf to "
            f"{PDF_SOURCE_PREFIX} and rerun."
        )

    # This is deliberately a small validation corpus, not a bulk production path.
    # Only file metadata is collected; document bytes remain in Spark executors.
    results = {
        "run_id": run_id,
        "discovered": len(source_rows),
        "processed": 0,
        "failed": 0,
        "elements_written": 0,
        "documents": [],
    }

    for source in source_rows:
        source_uri = source["path"]
        source_file = source_uri.rsplit("/", 1)[-1]
        file_length = int(source["length"] or 0)
        if file_length <= 0 or file_length > MAX_PDF_BYTES:
            results["failed"] += 1
            results["documents"].append({
                "source_file": source_file,
                "status": "FAILED",
                "reason": f"Invalid PDF byte length: {file_length}",
            })
            continue

        try:
            # Pin schema 2.0. Disable AI figure descriptions for this text/table test
            # to reduce unnecessary compute and cost.
            one_file = binary.where(F.col("path") == source_uri)
            parsed_json_row = (
                one_file.select(
                    F.sha2(F.col("content"), 256).alias("source_sha256"),
                    F.to_json(F.expr(
                        "ai_parse_document(content, map('version','2.0', 'descriptionElementTypes',''))"
                    )).alias("parsed_json"),
                ).collect()[0]
            )
            source_hash = parsed_json_row["source_sha256"]
            payload = json.loads(parsed_json_row["parsed_json"])
            document = payload.get("document") or {}
            pages = document.get("pages") or []
            elements = document.get("elements") or []
            page_errors = payload.get("error_status") or []

            if not pages:
                raise ValueError("Parser returned no page metadata")
            if not elements:
                raise ValueError("Parser returned no document elements")

            records = make_element_records(
                source_uri=source_uri,
                source_file=source_file,
                source_sha256=source_hash,
                parsed_payload=payload,
                run_id=run_id,
            )
            if not records:
                raise ValueError("No usable element records were generated")
            ids = [record["element_id"] for record in records]
            if len(ids) != len(set(ids)):
                raise ValueError("Duplicate PDF element IDs generated")

            staged = spark.createDataFrame(records, schema=ELEMENT_SCHEMA)
            document_id = records[0]["document_id"]
            escaped_id = document_id.replace("'", "''")
            (staged.write.format("delta").mode("overwrite")
             .option("replaceWhere", f"document_id = '{escaped_id}'")
             .saveAsTable(TARGET_TABLE))

            combined_content = "\n".join(record["content"] for record in records).upper()
            second_page_marker_found = "PAGE TWO MARKER" in combined_content
            table_count = sum(1 for record in records if record["element_type"] == "table")
            results["processed"] += 1
            results["elements_written"] += len(records)
            results["documents"].append({
                "document_id": document_id,
                "source_file": source_file,
                "status": "SUCCESS_WITH_PAGE_ERRORS" if page_errors else "SUCCESS",
                "page_count": len(pages),
                "element_count": len(records),
                "table_elements": table_count,
                "page_error_count": len(page_errors),
                "page_two_marker_found": second_page_marker_found,
            })
            logger.info(
                "Parsed PDF %s: pages=%s elements=%s tables=%s page_errors=%s",
                source_file, len(pages), len(records), table_count, len(page_errors),
            )
            if page_errors:
                logger.warning("PDF page-level errors for %s: %s", source_file, json.dumps(page_errors)[:2000])
            if not second_page_marker_found:
                logger.warning("Expected second-page marker was not found in extracted text: %s", source_file)

        except Exception as exc:
            results["failed"] += 1
            results["documents"].append({
                "source_file": source_file,
                "status": "FAILED",
                "reason": f"{type(exc).__name__}: {str(exc)[:1000]}",
            })
            logger.exception("PDF parsing failed for source_file=%s", source_file)

    logger.info("PDF validation finished: %s", json.dumps(results, ensure_ascii=False))
    return results


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    summary = run(spark_session)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    if summary["failed"] > 0:
        raise RuntimeError(
            f"PDF validation failed for {summary['failed']} file(s). Run ID: {summary['run_id']}"
        )
