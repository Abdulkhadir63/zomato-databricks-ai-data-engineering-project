"""Incremental Markdown ingestion for the Zomato Operations Knowledge Base.

Run as a Databricks Python file task. Configure the Job for maximum concurrent
runs = 1 so two runs cannot replace the same document concurrently.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from markdown_it import MarkdownIt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_PREFIX = (
    "s3://databricks-zomato-ai-datalake/"
    "knowledge-base/source/kb_version=0.1.0/"
)
MANIFEST_URI = SOURCE_PREFIX + "document-manifest.json"
CATALOG = "zomato_dev"
SCHEMA = "ops_knowledge"
STATE_TABLE = f"{CATALOG}.{SCHEMA}.document_ingestion_state"
OPERATIONAL_TABLE = f"{CATALOG}.{SCHEMA}.parsed_document_elements"
EVALUATION_TABLE = f"{CATALOG}.{SCHEMA}.parsed_evaluation_document_elements"
PARSER_NAME = "markdown-it-py"
PARSER_IMPLEMENTATION_VERSION = "1.0.0"
MAX_DOCUMENT_BYTES = 10 * 1024 * 1024
MAX_TOTAL_SOURCE_BYTES = 50 * 1024 * 1024

logger = logging.getLogger("zomato_knowledge_markdown_ingestion")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
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

STATE_SCHEMA = StructType([
    StructField("document_id", StringType(), False),
    StructField("source_file", StringType(), True),
    StructField("source_uri", StringType(), True),
    StructField("document_version", StringType(), True),
    StructField("source_sha256", StringType(), True),
    StructField("parser_version", StringType(), True),
    StructField("manifest_record_sha256", StringType(), True),
    StructField("element_count", LongType(), True),
    StructField("attempt_status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("run_id", StringType(), False),
    StructField("attempted_at", TimestampType(), False),
])

# ---------------------------------------------------------------------------
# Source discovery and validation
# ---------------------------------------------------------------------------
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_hash(value: Dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).encode("utf-8")
    return sha256_bytes(encoded)


def read_one_binary(spark: SparkSession, uri: str) -> bytes:
    rows = (spark.read.format("binaryFile").load(uri)
            .select("path", "length", "content").limit(2).collect())
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one object at {uri}; found {len(rows)}")
    if rows[0]["content"] is None:
        raise ValueError(f"Object has no content: {uri}")
    return bytes(rows[0]["content"])


def read_manifest(spark: SparkSession) -> Dict[str, Any]:
    try:
        manifest = json.loads(read_one_binary(spark, MANIFEST_URI).decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("Manifest is not valid UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("Manifest is not valid JSON") from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("documents"), list):
        raise ValueError("Manifest must be an object containing a documents array")
    if not manifest["documents"]:
        raise ValueError("Manifest documents array is empty")

    seen_files, seen_ids = set(), set()
    for i, doc in enumerate(manifest["documents"]):
        if not isinstance(doc, dict):
            raise ValueError(f"Manifest document {i} is not an object")
        for key in ("file", "document_id", "sha256"):
            if not doc.get(key):
                raise ValueError(f"Manifest document {i} missing {key}")
        if doc["file"] in seen_files or doc["document_id"] in seen_ids:
            raise ValueError(f"Duplicate filename or document_id in manifest: {doc}")
        seen_files.add(doc["file"])
        seen_ids.add(doc["document_id"])
        digest = doc["sha256"]
        if (not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdefABCDEF" for c in digest)):
            raise ValueError(f"Invalid SHA-256 in manifest for {doc['file']}")
    return manifest


def discover_markdown(spark: SparkSession) -> Dict[str, bytes]:
    rows = (spark.read.format("binaryFile").load(SOURCE_PREFIX + "*.md")
            .select("path", "length", "content").collect())
    result, total = {}, 0
    for row in rows:
        filename = row["path"].rsplit("/", 1)[-1]
        size = int(row["length"] or 0)
        payload = bytes(row["content"])
        if size <= 0 or size > MAX_DOCUMENT_BYTES or len(payload) != size:
            raise ValueError(f"Invalid size/content for source object {row['path']}")
        if filename in result:
            raise ValueError(f"Duplicate source filename discovered: {filename}")
        result[filename] = payload
        total += size
    if total > MAX_TOTAL_SOURCE_BYTES:
        raise ValueError("Total Markdown source size exceeds configured safety limit")
    return result


def validate_inventory(docs: Sequence[Dict[str, Any]], sources: Dict[str, bytes]) -> None:
    expected = {d["file"] for d in docs if d["file"].lower().endswith(".md")}
    actual = set(sources)
    missing, unexpected = sorted(expected - actual), sorted(actual - expected)
    if missing or unexpected:
        raise ValueError(f"Manifest/S3 inventory mismatch. Missing={missing}; unexpected={unexpected}")

# ---------------------------------------------------------------------------
# Delta table setup and state management
# ---------------------------------------------------------------------------
def ensure_tables(spark: SparkSession) -> None:
    element_ddl = f"""(
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
    spark.sql(f"CREATE TABLE IF NOT EXISTS {OPERATIONAL_TABLE} {element_ddl}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {EVALUATION_TABLE} {element_ddl}")

    state_cols = {f.name for f in spark.table(STATE_TABLE).schema.fields}
    if "last_successful_manifest_record_sha256" not in state_cols:
        spark.sql(
            f"ALTER TABLE {STATE_TABLE} ADD COLUMNS "
            "(last_successful_manifest_record_sha256 STRING)"
        )
        logger.info("Added manifest-record hash to ingestion-state table")


def get_state(spark: SparkSession, document_id: str) -> Optional[Any]:
    rows = (spark.table(STATE_TABLE)
            .where(F.col("document_id") == document_id).limit(2).collect())
    if len(rows) > 1:
        raise ValueError(f"Duplicate state rows for {document_id}")
    return rows[0] if rows else None


def is_unchanged(state: Optional[Any], source_hash: str, doc_version: str,
                 parser_version: str, manifest_hash: str) -> bool:
    if state is None:
        return False
    return (
        state["last_attempt_status"] == "SUCCESS"
        and state["last_successful_source_sha256"] == source_hash
        and state["last_successful_document_version"] == doc_version
        and state["last_successful_parser_version"] == parser_version
        and state["last_successful_manifest_record_sha256"] == manifest_hash
    )


def upsert_state(spark: SparkSession, *, document: Dict[str, Any], source_uri: str,
                 source_hash: Optional[str], parser_version: str,
                 manifest_hash: Optional[str], element_count: Optional[int],
                 status: str, error: Optional[str], run_id: str) -> None:
    now = datetime.utcnow()
    row = {
        "document_id": document["document_id"], "source_file": document["file"],
        "source_uri": source_uri, "document_version": str(document.get("version") or ""),
        "source_sha256": source_hash, "parser_version": parser_version,
        "manifest_record_sha256": manifest_hash, "element_count": element_count,
        "attempt_status": status, "error_message": error[:4000] if error else None,
        "run_id": run_id, "attempted_at": now,
    }
    view = "state_update_" + uuid.uuid4().hex
    spark.createDataFrame([row], schema=STATE_SCHEMA).createOrReplaceTempView(view)
    try:
        spark.sql(f"""
        MERGE INTO {STATE_TABLE} t USING {view} s
        ON t.document_id = s.document_id
        WHEN MATCHED THEN UPDATE SET
          t.source_file = s.source_file,
          t.source_uri = s.source_uri,
          t.last_successful_document_version = CASE WHEN s.attempt_status='SUCCESS' THEN s.document_version ELSE t.last_successful_document_version END,
          t.last_successful_source_sha256 = CASE WHEN s.attempt_status='SUCCESS' THEN s.source_sha256 ELSE t.last_successful_source_sha256 END,
          t.last_successful_parser_version = CASE WHEN s.attempt_status='SUCCESS' THEN s.parser_version ELSE t.last_successful_parser_version END,
          t.last_successful_manifest_record_sha256 = CASE WHEN s.attempt_status='SUCCESS' THEN s.manifest_record_sha256 ELSE t.last_successful_manifest_record_sha256 END,
          t.last_successful_element_count = CASE WHEN s.attempt_status='SUCCESS' THEN s.element_count ELSE t.last_successful_element_count END,
          t.last_successful_at = CASE WHEN s.attempt_status='SUCCESS' THEN s.attempted_at ELSE t.last_successful_at END,
          t.last_attempted_document_version = s.document_version,
          t.last_attempted_source_sha256 = s.source_sha256,
          t.last_attempted_parser_version = s.parser_version,
          t.last_attempt_status = s.attempt_status,
          t.last_attempt_at = s.attempted_at,
          t.last_error = s.error_message,
          t.last_ingestion_run_id = s.run_id,
          t.updated_at = s.attempted_at
        WHEN NOT MATCHED THEN INSERT (
          document_id, source_file, source_uri,
          last_successful_document_version, last_successful_source_sha256,
          last_successful_parser_version, last_successful_manifest_record_sha256,
          last_successful_element_count, last_successful_at,
          last_attempted_document_version, last_attempted_source_sha256,
          last_attempted_parser_version, last_attempt_status, last_attempt_at,
          last_error, last_ingestion_run_id, updated_at
        ) VALUES (
          s.document_id, s.source_file, s.source_uri,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.document_version ELSE NULL END,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.source_sha256 ELSE NULL END,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.parser_version ELSE NULL END,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.manifest_record_sha256 ELSE NULL END,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.element_count ELSE NULL END,
          CASE WHEN s.attempt_status='SUCCESS' THEN s.attempted_at ELSE NULL END,
          s.document_version, s.source_sha256, s.parser_version,
          s.attempt_status, s.attempted_at, s.error_message, s.run_id, s.attempted_at
        )
        """)
    finally:
        spark.catalog.dropTempView(view)

# ---------------------------------------------------------------------------
# Markdown token parsing and element extraction
# ---------------------------------------------------------------------------
def split_top_level_blocks(tokens: Sequence[Any]) -> Iterable[List[Any]]:
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.nesting == 1:
            depth, end = 0, None
            for j in range(i, len(tokens)):
                depth += tokens[j].nesting
                if depth == 0:
                    end = j
                    break
            if end is None:
                raise ValueError(f"Unbalanced Markdown tokens at position {i}")
            yield list(tokens[i:end + 1])
            i = end + 1
        else:
            if token.type in ("fence", "hr", "html_block"):
                yield [token]
            i += 1


def block_lines(block: Sequence[Any], lines: Sequence[str]) -> Tuple[Optional[int], Optional[int], str]:
    mapped = [t for t in block if t.map is not None]
    if not mapped:
        return None, None, ""
    start0 = min(t.map[0] for t in mapped)
    end_exclusive = max(t.map[1] for t in mapped)
    return start0 + 1, end_exclusive, "\n".join(lines[start0:end_exclusive])


def table_json(block: Sequence[Any]) -> str:
    rows: List[List[str]] = []
    row: Optional[List[str]] = None
    cell: Optional[List[str]] = None
    for token in block:
        if token.type == "tr_open":
            row = []
        elif token.type in ("th_open", "td_open"):
            cell = []
        elif token.type == "inline" and cell is not None:
            cell.append(token.content)
        elif token.type in ("th_close", "td_close"):
            if row is None or cell is None:
                raise ValueError("Malformed table token sequence")
            row.append(" ".join(cell).strip())
            cell = None
        elif token.type == "tr_close" and row is not None:
            rows.append(row)
            row = None
    return json.dumps({"headers": rows[0] if rows else [], "rows": rows[1:]}, ensure_ascii=False)


def stable_element_id(document_id: str, doc_version: str, source_hash: str,
                      parser_version: str, element_type: str,
                      line_start: Optional[int], line_end: Optional[int],
                      content: str) -> str:
    identity = {
        "document_id": document_id, "document_version": doc_version,
        "source_sha256": source_hash, "parser_version": parser_version,
        "element_type": element_type, "line_start": line_start,
        "line_end": line_end, "content": content,
    }
    return sha256_bytes(json.dumps(identity, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":")).encode("utf-8"))


def parse_document(*, text: str, doc: Dict[str, Any], source_uri: str,
                   source_hash: str, manifest_hash: str, parser_version: str,
                   library_version: str, run_id: str) -> List[Dict[str, Any]]:
    parser = MarkdownIt("commonmark").enable("table")
    tokens = parser.parse(text)
    lines = text.splitlines()
    parsed_at = datetime.utcnow()

    doc_id = doc["document_id"]
    doc_version = str(doc.get("version") or "")
    doc_type = doc.get("document_type")
    retrieval_eligible = bool(doc.get("production_retrieval_eligible", False))
    heading_stack: List[Dict[str, Any]] = []
    output: List[Dict[str, Any]] = []

    # The recovery runbook uses a three-line Pandoc title block. Preserve it as
    # document metadata and do not misclassify it as an operational paragraph.
    pandoc_title = len(lines) >= 3 and all(line.startswith("% ") for line in lines[:3])
    if pandoc_title:
        content = "\n".join(lines[:3])
        output.append(make_record(
            doc, source_uri, source_hash, manifest_hash, parser_version,
            library_version, run_id, parsed_at, "document_metadata", content,
            1, 3, None, None, None, None,
        ))

    for block in split_top_level_blocks(tokens):
        first = block[0]
        kind = first.type
        line_start, line_end, raw = block_lines(block, lines)
        if pandoc_title and kind == "paragraph_open" and line_start == 1 and line_end == 3:
            continue

        inline = [t for t in block if t.type == "inline"]
        heading_level = None
        heading_text = None
        structured = None
        code_language = None

        if kind == "heading_open":
            element_type = "heading"
            heading_level = int(first.tag[1:])
            if not inline:
                raise ValueError(f"Heading missing text at line {line_start}")
            heading_text = inline[0].content.strip()
            content = heading_text
            while heading_stack and heading_stack[-1]["level"] >= heading_level:
                heading_stack.pop()
            heading_stack.append({"level": heading_level, "text": heading_text})
        elif kind == "paragraph_open":
            element_type = "paragraph"
            content = inline[0].content.strip() if inline else raw.strip()
        elif kind == "blockquote_open":
            element_type, content = "blockquote", raw.strip()
        elif kind == "bullet_list_open":
            element_type, content = "unordered_list", raw.strip()
        elif kind == "ordered_list_open":
            element_type, content = "ordered_list", raw.strip()
        elif kind == "table_open":
            element_type, content = "table", raw.strip()
            structured = table_json(block)
        elif kind == "fence":
            element_type, content = "code_block", first.content
            code_language = (first.info or "").strip() or None
        elif kind == "hr":
            element_type, content = "horizontal_rule", raw.strip() or "---"
        elif kind == "html_block":
            element_type, content = "html_block", raw.strip()
        else:
            element_type, content = "other_block", raw.strip()

        if not content:
            continue
        section_path = " > ".join(x["text"] for x in heading_stack) or None
        output.append(make_record(
            doc, source_uri, source_hash, manifest_hash, parser_version,
            library_version, run_id, parsed_at, element_type, content,
            line_start, line_end, section_path, heading_level, heading_text,
            structured, code_language,
        ))

    if not output:
        raise ValueError(f"Parser returned zero elements for {doc['file']}")
    return output


def make_record(doc: Dict[str, Any], source_uri: str, source_hash: str,
                manifest_hash: str, parser_version: str, library_version: str,
                run_id: str, parsed_at: datetime, element_type: str, content: str,
                line_start: Optional[int], line_end: Optional[int],
                section_path: Optional[str], heading_level: Optional[int],
                heading_text: Optional[str], structured: Optional[str],
                code_language: Optional[str] = None) -> Dict[str, Any]:
    doc_version = str(doc.get("version") or "")
    doc_id = doc["document_id"]
    element_id = stable_element_id(doc_id, doc_version, source_hash,
                                   parser_version, element_type,
                                   line_start, line_end, content)
    return {
        "element_id": element_id,
        "document_id": doc_id,
        "source_file": doc["file"],
        "document_version": doc_version,
        "document_type": doc.get("document_type"),
        "component": doc.get("component"),
        "document_status": doc.get("status"),
        "evidence_state": doc.get("evidence_state"),
        "production_retrieval_eligible": bool(doc.get("production_retrieval_eligible", False)),
        "element_type": element_type,
        "heading_level": heading_level,
        "heading_text": heading_text,
        "section_path": section_path,
        "content": content,
        "structured_content": structured,
        "code_language": code_language,
        "source_uri": source_uri,
        "source_sha256": source_hash,
        "source_line_start": line_start,
        "source_line_end": line_end,
        "page_number": None,
        "parser_name": PARSER_NAME,
        "parser_version": parser_version,
        "parser_library_version": library_version,
        "manifest_record_sha256": manifest_hash,
        "ingestion_run_id": run_id,
        "parsed_at": parsed_at,
    }

# ---------------------------------------------------------------------------
# Validated Delta replacement
# ---------------------------------------------------------------------------
def replace_document_rows(spark: SparkSession, table: str, document_id: str,
                          records: List[Dict[str, Any]]) -> int:
    if not records:
        raise ValueError(f"Refusing to replace {document_id} with zero rows")
    if any(r["document_id"] != document_id for r in records):
        raise ValueError("Staged output contains more than one document_id")
    ids = [r["element_id"] for r in records]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate element IDs generated for {document_id}")

    staged = spark.createDataFrame(records, schema=ELEMENT_SCHEMA)
    if staged.where(F.col("document_id") != document_id).limit(1).count() > 0:
        raise ValueError("Staged DataFrame document_id validation failed")

    escaped_id = document_id.replace("'", "''")
    (staged.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"document_id = '{escaped_id}'")
     .saveAsTable(table))
    return len(records)

# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def run(spark: SparkSession) -> Dict[str, Any]:
    run_id = str(uuid.uuid4())
    library_version = importlib.metadata.version(PARSER_NAME)
    parser_version = f"{PARSER_IMPLEMENTATION_VERSION}|{PARSER_NAME}={library_version}"

    logger.info("Starting ingestion run_id=%s parser_version=%s", run_id, parser_version)
    ensure_tables(spark)

    manifest = read_manifest(spark)
    docs = [d for d in manifest["documents"] if d["file"].lower().endswith(".md")]
    sources = discover_markdown(spark)
    validate_inventory(docs, sources)

    summary = {
        "run_id": run_id, "discovered": len(sources), "processed": 0,
        "skipped": 0, "failed": 0, "elements_written": 0,
        "document_results": [],
    }

    for doc in docs:
        filename = doc["file"]
        document_id = doc["document_id"]
        source_uri = SOURCE_PREFIX + filename
        doc_version = str(doc.get("version") or "")
        payload = sources[filename]
        actual_hash = sha256_bytes(payload)
        manifest_hash = canonical_hash(doc)

        if actual_hash != doc["sha256"].lower():
            error = "SOURCE_HASH_MISMATCH: S3 bytes do not match manifest SHA-256"
            try:
                upsert_state(spark, document=doc, source_uri=source_uri,
                             source_hash=actual_hash, parser_version=parser_version,
                             manifest_hash=manifest_hash, element_count=None,
                             status="FAILED", error=error, run_id=run_id)
            except Exception:
                logger.exception("Unable to record hash mismatch for %s", document_id)
            summary["failed"] += 1
            summary["document_results"].append({"document_id": document_id, "status": "FAILED", "reason": error})
            logger.error("Hash mismatch for %s", document_id)
            continue

        state = get_state(spark, document_id)
        if is_unchanged(state, actual_hash, doc_version, parser_version, manifest_hash):
            summary["skipped"] += 1
            summary["document_results"].append({"document_id": document_id, "status": "SKIPPED_UNCHANGED"})
            logger.info("Unchanged document skipped: %s", document_id)
            continue

        try:
            try:
                source_text = payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(f"Source is not valid UTF-8: {filename}") from exc
            if not source_text.strip():
                raise ValueError(f"Source document is empty: {filename}")

            records = parse_document(
                text=source_text, doc=doc, source_uri=source_uri,
                source_hash=actual_hash, manifest_hash=manifest_hash,
                parser_version=parser_version, library_version=library_version,
                run_id=run_id,
            )
            table = EVALUATION_TABLE if str(doc.get("document_type", "")).lower() == "evaluation_tests" else OPERATIONAL_TABLE
            count = replace_document_rows(spark, table, document_id, records)

            # Update successful state only after the Delta replacement commits.
            upsert_state(spark, document=doc, source_uri=source_uri,
                         source_hash=actual_hash, parser_version=parser_version,
                         manifest_hash=manifest_hash, element_count=count,
                         status="SUCCESS", error=None, run_id=run_id)

            summary["processed"] += 1
            summary["elements_written"] += count
            summary["document_results"].append({
                "document_id": document_id, "status": "SUCCESS",
                "target_table": table, "element_count": count,
            })
            logger.info("Processed %s: %s elements -> %s", document_id, count, table)

        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)}"
            logger.exception("Processing failed for document_id=%s", document_id)
            try:
                upsert_state(spark, document=doc, source_uri=source_uri,
                             source_hash=actual_hash, parser_version=parser_version,
                             manifest_hash=manifest_hash, element_count=None,
                             status="FAILED", error=error, run_id=run_id)
            except Exception:
                logger.exception("Unable to record failed state for %s", document_id)
            summary["failed"] += 1
            summary["document_results"].append({
                "document_id": document_id, "status": "FAILED", "reason": error[:500],
            })

    logger.info(
        "Finished run_id=%s discovered=%s processed=%s skipped=%s failed=%s elements=%s",
        run_id, summary["discovered"], summary["processed"],
        summary["skipped"], summary["failed"], summary["elements_written"],
    )
    return summary


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    result = run(spark_session)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    if result["failed"] > 0:
        raise RuntimeError(
            f"Markdown ingestion had {result['failed']} failed document(s). "
            f"Run ID: {result['run_id']}"
        )
