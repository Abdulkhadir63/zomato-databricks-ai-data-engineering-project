"""structure-aware, version-aware Markdown document chunking.

Run as a separate Databricks Python file task/job, not as a Lakeflow
Declarative Pipeline source file.

Scope: operational Markdown elements already written to
zomato_dev.ops_knowledge.parsed_document_elements.
Evaluation elements are intentionally excluded.
"""

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

SOURCE_TABLE = "zomato_dev.ops_knowledge.parsed_document_elements"
CHUNK_TABLE = "zomato_dev.ops_knowledge.document_chunks"
STATE_TABLE = "zomato_dev.ops_knowledge.document_chunk_state"

# Change this value whenever chunking behavior changes semantically.
CHUNKER_VERSION = "1.0.0"

# Character-based safety limit, not a tokenizer-based token limit.
# Current runbook sections are much smaller than this limit.
MAX_CHUNK_CHARS = 3500
OVERLAP_MAX_CHARS = 300

# The current knowledge base is small. Fail closed rather than accidentally
# collecting a large future corpus onto the driver.
MAX_SOURCE_ELEMENTS = 100_000
MAX_SOURCE_CONTENT_CHARS = 20_000_000

SKIPPED_ELEMENT_TYPES = {"document_metadata", "heading", "horizontal_rule"}


# ---------------------------------------------------------------------
# Target schemas
# ---------------------------------------------------------------------

CHUNK_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CHUNK_TABLE} (
    chunk_id STRING NOT NULL,
    document_id STRING NOT NULL,
    document_title STRING,
    source_file STRING,
    source_uri STRING,
    document_version STRING NOT NULL,
    source_sha256 STRING NOT NULL,
    manifest_record_sha256 STRING,
    document_metadata_sha256 STRING NOT NULL,
    parser_version STRING,
    chunker_version STRING NOT NULL,
    section_path STRING,
    chunk_sequence INT,
    chunk_text STRING NOT NULL,
    content_hash STRING NOT NULL,
    element_ids ARRAY<STRING>,
    overlap_element_ids ARRAY<STRING>,
    source_line_start INT,
    source_line_end INT,
    page_number INT,
    element_count INT,
    character_count INT,
    is_oversized BOOLEAN,
    production_retrieval_eligible BOOLEAN,
    document_status STRING,
    evidence_state STRING,
    chunk_created_at TIMESTAMP
)
USING DELTA
"""

STATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    document_id STRING NOT NULL,
    active_document_version STRING,
    active_source_sha256 STRING,
    active_manifest_record_sha256 STRING,
    active_document_metadata_sha256 STRING,
    active_parser_version STRING,
    active_chunker_version STRING,
    active_chunk_count BIGINT,
    active_published_at TIMESTAMP,
    last_attempt_status STRING,
    last_attempt_at TIMESTAMP,
    last_error STRING
)
USING DELTA
"""

CHUNK_SCHEMA = T.StructType([
    T.StructField("chunk_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("document_title", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("source_uri", T.StringType(), True),
    T.StructField("document_version", T.StringType(), False),
    T.StructField("source_sha256", T.StringType(), False),
    T.StructField("manifest_record_sha256", T.StringType(), True),
    T.StructField("document_metadata_sha256", T.StringType(), False),
    T.StructField("parser_version", T.StringType(), True),
    T.StructField("chunker_version", T.StringType(), False),
    T.StructField("section_path", T.StringType(), True),
    T.StructField("chunk_sequence", T.IntegerType(), True),
    T.StructField("chunk_text", T.StringType(), False),
    T.StructField("content_hash", T.StringType(), False),
    T.StructField("element_ids", T.ArrayType(T.StringType(), False), False),
    T.StructField("overlap_element_ids", T.ArrayType(T.StringType(), False), False),
    T.StructField("source_line_start", T.IntegerType(), True),
    T.StructField("source_line_end", T.IntegerType(), True),
    T.StructField("page_number", T.IntegerType(), True),
    T.StructField("element_count", T.IntegerType(), False),
    T.StructField("character_count", T.IntegerType(), False),
    T.StructField("is_oversized", T.BooleanType(), False),
    T.StructField("production_retrieval_eligible", T.BooleanType(), False),
    T.StructField("document_status", T.StringType(), True),
    T.StructField("evidence_state", T.StringType(), True),
    T.StructField("chunk_created_at", T.TimestampType(), False),
])

STATE_SCHEMA = T.StructType([
    T.StructField("document_id", T.StringType(), False),
    T.StructField("active_document_version", T.StringType(), True),
    T.StructField("active_source_sha256", T.StringType(), True),
    T.StructField("active_manifest_record_sha256", T.StringType(), True),
    T.StructField("active_document_metadata_sha256", T.StringType(), True),
    T.StructField("active_parser_version", T.StringType(), True),
    T.StructField("active_chunker_version", T.StringType(), True),
    T.StructField("active_chunk_count", T.LongType(), True),
    T.StructField("active_published_at", T.TimestampType(), True),
    T.StructField("last_attempt_status", T.StringType(), True),
    T.StructField("last_attempt_at", T.TimestampType(), True),
    T.StructField("last_error", T.StringType(), True),
])


# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def first_non_empty(rows, column, default=None):
    values = []
    for row in rows:
        value = row.get(column)
        if value is not None and str(value).strip() != "":
            values.append(str(value).strip())
    unique_values = sorted(set(values))
    if len(unique_values) > 1:
        raise ValueError(
            f"Document metadata is inconsistent: {column} has {unique_values}"
        )
    return unique_values[0] if unique_values else default


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def extract_title(rows, source_file):
    # Prefer an explicit title column if the parser output contains one.
    explicit_title = first_non_empty(rows, "document_title", default=None)
    if explicit_title:
        return explicit_title

    # The existing runbook parser preserves Pandoc-style metadata lines.
    for row in rows:
        if row.get("element_type") == "document_metadata":
            lines = [line.strip() for line in (row.get("content") or "").splitlines()]
            metadata_values = [line.lstrip("%").strip() for line in lines]
            metadata_values = [line for line in metadata_values if line]
            if len(metadata_values) >= 2:
                return metadata_values[1]

    return os.path.splitext(os.path.basename(source_file or "document"))[0]


def render_element(row):
    content = (row.get("content") or "").strip()
    if row.get("element_type") == "code_block":
        return f"```text\n{content}\n```"
    return content


def render_chunk_text(document_title, section_path, units):
    heading = f"# {document_title}\n\n## {section_path}\n\n"
    body = "\n\n".join(unit["rendered_content"] for unit in units)
    return heading + body


def make_chunks_for_document(document_id, rows):
    """Create deterministic section-aware chunks for one document."""

    source_file = first_non_empty(rows, "source_file", default=None)
    source_uri = first_non_empty(rows, "source_uri", default=None)
    document_version = first_non_empty(rows, "document_version", default=None)
    source_sha256 = first_non_empty(rows, "source_sha256", default=None)
    parser_version = first_non_empty(rows, "parser_version", default="UNKNOWN")
    manifest_hash = first_non_empty(
        rows, "manifest_record_sha256", default=None
    )
    document_status = first_non_empty(rows, "document_status", default="UNKNOWN")
    evidence_state = first_non_empty(rows, "evidence_state", default="UNKNOWN")
    document_type = first_non_empty(rows, "document_type", default="UNKNOWN")

    if not document_version:
        raise ValueError(f"{document_id}: document_version is missing")
    if not source_sha256 or len(source_sha256) != 64:
        raise ValueError(f"{document_id}: source_sha256 is missing or not 64 chars")
    if not source_uri or not source_file:
        raise ValueError(f"{document_id}: source_file/source_uri is missing")

    document_title = extract_title(rows, source_file)

    eligibility_values = {
        parse_bool(row.get("production_retrieval_eligible"), default=False)
        for row in rows
        if row.get("production_retrieval_eligible") is not None
    }
    if len(eligibility_values) > 1:
        raise ValueError(f"{document_id}: inconsistent retrieval eligibility")
    # Fail closed: missing eligibility metadata means not approved for retrieval.
    retrieval_eligible = next(iter(eligibility_values), False)

    # Fingerprint all document-level metadata that affects retrieval behavior.
    # This also detects eligibility/status changes when the parser output does
    # not expose the original manifest-record hash as a separate column.
    metadata_payload = {
        "document_id": document_id,
        "document_title": document_title,
        "source_file": source_file,
        "source_uri": source_uri,
        "document_version": document_version,
        "source_sha256": source_sha256,
        "manifest_record_sha256": manifest_hash,
        "parser_version": parser_version,
        "document_type": document_type,
        "document_status": document_status,
        "evidence_state": evidence_state,
        "production_retrieval_eligible": retrieval_eligible,
    }
    document_metadata_sha256 = sha256_text(
        json.dumps(metadata_payload, sort_keys=True, ensure_ascii=False)
    )

    # Group by exact leaf section path. Parent headings remain represented in
    # the full section_path, so heading-only parent groups need not become chunks.
    sections = defaultdict(list)
    for row in rows:
        element_type = (row.get("element_type") or "").strip().lower()
        if element_type in SKIPPED_ELEMENT_TYPES:
            continue

        content = (row.get("content") or "").strip()
        if not content:
            continue

        section_path = (row.get("section_path") or "Document Overview").strip()
        sections[section_path].append({
            "element_id": str(row["element_id"]),
            "element_type": element_type,
            "source_line_start": int(row["source_line_start"]),
            "source_line_end": int(row["source_line_end"]),
            "content": content,
            "rendered_content": render_element(row),
            "_overlap": False,
        })

    ordered_sections = sorted(
        sections.items(),
        key=lambda item: min(unit["source_line_start"] for unit in item[1])
    )

    output = []
    sequence = 0
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for section_path, units in ordered_sections:
        units.sort(key=lambda unit: (
            unit["source_line_start"],
            unit["source_line_end"],
            unit["element_id"],
        ))

        current = []

        def emit(chunk_units):
            nonlocal sequence
            if not chunk_units:
                return

            sequence += 1
            chunk_text = render_chunk_text(document_title, section_path, chunk_units)
            content_hash = sha256_text(chunk_text)
            core_units = [unit for unit in chunk_units if not unit["_overlap"]]
            if not core_units:
                raise ValueError(f"{document_id}: generated overlap-only chunk")

            element_ids = [unit["element_id"] for unit in chunk_units]
            overlap_ids = [unit["element_id"] for unit in chunk_units if unit["_overlap"]]
            first_core_id = core_units[0]["element_id"]
            last_core_id = core_units[-1]["element_id"]

            identity = "|".join([
                document_id,
                document_version,
                source_sha256,
                manifest_hash or "",
                document_metadata_sha256,
                parser_version,
                CHUNKER_VERSION,
                section_path,
                first_core_id,
                last_core_id,
                content_hash,
            ])
            chunk_id = sha256_text(identity)

            output.append({
                "chunk_id": chunk_id,
                "document_id": document_id,
                "document_title": document_title,
                "source_file": source_file,
                "source_uri": source_uri,
                "document_version": document_version,
                "source_sha256": source_sha256,
                "manifest_record_sha256": manifest_hash,
                "document_metadata_sha256": document_metadata_sha256,
                "parser_version": parser_version,
                "chunker_version": CHUNKER_VERSION,
                "section_path": section_path,
                "chunk_sequence": sequence,
                "chunk_text": chunk_text,
                "content_hash": content_hash,
                "element_ids": element_ids,
                "overlap_element_ids": overlap_ids,
                "source_line_start": min(u["source_line_start"] for u in chunk_units),
                "source_line_end": max(u["source_line_end"] for u in chunk_units),
                "page_number": None,  # Markdown provenance is line-based.
                "element_count": len(element_ids),
                "character_count": len(chunk_text),
                "is_oversized": len(chunk_text) > MAX_CHUNK_CHARS,
                "production_retrieval_eligible": retrieval_eligible,
                "document_status": document_status,
                "evidence_state": evidence_state,
                "chunk_created_at": created_at,
            })

        for unit in units:
            candidate = current + [unit]
            candidate_text = render_chunk_text(document_title, section_path, candidate)

            if current and len(candidate_text) > MAX_CHUNK_CHARS:
                emit(current)

                # Controlled one-element overlap at a semantic boundary.
                previous = current[-1]
                carry = None
                if len(previous["rendered_content"]) <= OVERLAP_MAX_CHARS:
                    carry = dict(previous)
                    carry["_overlap"] = True

                current = [carry] if carry else []
                if current and len(render_chunk_text(
                    document_title, section_path, current + [unit]
                )) > MAX_CHUNK_CHARS:
                    current = []

            current.append(unit)

        emit(current)

    if not output:
        raise ValueError(
            f"{document_id}: no meaningful content elements produced chunks; refusing publish"
        )

    return output, {
        "document_id": document_id,
        "document_version": document_version,
        "source_sha256": source_sha256,
        "manifest_record_sha256": manifest_hash,
        "document_metadata_sha256": document_metadata_sha256,
        "parser_version": parser_version,
        "chunker_version": CHUNKER_VERSION,
        "chunk_count": len(output),
    }


def run_local_chunker_self_test():
    """Exercise long-section splitting, overlap and deterministic IDs in memory."""
    global MAX_CHUNK_CHARS
    original_limit = MAX_CHUNK_CHARS
    MAX_CHUNK_CHARS = 550
    try:
        synthetic_rows = []
        for index in range(1, 6):
            synthetic_rows.append({
                "document_id": "DAY20-SELF-TEST",
                "element_id": f"self-test-element-{index}",
                "element_type": "paragraph",
                "section_path": "Synthetic > Long Section",
                "source_line_start": index * 2,
                "source_line_end": index * 2,
                "content": f"Element {index}: " + (chr(64 + index) * 165),
                "source_file": "synthetic.md",
                "source_uri": "s3://synthetic/day20/synthetic.md",
                "source_sha256": "a" * 64,
                "document_version": "test-1.0",
                "parser_version": "test-parser-1.0",
                "manifest_record_sha256": "b" * 64,
                "production_retrieval_eligible": False,
                "document_status": "TEST_ONLY",
                "evidence_state": "SYNTHETIC_FIXTURE",
            })

        first_run, _ = make_chunks_for_document("DAY20-SELF-TEST", synthetic_rows)
        second_run, _ = make_chunks_for_document("DAY20-SELF-TEST", synthetic_rows)

        assert len(first_run) > 1, "Long section was not split"
        assert any(chunk["overlap_element_ids"] for chunk in first_run), (
            "Expected controlled overlap was not created"
        )
        assert [c["chunk_id"] for c in first_run] == [
            c["chunk_id"] for c in second_run
        ], "Chunk IDs are not deterministic"

        core_ids = []
        for chunk in first_run:
            core_ids.extend([
                element_id for element_id in chunk["element_ids"]
                if element_id not in set(chunk["overlap_element_ids"])
            ])
        assert sorted(core_ids) == sorted(
            row["element_id"] for row in synthetic_rows
        ), "Core elements were dropped or duplicated during splitting"

        changed_rows = [
            {**row, "source_sha256": "c" * 64}
            for row in synthetic_rows
        ]
        changed_run, _ = make_chunks_for_document("DAY20-SELF-TEST", changed_rows)
        assert first_run[0]["chunk_id"] != changed_run[0]["chunk_id"], (
            "Source update did not generate a new chunk identity"
        )

        print("chunker_self_test=PASS")
        print(f"synthetic_long_section_chunks={len(first_run)}")
        print("overlap_test=PASS | deterministic_id_test=PASS | source_change_test=PASS")
    finally:
        MAX_CHUNK_CHARS = original_limit


run_local_chunker_self_test()


# ---------------------------------------------------------------------
# Main ingestion / publish workflow
# ---------------------------------------------------------------------

spark.sql(CHUNK_TABLE_DDL)
spark.sql(STATE_TABLE_DDL)

source_df = spark.table(SOURCE_TABLE)
source_columns = set(source_df.columns)
required_columns = {
    "document_id", "element_id", "element_type", "section_path", "content",
    "source_file", "source_uri", "source_sha256", "document_version",
    "source_line_start", "source_line_end",
}
missing_columns = sorted(required_columns - source_columns)
if missing_columns:
    raise ValueError(f"Parsed source table is missing required columns: {missing_columns}")

optional_columns = [
    "parser_version", "manifest_record_sha256", "document_title",
    "production_retrieval_eligible", "document_status", "evidence_state",
    "document_type",
]
selected_columns = sorted(required_columns) + [
    column for column in optional_columns if column in source_columns
]

profile = source_df.agg(
    F.count("*").alias("row_count"),
    F.coalesce(F.sum(F.length(F.coalesce(F.col("content"), F.lit("")))), F.lit(0)).alias("content_chars"),
).first()

if profile["row_count"] > MAX_SOURCE_ELEMENTS:
    raise RuntimeError(
        f"Source has {profile['row_count']} elements, above driver safety limit "
        f"{MAX_SOURCE_ELEMENTS}. Use a distributed chunking implementation."
    )
if profile["content_chars"] > MAX_SOURCE_CONTENT_CHARS:
    raise RuntimeError(
        f"Source has {profile['content_chars']} content characters, above driver safety "
        f"limit {MAX_SOURCE_CONTENT_CHARS}. Use a distributed chunking implementation."
    )

source_rows = [row.asDict(recursive=True) for row in source_df.select(*selected_columns).collect()]
if not source_rows:
    raise RuntimeError("Parsed operational source table is empty; refusing to publish.")

rows_by_document = defaultdict(list)
for row in source_rows:
    rows_by_document[str(row["document_id"])].append(row)

# Load active pointers once. This state is the source of truth for which
# document/chunker snapshot is currently published.
state_rows = spark.table(STATE_TABLE).collect()
state_document_ids = [row["document_id"] for row in state_rows]
if len(state_document_ids) != len(set(state_document_ids)):
    raise RuntimeError(
        f"{STATE_TABLE} contains duplicate document_id rows. "
        "Resolve state-table duplicates before publishing chunks."
    )
state_by_document = {row["document_id"]: row.asDict(recursive=True) for row in state_rows}

new_chunk_records = []
new_state_records = []
skipped_documents = []
processed_documents = []

for document_id, document_rows in sorted(rows_by_document.items()):
    # Validate identity metadata and build only if the active snapshot is stale.
    _, expected = make_chunks_for_document(document_id, document_rows)
    state = state_by_document.get(document_id)

    desired_identity_matches = bool(state) and all([
        state.get("active_document_version") == expected["document_version"],
        state.get("active_source_sha256") == expected["source_sha256"],
        state.get("active_manifest_record_sha256") == expected["manifest_record_sha256"],
        state.get("active_document_metadata_sha256") == expected["document_metadata_sha256"],
        state.get("active_parser_version") == expected["parser_version"],
        state.get("active_chunker_version") == expected["chunker_version"],
    ])

    existing_count = 0
    if desired_identity_matches:
        existing_count = (
            spark.table(CHUNK_TABLE)
            .where(
                (F.col("document_id") == document_id)
                & (F.col("document_version") == expected["document_version"])
                & (F.col("source_sha256") == expected["source_sha256"])
                & F.col("manifest_record_sha256").eqNullSafe(expected["manifest_record_sha256"])
                & (F.col("document_metadata_sha256") == expected["document_metadata_sha256"])
                & (F.col("parser_version") == expected["parser_version"])
                & (F.col("chunker_version") == expected["chunker_version"])
            )
            .count()
        )

    if (
        desired_identity_matches
        and existing_count == int(state.get("active_chunk_count") or -1)
        and existing_count > 0
    ):
        skipped_documents.append(document_id)
        continue

    chunks, expected = make_chunks_for_document(document_id, document_rows)
    new_chunk_records.extend(chunks)
    new_state_records.append({
        "document_id": document_id,
        "active_document_version": expected["document_version"],
        "active_source_sha256": expected["source_sha256"],
        "active_manifest_record_sha256": expected["manifest_record_sha256"],
        "active_document_metadata_sha256": expected["document_metadata_sha256"],
        "active_parser_version": expected["parser_version"],
        "active_chunker_version": expected["chunker_version"],
        "active_chunk_count": expected["chunk_count"],
        "active_published_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "last_attempt_status": "SUCCESS",
        "last_attempt_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "last_error": None,
    })
    processed_documents.append(document_id)


if new_chunk_records:
    staged_chunks = spark.createDataFrame(new_chunk_records, schema=CHUNK_SCHEMA)
    staged_chunks.createOrReplaceTempView("_staged_document_chunks")

    # Immutable chunk snapshots: insert missing IDs, never overwrite history.
    spark.sql(f"""
        MERGE INTO {CHUNK_TABLE} AS target
        USING _staged_document_chunks AS source
        ON target.chunk_id = source.chunk_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Validate the just-written snapshots before changing the active pointer.
    for state_record in new_state_records:
        document_id = state_record["document_id"]
        condition = (
            (F.col("document_id") == document_id)
            & (F.col("document_version") == state_record["active_document_version"])
            & (F.col("source_sha256") == state_record["active_source_sha256"])
            & F.col("manifest_record_sha256").eqNullSafe(state_record["active_manifest_record_sha256"])
            & (F.col("document_metadata_sha256") == state_record["active_document_metadata_sha256"])
            & (F.col("parser_version") == state_record["active_parser_version"])
            & (F.col("chunker_version") == state_record["active_chunker_version"])
        )
        written = spark.table(CHUNK_TABLE).where(condition)
        actual_count = written.count()
        duplicate_count = (
            written.groupBy("chunk_id").count().where(F.col("count") > 1).count()
        )
        invalid_count = written.where(
            F.col("chunk_text").isNull()
            | (F.length(F.trim(F.col("chunk_text"))) == 0)
            | F.col("source_uri").isNull()
            | F.col("source_sha256").isNull()
            | F.col("section_path").isNull()
            | F.col("element_ids").isNull()
            | (F.size(F.col("element_ids")) == 0)
        ).count()

        expected_count = int(state_record["active_chunk_count"])
        if actual_count != expected_count or duplicate_count != 0 or invalid_count != 0:
            raise RuntimeError(
                f"Chunk validation failed for {document_id}: "
                f"expected={expected_count}, actual={actual_count}, "
                f"duplicate_ids={duplicate_count}, invalid_rows={invalid_count}. "
                "Active document pointer was not updated."
            )

    # Publish only after all newly written documents pass validation.
    state_df = spark.createDataFrame(new_state_records, schema=STATE_SCHEMA)
    state_df.createOrReplaceTempView("_staged_document_chunk_state")
    spark.sql(f"""
        MERGE INTO {STATE_TABLE} AS target
        USING _staged_document_chunk_state AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("=" * 72)
print("DAY 20 DOCUMENT CHUNKING SUMMARY")
print(f"source_table={SOURCE_TABLE}")
print(f"chunk_table={CHUNK_TABLE}")
print(f"state_table={STATE_TABLE}")
print(f"chunker_version={CHUNKER_VERSION}")
print(f"max_chunk_chars={MAX_CHUNK_CHARS}")
print(f"documents_discovered={len(rows_by_document)}")
print(f"documents_processed={len(processed_documents)}")
print(f"documents_skipped_unchanged={len(skipped_documents)}")
print(f"chunks_staged_for_publish={len(new_chunk_records)}")
print(f"processed_document_ids={processed_documents}")
print(f"skipped_document_ids={skipped_documents}")
print("=" * 72)

# Current-version view includes all active chunks, including documents that
# are not yet approved for production retrieval.
spark.sql(f"""
CREATE OR REPLACE VIEW zomato_dev.ops_knowledge.active_document_chunks AS
SELECT c.*
FROM {CHUNK_TABLE} AS c
INNER JOIN {STATE_TABLE} AS s
    ON  c.document_id = s.document_id
    AND c.document_version = s.active_document_version
    AND c.source_sha256 = s.active_source_sha256
    AND c.manifest_record_sha256 <=> s.active_manifest_record_sha256
    AND c.document_metadata_sha256 = s.active_document_metadata_sha256
    AND c.parser_version = s.active_parser_version
    AND c.chunker_version = s.active_chunker_version
""")

# This is the only view future production retrieval should consume.
spark.sql("""
CREATE OR REPLACE VIEW zomato_dev.ops_knowledge.production_retrieval_chunks AS
SELECT *
FROM zomato_dev.ops_knowledge.active_document_chunks
WHERE production_retrieval_eligible = TRUE
""")

print("Views refreshed: active_document_chunks, production_retrieval_chunks")
