"""Zomato Operations Intelligence Assistant - Day 22 integration tests.

Tests the isolated synthetic AI Search source/index, not production runbooks.
The final run should leave the three original synthetic fixtures restored.

Run this Python file from the Databricks Workspace ai_search directory on
Serverless Python compute. It is not a Lakeflow pipeline task.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)

try:
    from .config import (
        TEST_SOURCE_TABLE,
        MAX_EMBEDDING_TEXT_BYTES,
    )
    from .index import sync_test_index
    from .search import search_test_runbooks
except ImportError:
    # Supports Databricks Run File when this directory is on sys.path.
    from config import TEST_SOURCE_TABLE, MAX_EMBEDDING_TEXT_BYTES
    from index import sync_test_index
    from search import search_test_runbooks

logger = logging.getLogger("zomato_ai_search_tests")

EXPECTED_FIXTURE_COUNT = 3
FIXTURE_URI_PREFIX = "test-fixture://day22/"
UPDATE_MARKER = "S3-ARRIVAL-UPDATE-CANARY-7F31"
TOP_K = 1

TEST_SOURCE_SCHEMA = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("document_title", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_uri", StringType(), True),
    StructField("document_version", StringType(), True),
    StructField("source_sha256", StringType(), True),
    StructField("section_path", StringType(), True),
    StructField("source_line_start", IntegerType(), True),
    StructField("source_line_end", IntegerType(), True),
    StructField("document_status", StringType(), True),
    StructField("evidence_state", StringType(), True),
    StructField("production_retrieval_eligible", BooleanType(), False),
    StructField("chunker_version", StringType(), True),
    StructField("chunk_created_at", TimestampType(), True),
    StructField("is_test_fixture", BooleanType(), False),
    StructField("test_case_id", StringType(), False),
])

SOURCE_COLUMNS = [field.name for field in TEST_SOURCE_SCHEMA.fields]
UPDATE_COLUMNS = [
    column
    for column in SOURCE_COLUMNS
    if column not in {"chunk_id", "chunk_created_at"}
]


def build_test_fixtures():
    """Return three deterministic synthetic incident examples."""
    return [
        {
            "chunk_id": "TEST-INC-001",
            "document_id": "TEST-DOC-001",
            "document_title": "Synthetic S3 Missing File Runbook",
            "source_file": "test_s3_missing.md",
            "section_path": "S3 / Missing Source File",
            "test_case_id": "TEST-INC-001",
            "chunk_text": (
                "Synthetic test procedure: When an expected source CSV "
                "file is missing from S3, inspect the expected landing "
                "prefix, verify the source file inventory, check file "
                "arrival evidence, and inspect the ingestion job logs."
            ),
        },
        {
            "chunk_id": "TEST-INC-002",
            "document_id": "TEST-DOC-002",
            "document_title": "Synthetic Schema Validation Runbook",
            "source_file": "test_schema_mismatch.md",
            "section_path": "Schema / Missing Required Column",
            "test_case_id": "TEST-INC-002",
            "chunk_text": (
                "Synthetic test procedure: When a required source column "
                "is missing, compare the observed schema with the expected "
                "source contract, inspect rescued data and identify the "
                "affected ingestion flow."
            ),
        },
        {
            "chunk_id": "TEST-INC-003",
            "document_id": "TEST-DOC-003",
            "document_title": "Synthetic Data Quality Runbook",
            "source_file": "test_data_quality.md",
            "section_path": "Data Quality / Quarantine",
            "test_case_id": "TEST-INC-003",
            "chunk_text": (
                "Synthetic test procedure: When data quality validation "
                "fails, inspect failed expectations, review rejection "
                "reasons, check quarantine records and verify the affected "
                "source data."
            ),
        },
    ]


def create_test_source_table(spark):
    """Create the isolated Delta fixture table if it does not exist."""
    logger.info("Creating isolated test source table")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TEST_SOURCE_TABLE} (
            chunk_id STRING NOT NULL,
            chunk_text STRING NOT NULL,
            document_id STRING NOT NULL,
            document_title STRING,
            source_file STRING,
            source_uri STRING,
            document_version STRING,
            source_sha256 STRING,
            section_path STRING,
            source_line_start INT,
            source_line_end INT,
            document_status STRING,
            evidence_state STRING,
            production_retrieval_eligible BOOLEAN NOT NULL,
            chunker_version STRING,
            chunk_created_at TIMESTAMP,
            is_test_fixture BOOLEAN NOT NULL,
            test_case_id STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableRowTracking' = 'true',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    # Ensure existing versions of this table also have both required properties.
    spark.sql(f"""
        ALTER TABLE {TEST_SOURCE_TABLE}
        SET TBLPROPERTIES (
            'delta.enableRowTracking' = 'true',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    logger.info("Test source table is available with CDF and row tracking")


def prepare_candidate_dataframe(spark, changed_fixture: Optional[Dict] = None):
    """Build the full three-row fixture snapshot, optionally changing one row."""
    fixtures = build_test_fixtures()

    if changed_fixture:
        for fixture in fixtures:
            if fixture["chunk_id"] == changed_fixture["chunk_id"]:
                fixture.update(changed_fixture)
                break
        else:
            raise ValueError(
                f"Unknown fixture ID: {changed_fixture['chunk_id']}"
            )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []

    for fixture in fixtures:
        text = fixture["chunk_text"]
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

        rows.append((
            fixture["chunk_id"],
            text,
            fixture["document_id"],
            fixture["document_title"],
            fixture["source_file"],
            f"{FIXTURE_URI_PREFIX}{fixture['source_file']}",
            fixture.get("document_version", "1.0.0-test"),
            text_hash,
            fixture["section_path"],
            1,
            10,
            "TEST_FIXTURE",
            "SYNTHETIC_TEST_DATA",
            False,
            "day22-test-1.0",
            now,
            True,
            fixture["test_case_id"],
        ))

    return spark.createDataFrame(rows, schema=TEST_SOURCE_SCHEMA)


def validate_candidate(candidate_df):
    """Fail fast if the fixture snapshot violates the test data contract."""
    logger.info("Validating synthetic candidates")

    if candidate_df.count() != EXPECTED_FIXTURE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_FIXTURE_COUNT} fixtures, "
            f"received {candidate_df.count()}"
        )

    duplicates = (
        candidate_df.groupBy("chunk_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError("Duplicate test chunk IDs detected")

    invalid_classification = candidate_df.filter(
        ~F.coalesce(F.col("is_test_fixture"), F.lit(False))
        | F.coalesce(
            F.col("production_retrieval_eligible"), F.lit(False)
        )
    ).limit(1).count()
    if invalid_classification:
        raise ValueError("Synthetic fixture approval boundary violated")

    invalid_text = candidate_df.filter(
        F.col("chunk_text").isNull()
        | (F.trim(F.col("chunk_text")) == "")
    ).limit(1).count()
    if invalid_text:
        raise ValueError("Empty test chunk text detected")

    oversized = candidate_df.filter(
        F.length(F.encode(F.col("chunk_text"), "UTF-8"))
        > MAX_EMBEDDING_TEXT_BYTES
    ).limit(1).count()
    if oversized:
        raise ValueError("Embedding text exceeds configured byte limit")

    logger.info("Candidate validation passed")


def publish_test_fixtures(spark, candidate_df):
    """Idempotently publish the complete fixture snapshot using Delta MERGE."""
    logger.info("Publishing synthetic fixtures")
    candidate_df.createOrReplaceTempView("day22_test_candidates")

    update_condition = " OR ".join(
        f"NOT (target.{column} <=> source.{column})"
        for column in UPDATE_COLUMNS
    )

    spark.sql(f"""
        MERGE INTO {TEST_SOURCE_TABLE} AS target
        USING day22_test_candidates AS source
          ON target.chunk_id = source.chunk_id
        WHEN MATCHED AND ({update_condition}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)
    logger.info("Test fixture publication completed")


def validate_test_source(spark):
    """Validate the final Delta fixture table and row-tracking property."""
    logger.info("Validating final test source")
    test_df = spark.table(TEST_SOURCE_TABLE)
    row_count = test_df.count()

    if row_count != EXPECTED_FIXTURE_COUNT:
        raise RuntimeError(
            f"Expected {EXPECTED_FIXTURE_COUNT} records, found {row_count}"
        )

    invalid = test_df.filter(
        ~F.coalesce(F.col("is_test_fixture"), F.lit(False))
        | F.coalesce(
            F.col("production_retrieval_eligible"), F.lit(False)
        )
    ).limit(1).count()
    if invalid:
        raise RuntimeError("Invalid test fixture classification")

    duplicates = (
        test_df.groupBy("chunk_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise RuntimeError("Duplicate IDs found in test source")

    invalid_text = test_df.filter(
        F.col("chunk_text").isNull()
        | (F.trim(F.col("chunk_text")) == "")
    ).limit(1).count()
    if invalid_text:
        raise RuntimeError("Invalid text found in test source")

    properties = {
        row["key"]: row["value"]
        for row in spark.sql(
            f"SHOW TBLPROPERTIES {TEST_SOURCE_TABLE}"
        ).collect()
    }
    if properties.get("delta.enableRowTracking") != "true":
        raise RuntimeError("Delta row tracking is disabled")
    if properties.get("delta.enableChangeDataFeed") != "true":
        raise RuntimeError("Delta Change Data Feed is disabled")

    logger.info("Test source validation PASSED")
    return {
        "table": TEST_SOURCE_TABLE,
        "row_count": row_count,
        "unique_chunk_ids": row_count,
        "production_eligible": False,
        "row_tracking": True,
        "change_data_feed": True,
        "status": "PASS",
    }


def prepare_test_source(spark, changed_fixture: Optional[Dict] = None):
    """Create, validate and publish either baseline or changed test fixtures."""
    create_test_source_table(spark)
    candidate_df = prepare_candidate_dataframe(
        spark,
        changed_fixture=changed_fixture,
    )
    validate_candidate(candidate_df)
    publish_test_fixtures(spark, candidate_df)
    return validate_test_source(spark)


def run_known_answer_tests():
    """Verify each known incident query retrieves its expected synthetic chunk."""
    cases = [
        {
            "case": "Missing S3 source",
            "query": (
                "The expected CSV file never arrived in S3. "
                "How should I investigate?"
            ),
            "expected_chunk": "TEST-INC-001",
            "expected_document": "TEST-DOC-001",
        },
        {
            "case": "Schema mismatch",
            "query": (
                "A required column disappeared from incoming source data. "
                "What should I check?"
            ),
            "expected_chunk": "TEST-INC-002",
            "expected_document": "TEST-DOC-002",
        },
        {
            "case": "Data quality failure",
            "query": (
                "My data quality checks failed. How do I investigate "
                "quarantined records?"
            ),
            "expected_chunk": "TEST-INC-003",
            "expected_document": "TEST-DOC-003",
        },
    ]

    results_summary = []
    for case in cases:
        results = search_test_runbooks(
            query=case["query"],
            top_k=TOP_K,
        )

        actual_chunk = results[0].get("chunk_id") if results else None
        actual_document = results[0].get("document_id") if results else None
        passed = (
            actual_chunk == case["expected_chunk"]
            and actual_document == case["expected_document"]
        )

        outcome = {
            "case": case["case"],
            "expected_chunk": case["expected_chunk"],
            "actual_chunk": actual_chunk,
            "status": "PASS" if passed else "FAIL",
        }
        results_summary.append(outcome)
        logger.info("Retrieval result: %s", outcome)

        if passed:
            required_provenance = [
                "source_uri",
                "document_version",
                "section_path",
                "source_line_start",
                "source_line_end",
            ]
            missing = [
                field for field in required_provenance
                if results[0].get(field) is None
            ]
            if missing:
                raise AssertionError(
                    f"Missing provenance fields for {actual_chunk}: {missing}"
                )

    failures = [item for item in results_summary if item["status"] != "PASS"]
    if failures:
        raise AssertionError(f"Known-answer retrieval tests failed: {failures}")

    return results_summary


def run_no_result_test():
    """Verify an impossible document_id filter produces an empty result."""
    logger.info("Running no-result test")
    results = search_test_runbooks(
        query="How should I investigate a missing source CSV?",
        top_k=3,
        additional_filters={
            "document_id": "DOCUMENT-DOES-NOT-EXIST"
        },
    )

    if results:
        raise AssertionError(
            f"Expected zero results but received {len(results)}"
        )

    logger.info("No-result test PASSED")
    return {"test": "no_result", "result_count": 0, "status": "PASS"}


def validate_original_test_fixtures(spark):
    """Verify cleanup restored all three baseline records and removed the canary."""
    df = spark.table(TEST_SOURCE_TABLE)
    rows = {
        row["chunk_id"]: row.asDict(recursive=True)
        for row in df.select("chunk_id", "chunk_text", "document_version").collect()
    }

    expected_ids = {"TEST-INC-001", "TEST-INC-002", "TEST-INC-003"}
    if set(rows) != expected_ids:
        raise AssertionError(
            f"Fixture restoration mismatch. Expected {expected_ids}, found {set(rows)}"
        )

    changed = rows["TEST-INC-001"]
    if changed.get("document_version") != "1.0.0-test":
        raise AssertionError(
            f"Baseline version not restored: {changed.get('document_version')}"
        )
    if UPDATE_MARKER in (changed.get("chunk_text") or ""):
        raise AssertionError("Incremental update marker remained after restoration")

    # Validate the index itself also serves the restored baseline, not just Delta.
    restored_search = search_test_runbooks(
        query="How do I investigate an expected source CSV file missing from S3?",
        top_k=1,
        additional_filters={"test_case_id": "TEST-INC-001"},
    )
    if not restored_search:
        raise AssertionError("Restored baseline fixture is not retrievable from AI Search")
    if restored_search[0].get("document_version") != "1.0.0-test":
        raise AssertionError(
            "AI Search index is not serving restored baseline version "
            "1.0.0-test"
        )
    if UPDATE_MARKER in (restored_search[0].get("chunk_text") or ""):
        raise AssertionError("AI Search still returns the changed update marker")

    return {
        "row_count": len(rows),
        "baseline_version": "1.0.0-test",
        "index_baseline_verified": True,
        "status": "PASS",
    }


def run_incremental_sync_test(spark):
    """Change one fixture, wait for its Delta commit to be indexed, then restore it."""
    logger.info("Starting changed-document synchronization test")

    changed_fixture = {
        "chunk_id": "TEST-INC-001",
        "chunk_text": (
            "Synthetic test procedure: When an expected source CSV file "
            "is missing from S3, inspect the expected landing prefix, "
            "verify the source file inventory and inspect ingestion job logs. "
            f"Updated retrieval marker: {UPDATE_MARKER}."
        ),
        "document_version": "1.0.1-test",
    }

    test_result = None
    test_error = None
    cleanup_error = None
    restoration_result = None

    try:
        prepare_test_source(spark, changed_fixture=changed_fixture)

        # This now waits until the AI Search index processed the exact latest
        # Delta source version, not merely until the index reports ready=True.
        changed_index_description = sync_test_index(spark)

        results = search_test_runbooks(
            query=UPDATE_MARKER,
            top_k=1,
            additional_filters={"test_case_id": "TEST-INC-001"},
        )

        if not results:
            raise AssertionError(
                "Updated fixture was not retrieved after its source commit was indexed"
            )

        record = results[0]
        if UPDATE_MARKER not in record.get("chunk_text", ""):
            raise AssertionError("Updated marker missing from retrieved text")
        if record.get("document_version") != "1.0.1-test":
            raise AssertionError(
                "Updated document version was not retrieved after synchronization"
            )

        changed_status = changed_index_description.get("status") or {}
        test_result = {
            "test": "changed_document_sync",
            "updated_version": record["document_version"],
            "index_state": changed_status.get("detailed_state"),
            "status": "PASS",
        }
        logger.info("Changed-document incremental sync test PASSED: %s", test_result)

    except Exception as exc:
        test_error = exc
        logger.exception("Changed-document incremental sync test FAILED")

    finally:
        # Always attempt deterministic restoration, even when sync/search fails.
        logger.info("Restoring original test fixtures")
        try:
            prepare_test_source(spark)
            restored_index_description = sync_test_index(spark)
            restoration_result = validate_original_test_fixtures(spark)

            restored_status = restored_index_description.get("status") or {}
            restoration_result["index_state"] = restored_status.get("detailed_state")
            restoration_result["indexed_row_count"] = restored_status.get(
                "indexed_row_count"
            )
            logger.info("Original test fixtures restored and synchronized: %s", restoration_result)
        except Exception as exc:
            cleanup_error = exc
            logger.exception("Failed to restore/synchronize original test fixtures")

    if test_error and cleanup_error:
        raise RuntimeError(
            "Incremental sync test failed and fixture restoration also failed. "
            f"Test error={test_error!r}; cleanup error={cleanup_error!r}"
        ) from cleanup_error
    if cleanup_error:
        raise RuntimeError(
            f"Fixture restoration/synchronization failed: {cleanup_error!r}"
        ) from cleanup_error
    if test_error:
        raise test_error

    return {
        **(test_result or {}),
        "restoration": restoration_result,
        "status": "PASS",
    }


def main():
    """Execute the Day 22 synthetic AI Search integration test suite."""
    logger.info("STARTING DAY 22 AI SEARCH TEST SUITE")
    spark = SparkSession.builder.getOrCreate()

    source_result = prepare_test_source(spark)
    logger.info("Source preparation: %s", source_result)

    sync_test_index(spark)
    known_answer_results = run_known_answer_tests()
    no_result_result = run_no_result_test()
    incremental_result = run_incremental_sync_test(spark)

    summary = {
        "source_validation": source_result["status"],
        "known_answer_tests": known_answer_results,
        "no_result_test": no_result_result,
        "incremental_sync_test": incremental_result,
        "test_source_restored": incremental_result["restoration"]["status"] == "PASS",
    }

    print("\n" + "=" * 65)
    print("DAY 22 AI SEARCH INTEGRATION TEST SUMMARY")
    print("=" * 65)
    for key, value in summary.items():
        print(f"\n{key}: {value}")
    print("\nDAY 22 TEST EXECUTION FINISHED")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
