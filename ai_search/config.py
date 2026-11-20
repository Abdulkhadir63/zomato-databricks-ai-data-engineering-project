"""Central configuration for the Zomato Operations Intelligence Assistant."""

CATALOG = "zomato_dev"
SCHEMA = "ops_knowledge"

DOCUMENT_CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.document_chunks"
ACTIVE_CHUNKS_VIEW = f"{CATALOG}.{SCHEMA}.active_document_chunks"
APPROVED_CHUNKS_VIEW = f"{CATALOG}.{SCHEMA}.production_retrieval_chunks"

# Project-production corpus: the user's six current Zomato operational runbooks.
# This is a project-level publication decision, not a claim of external Zomato approval.

AI_SEARCH_SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.project_runbook_search_source"
AI_SEARCH_ENDPOINT_NAME = "zomato_ops_search_endpoint"
AI_SEARCH_INDEX_NAME = f"{CATALOG}.{SCHEMA}.project_runbook_chunks_index"
PROJECT_PRODUCTION_DOCUMENT_IDS = [
    "ZOMATO-OPS-ARCH-001",
    "ZOMATO-OPS-DATA-001",
    "ZOMATO-OPS-DQ-001",
    "ZOMATO-OPS-PIPE-001",
    "ZOMATO-OPS-RB-001",
    "ZOMATO-OPS-REPLAY-001",
]
PROJECT_SCOPE_VALUE = "PERSONAL_PROJECT_PRODUCTION"

# Isolated synthetic regression-test corpus. Never use it from the production agent.
TEST_SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.ai_search_test_source_chunks"
TEST_INDEX_NAME = f"{CATALOG}.{SCHEMA}.runbook_chunks_test_index"

PRIMARY_KEY_COLUMN = "chunk_id"
EMBEDDING_SOURCE_COLUMN = "chunk_text"
SYNC_MODE = "TRIGGERED"
DEFAULT_TOP_K = 5
MAX_EMBEDDING_TEXT_BYTES = 32764

# Existing metadata fields already selected for the Day 22 test index.
INDEX_METADATA_COLUMNS = [
    "document_id",
    "document_title",
    "source_file",
    "source_uri",
    "document_version",
    "source_sha256",
    "section_path",
    "source_line_start",
    "source_line_end",
    "document_status",
    "evidence_state",
    "production_retrieval_eligible",
]

# Production index receives the original evidence fields plus the project-specific
# eligibility flag. Original document_status/evidence_state are never overwritten.
PROJECT_PRODUCTION_METADATA_COLUMNS = [
    *INDEX_METADATA_COLUMNS,
    "project_retrieval_eligible",
]

PRODUCTION_RETURN_COLUMNS = [
    PRIMARY_KEY_COLUMN,
    EMBEDDING_SOURCE_COLUMN,
    *PROJECT_PRODUCTION_METADATA_COLUMNS,
]

TEST_RETURN_COLUMNS = [
    PRIMARY_KEY_COLUMN,
    EMBEDDING_SOURCE_COLUMN,
    *INDEX_METADATA_COLUMNS,
    "is_test_fixture",
    "test_case_id",
]

PRODUCTION_ALLOWED_FILTERS = {
    "document_id",
    "document_version",
    "section_path",
    "document_status",
    "evidence_state",
}

TEST_ALLOWED_FILTERS = {
    "document_id",
    "document_version",
    "section_path",
    "test_case_id",
}

EMBEDDING_MODEL_ENDPOINT_NAME = "databricks-qwen3-embedding-0-6b"
