
"""
DAY 23 - HUMAN-JUDGED RETRIEVAL EVALUATION

Reuses the existing Databricks AI Search implementation.

Relevance:
    2 = Highly relevant
    1 = Partially relevant
    0 = Irrelevant

Read-only against the existing retrieval infrastructure.
"""

import math
from search import search_runbooks


# ============================================================
# 1. CONFIGURATION
# ============================================================

TOP_K = 5

# Use document ID + version + section path as the
# judgement identity.
#
# Each of the 153 source chunks has a unique section path
# within its document/version in our current corpus.

def J(document_id, version, section_path, grade):

    return {
        "document_id": document_id,
        "document_version": version,
        "section_path": section_path,
        "grade": grade,
    }


# ============================================================
# 2. FIFTEEN EVALUATION QUESTIONS
# ============================================================

TEST_CASES = [

    {
        "id": "Q01",
        "query": "What components sit between source CSV files and Gold, and which component orchestrates execution?"
    },
    {
        "id": "Q02",
        "query": "An expected source CSV has not arrived in S3. How should I investigate?"
    },
    {
        "id": "Q03",
        "query": "A new optional column appears in an incoming CSV. What schema evolution behavior is documented?"
    },
    {
        "id": "Q04",
        "query": "Why might required-column validation pass even when an individual incoming CSV has an incorrect header?"
    },
    {
        "id": "Q05",
        "query": "How do Bronze, Silver and Gold processing stages depend on one another?"
    },
    {
        "id": "Q06",
        "query": "Quarantine volume suddenly increases. What should I investigate?"
    },
    {
        "id": "Q07",
        "query": "Menu and order-item records reference nonexistent food IDs. Should we delete those records or fabricate missing food records?"
    },
    {
        "id": "Q08",
        "query": "Silver contains duplicate business keys after incremental processing. What should I investigate?"
    },
    {
        "id": "Q09",
        "query": "Late-arriving source data was missed. What replay or backfill procedure is documented?"
    },
    {
        "id": "Q10",
        "query": "A checkpoint reset could cause a full refresh. What safety considerations apply?"
    },
    {
        "id": "Q11",
        "query": "A Lakeflow Job task failed. What operational evidence must be inspected before diagnosing the cause?"
    },
    {
        "id": "Q12",
        "query": "Gold tables are stale even though the pipeline reports success. How should I investigate?"
    },
    {
        "id": "Q13",
        "query": "Two Gold revenue outputs disagree. What investigation should take place?"
    },
    {
        "id": "Q14",
        "query": "Can the Operations Intelligence Agent independently delete tables or execute destructive recovery?"
    },
    {
        "id": "Q15",
        "query": "What should happen when sensitive information such as users.password appears in operational evidence?"
    }
]


# ============================================================
# 3. HUMAN-JUDGED RELEVANCE LABELS
# ============================================================

JUDGEMENTS = {

    # Q01
    "Q01": [
        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 2. Logical end-to-end flow", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 9. Approval checklist", 0),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 7. What is not yet documented", 0),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 2. Contract boundaries", 1),

        J("ZOMATO-OPS-ARCH-001", "0.1.0",
          "Architecture and System Boundaries > 3. Logical data-plane architecture > 3.1 RAW", 1),
    ],

    # Q02
    "Q02": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > A. Source and S3 incidents > INC-001: Expected source file missing", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > A. Source and S3 incidents > INC-003: S3 file is incomplete or partially published", 1),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "11. Incident Testing and Runbook Validation", 0),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > B. Auto Loader and Bronze incidents > INC-006: Required column missing", 0),

        J("ZOMATO-OPS-ARCH-001", "0.1.0",
          "Architecture and System Boundaries > 3. Logical data-plane architecture > 3.1 RAW", 1),
    ],

    # Q03
    "Q03": [
        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 4. Schema evolution policy recorded in project notes", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > B. Auto Loader and Bronze incidents > INC-007: New source column detected", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > B. Auto Loader and Bronze incidents > INC-006: Required column missing", 1),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 8. Change-management expectations", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.2 Bronze ingestion", 2),
    ],

    # Q04
    "Q04": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > B. Auto Loader and Bronze incidents > INC-006: Required column missing", 2),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 3. Recorded Bronze ingestion behavior > 3.2 Required-column enforcement caveat", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "2. Platform Context and Incident Boundaries > 2.4 Recorded ingestion and transformation behavior", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.2 Bronze ingestion", 2),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 3. Recorded Bronze ingestion behavior", 2),
    ],

    # Q05
    "Q05": [
        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 2. Logical end-to-end flow", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.5 Gold", 1),

        J("ZOMATO-OPS-ARCH-001", "0.1.0",
          "Architecture and System Boundaries > 4. Orchestration and operational control boundary", 1),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 7. What is not yet documented", 1),

        J(
    "ZOMATO-OPS-PIPE-001",
    "0.1.0",
    "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.3 Silver canonicalization and validation",
    1
)

    ],

    # Q06
    "Q06": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > C. Silver transformation and data-quality incidents > INC-013: Quarantine volume suddenly increases", 2),

        J("ZOMATO-OPS-DQ-001", "0.1.0",
          "Data Quality and Quarantine > 2. Validation and quarantine model", 2),

        J("ZOMATO-OPS-DQ-001", "0.1.0",
          "Data Quality and Quarantine", 0),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "1. Purpose and Scope", 1),

        J("ZOMATO-OPS-DQ-001", "0.1.0",
          "Data Quality and Quarantine > 1. Purpose", 1),
    ],

    # Q07
    "Q07": [
        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Dictionary > 6. Known referential-integrity finding", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "2. Platform Context and Incident Boundaries > 2.5 Known data-quality finding", 2),

        J("ZOMATO-OPS-DQ-001", "0.1.0",
          "Data Quality and Quarantine > 4. Cross-entity referential-integrity assessment", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > C. Silver transformation and data-quality incidents > INC-014: Referential-integrity violation", 2),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Dictionary > 3. Recorded source schemas > 3.3 `order_items.csv`", 1),
    ],

    # Q08
    "Q08": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > D. CDC and incremental-processing incidents > INC-016: Duplicate business keys in Silver", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > A. Source and S3 incidents > INC-004: Duplicate source files", 1),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 4. Recorded Silver CDC behavior", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 7. Replay and recovery safety gates", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 9. Prohibited instructions until validation", 1),
    ],

    # Q09
    "Q09": [
        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 8. Known unverified items", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > D. CDC and incremental-processing incidents > INC-017: Late-arriving or out-of-order source data", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > D. CDC and incremental-processing incidents > INC-018: Missing historical data or incomplete backfill", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 7. Replay and recovery safety gates", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 2. Processing concepts and boundaries", 2),
    ],

    # Q10
    "Q10": [
        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 1. Purpose and current approval state", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > D. CDC and incremental-processing incidents > INC-019: Checkpoint reset or full-refresh risk", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 2. Processing concepts and boundaries", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 8. Known unverified items", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 12. External semantics reference", 1),
    ],

    # Q11
    "Q11": [
        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 8. Operational interpretation", 1),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "4. Standard Incident Response Procedure > Step 3: Inspect operational evidence", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > F. Lakeflow orchestration and compute incidents > INC-024: Lakeflow Job task failure", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "9. Operations Intelligence Agent Rules", 2),

        J("ZOMATO-OPS-ARCH-001", "0.1.0",
          "Architecture and System Boundaries > 4. Orchestration and operational control boundary", 2),
    ],

    # Q12
    "Q12": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > E. Gold and analytical incidents > INC-023: Gold table is stale while pipeline reports success", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.5 Gold", 1),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > E. Gold and analytical incidents > INC-020: Gold transformation failure", 1),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "10. Operational Monitoring Requirements", 2),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.4 Quarantine", 0),
    ],

    # Q13
    "Q13": [
        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 3. Stage responsibilities > 3.5 Gold", 1),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > E. Gold and analytical incidents > INC-022: Revenue or financial reconciliation mismatch", 2),

        J("ZOMATO-OPS-DQ-001", "0.1.0",
          "Data Quality and Quarantine > 6. Gold quality validation evidence", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "8. Post-Recovery Validation Checklist > Gold", 1),

        J("ZOMATO-OPS-PIPE-001", "0.1.0",
          "Pipeline Stages and Dependencies > 8. Operational interpretation", 1),
    ],

    # Q14
    "Q14": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "9. Operations Intelligence Agent Rules", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > G. Unity Catalog, deployment, and security incidents > INC-031: Accidental destructive operation", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "1. Purpose and Scope", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "7. Mandatory Recovery Safety Gates", 2),

        J("ZOMATO-OPS-REPLAY-001", "0.1.0",
          "Incremental Processing and Replay > 9. Prohibited instructions until validation", 2),
    ],

    # Q15
    "Q15": [
        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "5. Incident Catalogue and Recovery Playbooks > C. Silver transformation and data-quality incidents > INC-015: Sensitive data appears in output or operational evidence", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "9. Operations Intelligence Agent Rules", 2),

        J("ZOMATO-OPS-ARCH-001", "0.1.0",
          "Architecture and System Boundaries > 6. Security and data boundaries", 2),

        J("ZOMATO-OPS-RB-001", "1.0.0-draft",
          "4. Standard Incident Response Procedure > Step 6: Separate facts from hypotheses", 1),

        J("ZOMATO-OPS-DATA-001", "0.1.0",
          "Source Data Contracts > 6. Data classification and handling contract", 2),
    ],
}


# ============================================================
# 4. VALIDATE THE JUDGEMENT DATASET
# ============================================================

if len(TEST_CASES) != 15:
    raise ValueError("Expected exactly 15 evaluation questions.")

if set(JUDGEMENTS) != {x["id"] for x in TEST_CASES}:
    raise ValueError("Question IDs and judgement IDs do not match.")

JUDGEMENT_LOOKUP = {}

for query_id, labels in JUDGEMENTS.items():

    if len(labels) != TOP_K:
        raise ValueError(
            f"{query_id}: Expected {TOP_K} judgements."
        )

    lookup = {}

    for label in labels:

        identity = (
            label["document_id"],
            label["document_version"],
            label["section_path"],
        )

        if identity in lookup:
            raise ValueError(
                f"{query_id}: Duplicate judgement identity."
            )

        if label["grade"] not in (0, 1, 2):
            raise ValueError("Invalid relevance grade.")

        lookup[identity] = label["grade"]

    JUDGEMENT_LOOKUP[query_id] = lookup


# ============================================================
# 5. METRIC FUNCTIONS
# ============================================================

def calculate_dcg(grades):

    return sum(
        (2 ** grade - 1) / math.log2(rank + 1)
        for rank, grade in enumerate(grades, start=1)
    )


def calculate_ndcg_pool(grades):

    actual_dcg = calculate_dcg(grades)

    ideal_grades = sorted(grades, reverse=True)

    ideal_dcg = calculate_dcg(ideal_grades)

    if ideal_dcg == 0:
        return 0.0

    return actual_dcg / ideal_dcg


# ============================================================
# 6. RUN EVALUATION
# ============================================================

DETAIL_ROWS = []
METRIC_ROWS = []

for case in TEST_CASES:

    query_id = case["id"]

    print("\n" + "=" * 90)
    print(f"Evaluating {query_id}")
    print("=" * 90)

    results = search_runbooks(
        query=case["query"],
        top_k=TOP_K,
    )

    if len(results) != TOP_K:
        raise ValueError(
            f"{query_id}: Expected {TOP_K} results, "
            f"received {len(results)}."
        )

    lookup = JUDGEMENT_LOOKUP[query_id]

    seen = set()
    grades = []

    for rank, row in enumerate(results, start=1):

        identity = (
            row.get("document_id"),
            row.get("document_version"),
            row.get("section_path"),
        )

        if identity not in lookup:
            raise ValueError(
                f"{query_id}: Unjudged retrieval result detected.\n"
                f"DOCUMENT: {identity[0]}\n"
                f"VERSION: {identity[1]}\n"
                f"SECTION: {identity[2]}\n"
                "Stop evaluation and manually review this new result."
            )

        if identity in seen:
            raise ValueError(
                f"{query_id}: Duplicate retrieved section."
            )

        seen.add(identity)

        grade = lookup[identity]

        if row.get("project_retrieval_eligible") is not True:
            raise AssertionError(
                f"{query_id}: Ineligible chunk returned."
            )

        if not row.get("chunk_id"):
            raise ValueError(f"{query_id}: Missing chunk ID.")

        grades.append(grade)

        DETAIL_ROWS.append({
            "query_id": query_id,
            "rank": rank,
            "chunk_id": row["chunk_id"],
            "document_id": row["document_id"],
            "document_version": row["document_version"],
            "section_path": row["section_path"],
            "relevance_grade": grade,
        })

        label = {
            2: "HIGHLY RELEVANT",
            1: "PARTIALLY RELEVANT",
            0: "IRRELEVANT",
        }[grade]

        print(
            f"Rank={rank} | Grade={grade} | {label} | "
            f"{row['document_id']} | {row['section_path']}"
        )

    # Precision@5
    relevant_count = sum(grade > 0 for grade in grades)

    precision = relevant_count / TOP_K

    # Hit Rate@5
    hit_rate = int(relevant_count > 0)

    # MRR@5
    first_relevant_rank = next(
        (rank for rank, grade in enumerate(grades, 1)
         if grade > 0),
        None
    )

    reciprocal_rank = (
        1 / first_relevant_rank
        if first_relevant_rank is not None
        else 0.0
    )

    # Graded NDCG against the judged top-5 candidate pool.
    ndcg = calculate_ndcg_pool(grades)

    METRIC_ROWS.append({
        "query_id": query_id,
        "precision_at_5": precision,
        "hit_rate_at_5": hit_rate,
        "first_relevant_rank": first_relevant_rank,
        "reciprocal_rank_at_5": reciprocal_rank,
        "ndcg_at_5_pool": ndcg,
        "highly_relevant_count": grades.count(2),
        "partially_relevant_count": grades.count(1),
        "irrelevant_count": grades.count(0),
    })


# ============================================================
# 7. AGGREGATE EVALUATION
# ============================================================

query_count = len(METRIC_ROWS)

mean_precision = sum(
    x["precision_at_5"] for x in METRIC_ROWS
) / query_count

mean_hit_rate = sum(
    x["hit_rate_at_5"] for x in METRIC_ROWS
) / query_count

mean_mrr = sum(
    x["reciprocal_rank_at_5"] for x in METRIC_ROWS
) / query_count

mean_ndcg = sum(
    x["ndcg_at_5_pool"] for x in METRIC_ROWS
) / query_count

total_high = sum(
    x["highly_relevant_count"] for x in METRIC_ROWS
)

total_partial = sum(
    x["partially_relevant_count"] for x in METRIC_ROWS
)

total_irrelevant = sum(
    x["irrelevant_count"] for x in METRIC_ROWS
)


# ============================================================
# 8. DISPLAY RESULTS
# ============================================================

print("\n" + "#" * 90)
print("DAY 23 - FINAL HUMAN-JUDGED RETRIEVAL EVALUATION")
print("#" * 90)

print(f"Queries evaluated       : {query_count}")
print(f"Chunks evaluated        : {len(DETAIL_ROWS)}")
print(f"Top-K                   : {TOP_K}")

print("\nRELEVANCE DISTRIBUTION")
print(f"Highly relevant         : {total_high}")
print(f"Partially relevant      : {total_partial}")
print(f"Irrelevant              : {total_irrelevant}")

print("\nAGGREGATE METRICS")
print(f"Precision@5             : {mean_precision:.4f}")
print(f"Hit Rate@5              : {mean_hit_rate:.4f}")
print(f"MRR@5                   : {mean_mrr:.4f}")
print(f"NDCG@5 (judged pool)    : {mean_ndcg:.4f}")

print(
    "\nRecall@5                : NOT CALCULATED "
    "(complete relevant-chunk inventory required)"
)

print("\n" + "#" * 90)
print("QUERY-LEVEL METRICS")
print("#" * 90)

metrics_df = spark.createDataFrame(METRIC_ROWS)
details_df = spark.createDataFrame(DETAIL_ROWS)

display(
    metrics_df.orderBy("query_id")
)
SELECT
    ai_query(
        'system.ai.gpt-oss-20b',

        'Return status as MODEL_TEST_PASS,
         answer as LLM endpoint is responding,
         and used_chunk_ids as an empty array.',

        responseFormat =>
            'STRUCT<
                status:STRING,
                answer:STRING,
                used_chunk_ids:ARRAY<STRING>
            >'
    ) AS model_test;


print("\nCOMPLETE HUMAN-JUDGED RETRIEVAL DETAILS")

display(
    details_df.orderBy("query_id", "rank")
)

print("\nDAY 23 RELEVANCE EVALUATION: COMPLETED")
