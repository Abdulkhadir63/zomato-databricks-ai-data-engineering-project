<h1 align="center">
  <img src="https://cdn.simpleicons.org/zomato/E23744" width="30" height="30" alt="Zomato"/>
  &nbsp;ZOMATO-DATABRICKS-AI-DATA-ENGINEERING-PROJECT&nbsp;
  <img src="https://cdn.simpleicons.org/databricks/FF3621" width="30" height="30" alt="Databricks"/>
</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/AWS-Amazon%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3"/>
  <img src="https://img.shields.io/badge/Apache-Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/Delta-Lake-00A6D6?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/MLflow-Observability-0194E2?style=for-the-badge&logo=mlflow&logoColor=white" alt="MLflow"/>
</p>

<p align="center">
  <b>A Databricks Lakehouse for Zomato data, connected to an evidence-driven Operations Intelligence Agent.</b><br/>
  Seven source datasets • Bronze / Silver / Gold • Data quality • AI review enrichment • Genie analytics • RAG • AI Search • MLflow tracing • Human-controlled recovery
</p>

<p align="center">
  <a href="#-lets-talk-about-what-i-built">Project overview</a>
  •
  <a href="#-architecture">Architecture</a>
  •
  <a href="#-the-30-day-build-journey">30-day build</a>
  •
  <a href="#-repository-structure">Repository</a>
  •
  <a href="#-running-the-project">Run the project</a>
</p>

---

<h2 align="center">Let's Talk About What I Built</h2>

I built a Zomato data platform on Databricks and connected it to an Operations Intelligence Agent that helps investigate pipeline failures.

The project solves two related problems.

1. **The data problem:** Zomato source files need to become clean, traceable and useful analytical tables. Loading CSV files is only the first step. The platform also needs schema control, data quality checks, deduplication, reliable reruns and business-ready outputs.
2. **The operational problem:** when a pipeline fails, an engineer needs to understand which job and task failed, what the pipeline reported, whether the source schema changed, whether data quality checks failed and which recovery procedure applies. A model cannot know these facts just by receiving an error message.

I built the data platform first. I then built the investigation system around the operational evidence produced by that platform.

The data flow is:

```text
ZOMATO DATA PLATFORM

Zomato CSV datasets
        ↓
     AWS S3 RAW
        ↓
Lakeflow Declarative Pipelines
        ↓
      Bronze
        ↓
      Silver
        ↓
 Data Quality / Quarantine
        ↓
       Gold
        ↓
Facts, dimensions and marts
        ↓
SQL analytics / Genie
```

The Operations Intelligence flow is:

```text
PIPELINE OPERATIONS

Lakeflow Job
      ↓
Zomato pipeline task
      ↓
Success or failure
      ↓
Failure-handler path
      ↓
Capture job / run / task evidence
      ↓
Persist incident evidence in Delta
      ↓
Operations Intelligence Agent
      ↓
Read-only operational tools + runbook retrieval
      ↓
Structured investigation report
      ↓
Persist report and trace references
      ↓
Human reviews and performs any recovery action
```

The important design decision is that the AI agent does not own the data pipeline and does not repair production data by itself. The pipeline processes data. The operations workflow collects evidence. The agent investigates and recommends. A human remains responsible for recovery.

This is a portfolio engineering project built with production-oriented practices. It is not a certified enterprise service, and it does not claim an enterprise SLA, distributed exactly-once execution or autonomous self-healing.

---

<h2 align="center">🏗️ Architecture</h2>

I divided the project into two main parts: the **data plane** and the **operations/control plane**.

```text
DATA PLANE
→ Reads, validates, transforms and serves Zomato data.
```

```text
OPERATIONS / CONTROL PLANE
→ Observes pipeline execution, collects incident evidence,
  investigates failures and records the result.
```

The data plane is responsible for the actual records. It starts with source files in S3 and produces Delta tables through Bronze, Silver and Gold.

The operations plane is responsible for the lifecycle around a pipeline run. It uses Databricks job and pipeline metadata, evidence snapshots, governed read-only tools, operational runbooks, AI Search, the investigation workflow and durable Delta records.

### Complete data platform

```text
                         AWS S3
                    raw-source/ prefix
                           │
                           ▼
             Lakeflow Declarative Pipelines
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
          Bronze                    Source metadata
             │
             ▼
           Silver
             │
       ┌─────┴──────────┐
       ▼                ▼
   Valid data       Quarantine / DQ
       │                │
       ▼                └──→ Quality evidence
      Gold
       │
  ┌────┴─────────────┐
  ▼                  ▼
Facts / dimensions  Analytical marts
  │                  │
  └────────┬─────────┘
           ▼
     Genie / SQL analytics
```

### Complete Operations Intelligence path

```text
                 Lakeflow Jobs
                      │
                      ▼
             Zomato pipeline task
                      │
             ┌────────┴────────┐
             ▼                 ▼
          SUCCESS            FAILURE
             │                 │
       Normal workflow        ▼
                     Failure-handler task
                              │
                              ▼
                    Run/task context capture
                              │
                              ▼
                    Incident evidence snapshot
                              │
                              ▼
                     Delta persistence
                              │
                              ▼
                 Operations Intelligence Agent
                       │             │
                       │             └──→ AI Search
                       │                    │
                       │             operations_runbook_index
                       │
                       └──→ Read-only tools
                              │
                              ▼
                    Evidence reconciliation
                              │
                              ▼
                    Structured incident report
                              │
                              ▼
                   Durable report / MLflow trace
                              │
                              ▼
                        Human review
                              │
                              ▼
                 Human-controlled recovery
```

The incident evidence is written before the model is invoked. This ordering matters. If model serving, search or response generation fails, the incident still has a durable record that can be investigated again.

### Why I kept these responsibilities separate

I did not build one large application that handles ingestion, transformations, analytics, incident management and recovery in the same code path.

Each component has a clear job:

| Component | Responsibility |
|---|---|
| AWS S3 | Stores the original Zomato source files and project knowledge documents. |
| Lakeflow Declarative Pipelines | Defines and runs the Bronze, Silver and Gold data transformations. |
| Delta Lake | Stores the data layers and operational Delta tables. |
| Unity Catalog | Provides the governance and access-control layer for Databricks data objects and governed functions. |
| Lakeflow Jobs | Orchestrates pipeline execution and the failure-handling path. |
| Knowledge ingestion | Parses operational documents and prepares them for retrieval. |
| Databricks AI Search | Finds relevant sections from the project runbooks. |
| Operations Intelligence Agent | Connects live operational evidence with the retrieved procedures and produces an investigation. |
| MLflow | Records the execution trace of the agent workflow where supported by the workspace. |
| Human operator | Reviews the proposed next steps and performs approved recovery actions. |

---

<h2 align="center">📦 Zomato Source Data</h2>

The project uses seven CSV datasets. I profiled the files before building the transformations so that the pipeline was based on the actual source structure rather than assumptions about what a Zomato dataset should contain.

The source files are stored under the project's S3 raw prefix:

```text
s3://databricks-zomato-ai-datalake/raw-source/
```

The seven datasets and the approximate row counts recorded during profiling are:

| Dataset | Approximate rows | What it represents in this project |
|---|---:|---|
| `food` | 373,632 | Food reference data used by downstream food and menu transformations. |
| `menu` | 1,179,936 | Menu records and their supported relationships to restaurants and food. |
| `order_items` | 22,998,179 | Order-line level records. This is the largest profiled source. |
| `orders` | 10,000,000 | Order-level records. |
| `restaurant` | 148,544 | Restaurant reference and descriptive data. |
| `reviews` | 300,000 | Customer review records used for review analytics and AI enrichment. |
| `users` | 100,000 | User reference records. |

These are the approximate counts from the project profiling work. They describe the source snapshot used during development, not a live production feed. If the files are replaced or refreshed, the counts can change.

### Why source profiling comes first

Before creating a fact table or a dimension, I need to know:

- Which columns actually exist.
- Which fields have usable data types.
- Which identifiers can act as business keys.
- Which columns contain nulls or malformed values.
- Whether duplicate records exist.
- Whether the relationships between datasets are supported by actual key values.
- Which business questions the data can answer without inventing metrics.

This avoids a common data modelling mistake: building a fact table around assumed relationships and then trying to fix the model after the transformations are already written.

The source profiles also inform the Silver canonical schemas under:

```text
schemas/silver_canonical_schema/
```

There is one YAML schema definition for each of the seven datasets.

---

<h2 align="center">🗂️ Data Plane</h2>

The data plane processes the Zomato records. Its responsibility is to move the data from source files to analytical outputs while preserving enough metadata to understand where each record came from and how it was processed.

The main data path is:

```text
AWS S3 RAW
    ↓
Bronze Delta
    ↓
Silver Delta
    ↓
Data Quality
    ↓
Gold Delta
```

The data plane uses:

| Technology | How I use it | What problem it solves |
|---|---|---|
| AWS S3 | Stores the original CSV source files and knowledge documents. | Separates source storage from processing and keeps the original inputs available. |
| Databricks Lakeflow Declarative Pipelines | Defines and runs the transformations. | Keeps the data dependencies and transformation graph in one pipeline project. |
| Apache Spark / PySpark | Performs distributed data transformations. | Handles the larger datasets and transformation logic. |
| Delta Lake | Stores Bronze, Silver, Gold and operational tables. | Provides a transactional table layer instead of managing loose output files. |
| Auto Loader | Discovers new files for the incremental ingestion pattern. | Avoids treating every later delivery as a full reload. |
| Unity Catalog | Governs catalogs, schemas, tables and governed functions. | Provides a consistent governance layer for Databricks data assets. |
| Databricks SQL / Genie | Queries the curated Gold layer. | Separates analytical consumption from ingestion and transformation. |

The layers are not three copies of the same dataset. Each layer has a different responsibility.

---

<h2 align="center">☁️ AWS S3 — Raw Data Landing</h2>

I use Amazon S3 as the raw storage layer for the project.

The source prefix is:

```text
s3://databricks-zomato-ai-datalake/raw-source/
```

The original CSV files land in S3 before the Databricks pipeline processes them. Keeping the original files separate from the processed Delta tables gives the project a stable source boundary.

The relationship is:

```text
Original Zomato CSV
        ↓
      S3 RAW
        ↓
Lakeflow Declarative Pipeline
        ↓
   Bronze Delta
```

The S3 raw zone has two important responsibilities.

```text
1. Preserve the source files.
2. Provide a stable input location for the ingestion pipeline.
```

The knowledge-base documents use a separate S3 prefix. The project source documents were verified under:

```text
s3://databricks-zomato-ai-datalake/knowledge-base/source/kb_version=0.1.0/
```

I keep operational documents away from the raw business datasets because they serve a different purpose. Zomato records are processed by the Lakehouse. Runbooks and architecture documents are parsed and indexed for incident investigation.

### Why the original files stay in RAW

If a Silver transformation or Gold model needs to be corrected, the source file remains available for a controlled replay or backfill.

The raw zone also helps with investigation. If a pipeline reports a schema issue, an engineer can compare the delivered file with the expected contract instead of relying only on the final transformed table.

S3 does not perform the Spark transformations. It stores the source objects. Databricks reads them and performs the data processing.

---

<h2 align="center">🧱 Bronze Layer</h2>

Bronze is the first Delta layer after the raw files.

Its job is to bring the source records into Databricks while retaining the original source values and enough ingestion metadata to trace the data back to its source.

The flow is:

```text
S3 RAW
   ↓
Read source file
   ↓
Add ingestion metadata
   ↓
Bronze Delta table
```

The project contains Bronze ingestion definitions for all seven datasets:

```text
transformations/bronze_ingestion/
├── bronze_food.py
├── bronze_menu.py
├── bronze_order_items.py
├── bronze_orders.py
├── bronze_restaurant.py
├── bronze_reviews.py
└── bronze_users.py
```

### What Bronze preserves

The Bronze layer stays close to the source representation. The ingestion code also adds metadata used to trace the input and processing context.

The metadata pattern includes:

- `_source_file`
- `_source_file_modification_time`
- `_ingested_at`
- `_rescued_data`

The exact source columns are preserved according to the ingestion definition. Unexpected or rescued values remain visible instead of silently disappearing during the first read.

### Why Bronze does not perform all business cleaning

Bronze is not the place to mix every downstream business rule into the ingestion step.

If a source column changes, I need to be able to inspect what arrived. If I normalize or discard too much at ingestion time, I lose the evidence needed to understand the source change.

The responsibilities are therefore separated:

```text
Bronze
→ Capture the source and ingestion context.

Silver
→ Apply canonical types and transformation rules.

Data Quality
→ Decide whether the transformed records are acceptable.

Gold
→ Produce business-ready outputs.
```

### Why Auto Loader is used

Auto Loader is used for incremental file discovery where new files arrive after the initial source load. Its checkpointing and file-discovery behavior are not the same thing as business-key deduplication.

A checkpoint helps the ingestion process track file processing. It does not prove that two different files cannot contain the same business record. That is handled by the Silver and downstream data rules.

The project does not describe a static CSV snapshot as real-time streaming. The ingestion mode is selected based on the source delivery pattern.

---

<h2 align="center">🧹 Silver Layer</h2>

Silver is where the source-oriented Bronze records become canonical, typed and usable by downstream transformations.

The flow is:

```text
Bronze Delta
     ↓
Canonical schema
     ↓
Type conversion and standardization
     ↓
Required-field handling
     ↓
Deduplication and validation
     ↓
Silver Delta
```

The canonical schemas are stored in:

```text
schemas/silver_canonical_schema/
```

The directory contains:

```text
food.yml
menu.yml
order_items.yml
orders.yml
restaurant.yml
reviews.yml
users.yml
```

### What Silver handles

Silver is responsible for applying the schema and transformation rules consistently.

The work includes:

- Mapping incoming columns to the canonical column names.
- Converting values to the expected types.
- Applying safe date and numeric parsing where the source supports it.
- Normalizing values using explicit rules.
- Handling missing required fields.
- Deduplicating records using defined business keys and deterministic rules where the source supports a reliable ordering field.
- Preserving the source reference needed to investigate a rejected record.
- Making schema differences visible instead of silently accepting them.

### Why canonical schemas are stored separately

A schema should be reviewable without opening every transformation file.

The YAML definitions provide a place to record the expected Silver structure. The transformations consume those definitions and apply the rules to the source data.

This makes schema review easier and helps the Operations Intelligence Agent compare expected and observed schema evidence when a pipeline fails because of a source contract change.

### Deduplication is not the same as file checkpointing

A file checkpoint answers:

```text
Has this file already been discovered or processed by ingestion?
```

Business deduplication answers:

```text
Does this record represent a business entity or event already present in the target?
```

Those are different questions. Silver handles record-level deduplication based on the source contract and business key. The pipeline does not use a random row choice where a deterministic ordering rule is required.

---

<h2 align="center">✅ Data Quality and Quarantine</h2>

A successful Spark task does not automatically mean the data is correct.

That is why the project has explicit data quality rules and quarantine handling.

```text
Silver records
      ↓
Data quality checks
      ↓
  ┌───┴───────────┐
  ▼               ▼
Valid records   Invalid records
  │               │
  ▼               ▼
Gold eligible   Quarantine
                  │
                  ▼
          Rejection reason
          Source reference
          Quality evidence
```

### What the quality layer checks

The rules are based on the source contracts and the actual fields in each dataset. They include checks such as:

- Required identifiers are present where they are required by the contract.
- Numeric fields contain valid values and are within documented ranges.
- Date and timestamp fields can be parsed using the expected source format.
- Business keys do not violate the defined uniqueness rules.
- Supported relationships between datasets are valid.
- Required source columns exist and have compatible types.

Not every invalid row should have the same outcome.

A row-level issue that can be isolated may be quarantined with a rejection reason. A structural or critical contract violation may need to fail the relevant pipeline task. The policy depends on the rule and downstream impact.

### Why quarantine records instead of dropping them

Dropping an invalid record without preserving the reason creates a blind spot.

Quarantine gives the engineer a record of:

- Which source record was rejected.
- Which rule failed.
- What input or source file it came from.
- When the validation happened.
- Why it did not continue to the curated layer.

This makes data quality an observable part of the pipeline rather than a hidden side effect of a filter.

### Data quality and the Operations Intelligence Agent

The agent can inspect DQ results through a read-only operational tool. This allows it to correlate a failed pipeline task with the relevant quality checks instead of guessing from the exception message alone.

---

<h2 align="center">🏆 Gold Layer</h2>

Gold is the analytical layer. It contains the fact tables, dimension tables and marts built from the validated Silver datasets.

The flow is:

```text
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold dimensions and facts
   ↓
Analytical marts
```

The current transformation source includes the following Gold models.

### Dimension tables

| Table | Responsibility |
|---|---|
| `dim_customer` | Customer-related dimension attributes supported by the source model. |
| `dim_date` | Date dimension used for time-based analysis. |
| `dim_food` | Food reference attributes. |
| `dim_restaurants` | Restaurant reference attributes. |

### Fact tables

| Table | Responsibility |
|---|---|
| `fact_orders` | Order-level facts. |
| `fact_order_items` | Order-item level facts. |

### Analytical marts

| Mart | Purpose |
|---|---|
| `mart_daily_city_revenue` | Daily revenue analysis by city. |
| `mart_delivery_sla` | Delivery service-level analysis based on the available delivery fields. |
| `mart_restaurant_performance` | Restaurant performance metrics supported by the source data. |

The source code for these models is under:

```text
transformations/gold_business_ready/
```

### Why the grain matters

A fact table needs a defined grain. For example, an order-level fact and an order-item-level fact are not interchangeable.

If I join an order-level measure to multiple order-item rows without accounting for the change in grain, I can multiply the order measure. This is why the fact tables and analytical marts need independent SQL checks against the underlying source records.

Gold is not just a renamed copy of Silver. It is the model that makes the data useful to business users while preserving valid relationships and measures.

---

<h2 align="center">🧊 Delta Lake</h2>

I use Delta Lake as the table format for the Databricks data layers and the operational records.

The project is not treating S3 as a folder full of files and then asking every downstream consumer to understand the file layout. The data is represented as Delta tables managed through the Databricks and Unity Catalog environment.

The relationship is:

```text
Amazon S3
    ↓
Databricks processing
    ↓
Delta tables
    ↓
Unity Catalog
    ↓
SQL / PySpark / AI workloads
```

Delta tables are used for:

- Bronze source ingestion.
- Silver canonical records.
- Gold facts, dimensions and marts.
- Review enrichment results.
- Data quality and quarantine evidence where configured.
- Parsed runbook and chunk metadata.
- Incident evidence and investigation persistence.
- Approval requests and investigation history.

### Why Delta is useful in this project

The project needs more than the ability to write Parquet files. It needs a managed table layer that the pipeline, SQL queries and operational code can reference consistently.

Delta table history is also one of the evidence sources used during incident investigation. It can help an engineer understand which operations affected a table and whether the table changed around the time of a failure.

The agent only reads the table history. It does not issue writes, restores or destructive table operations.

---

<h2 align="center">⚙️ Apache Spark, PySpark and Lakeflow Declarative Pipelines</h2>

I use Spark and PySpark for the data transformations. Lakeflow Declarative Pipelines provides the pipeline structure and dependency graph.

The relationship is:

```text
Lakeflow Declarative Pipelines
             ↓
       Spark / PySpark
             ↓
    Transform and validate
             ↓
          Delta Lake
```

The pipeline source is organized by processing layer:

```text
transformations/
├── bronze_ingestion/
├── silver_cleaned/
└── gold_business_ready/
```

### Why use Declarative Pipelines

The pipeline definitions describe the datasets and their dependencies. This keeps the transformation graph together instead of scattering the entire workflow across unrelated scripts.

The code still needs to be deterministic and testable. Declarative orchestration does not remove the need to define schemas, handle invalid values or prove that the output is correct.

### Why Spark is used

The source contains large transaction datasets, especially `order_items` and `orders`. Spark provides the distributed data processing engine for reading and transforming those records.

The platform does not add a separate Airflow, dbt or Kafka stack. Lakeflow is the chosen Databricks-native pipeline and job environment for this project.

### What Lakeflow does not solve by itself

A pipeline can execute successfully and still produce incorrect business data if the transformation logic is wrong. That is why the project keeps schema definitions, DQ rules, quarantine and reconciliation alongside the pipeline code.

---

<h2 align="center">🔐 Unity Catalog and Data Governance</h2>

Unity Catalog is the governance layer for the Databricks objects used by the project.

It is responsible for organizing and governing catalogs, schemas, tables and supported functions. It is also the boundary used for read-only operational data access where the project uses governed functions.

The project uses workspace-specific catalog and schema names. The development workspace has objects under namespaces such as:

```text
zomato_dev.gold
zomato_dev.ops_knowledge
zomato_dev.ops_evidence
```

The exact catalog, schema and model-serving resource names should be configured for the target workspace rather than assumed to exist in every Databricks environment.

### Why governance matters to the agent

The Operations Intelligence Agent should not receive unrestricted access to the workspace or all data tables.

Its tools expose a narrow operational interface. The tool can return the evidence needed for an investigation without giving the model a general-purpose write path into the Lakehouse.

The model does not get to decide its own permissions. Access is controlled outside the model through the Databricks environment and the tool implementation.

---

<h2 align="center">🔗 How the Data Plane Connects Together</h2>

The seven source datasets are processed through the same overall pattern, while each dataset keeps its own source and canonical schema.

```text
S3 RAW
  │
  ├── food.csv ───────────────→ Bronze food ─────→ Silver food ─────┐
  ├── menu.csv ───────────────→ Bronze menu ─────→ Silver menu ─────┤
  ├── order_items.csv ────────→ Bronze items ────→ Silver items ────┤
  ├── orders.csv ─────────────→ Bronze orders ───→ Silver orders ───┤
  ├── restaurant.csv ─────────→ Bronze restaurant → Silver restaurant┤
  ├── reviews.csv ────────────→ Bronze reviews ───→ Silver reviews ──┤
  └── users.csv ──────────────→ Bronze users ─────→ Silver users ────┘
                                                                    │
                                                                    ▼
                                                        DQ / quarantine rules
                                                                    │
                                                                    ▼
                                                              Gold layer
                                                                    │
                                          ┌─────────────────────────┴──────────┐
                                          ▼                                    ▼
                                   Fact / dimensions                    Business marts
                                          │                                    │
                                          └──────────────────┬─────────────────┘
                                                             ▼
                                                     SQL / Genie analytics
```

The source data does not all have the same grain or key structure. The pipeline therefore shares the layer pattern without forcing all seven datasets through one generic schema.

The data plane produces the tables. The Operations Intelligence Agent uses the resulting operational evidence to explain pipeline behavior. These are connected, but they are not the same workload.

---

<h2 align="center">🤖 AI-Powered Review Enrichment</h2>

The project has a separate AI workload for customer reviews.

The purpose is to add structured classification output to the review data so that it can be analyzed alongside the other Gold data. This workload is not the failure-investigation agent and does not use the operational runbook index.

The general flow is:

```text
Silver reviews
      ↓
Validate review input
      ↓
Databricks ai_classify
      ↓
Validate classification output
      ↓
Persist enrichment result in Delta
      ↓
Downstream analysis
```

The implementation lives under:

```text
reviews_ai_enrichment/
└── zomato_reviews_enrich.py
```

The enrichment table created during the project is:

```text
ai_data.enriched_reviews
```

### Why I use classification instead of generating long summaries

The project needs structured review attributes that can be checked and analyzed. Sentiment and review-aspect labels are a more controlled output for this use case than asking a model to generate arbitrary paragraphs for every review.

The classification contract needs to define which labels are allowed and what happens when the input is empty, the response is malformed or inference fails.

### Why enrichment is persisted separately

The original review record should not be overwritten just because an AI classification was added.

Keeping the enrichment output separately makes it possible to:

- Preserve the original review.
- Track which review was classified.
- Store classification status and processing metadata.
- Re-run enrichment in a controlled way.
- Inspect failures without losing successful results.

The project treats an AI result as derived data. It does not treat model output as a replacement for the source review.

---

<h2 align="center">📊 Databricks Genie — Business Analytics</h2>

Genie is the natural-language analytics interface for the curated Gold layer.

The business analytics workload is separate from the Operations Intelligence Agent. Genie answers questions about the analytical data. The custom Operations Intelligence Agent investigates pipeline incidents.

The relationship is:

```text
Gold facts / dimensions / marts
              ↓
       Approved Genie context
              ↓
       Natural-language question
              ↓
       SQL-backed analysis
              ↓
        Result for the user
```

### Why I use Genie instead of building another SQL agent

The Gold tables already contain the curated business model. Building a second custom text-to-SQL agent would duplicate a responsibility that Genie is intended to provide in this architecture.

The quality of the answer still depends on the quality of the Gold model and the context provided to Genie. Table grain, column descriptions, metric definitions and example questions matter.

A natural-language answer should be checked against the underlying SQL and the actual table contents. If the source does not contain a metric, the system should not invent it.

---

<h2 align="center">🎛️ Operations / Control Plane</h2>

The Operations Intelligence part of the project exists because data pipelines fail for reasons that are not always visible from a single job status.

A useful investigation needs to answer questions such as:

```text
Which job failed?
Which run failed?
Which task failed?
What was the actual task-level result?
What error or exception was returned?
Was a required source column missing?
Did a data quality rule fail?
Did the source schema change?
Which tables may be affected downstream?
Which runbook procedure applies?
What evidence is still missing?
What is safe to do next?
```

The agent should not guess these answers. It needs tools that retrieve the relevant operational metadata and runbooks.

The operations flow is:

```text
Lakeflow Jobs
      ↓
Task / pipeline execution
      ↓
Operational metadata and failure evidence
      ↓
Incident evidence snapshot
      ↓
Durable Delta record
      ↓
Operations Intelligence Agent
      ↓
Investigation report
      ↓
Human review
```

The operations plane is not another data ingestion pipeline. It is the control and investigation workflow around the existing Lakehouse.

### Structured evidence and unstructured knowledge are different

The project deliberately separates two kinds of information.

```text
STRUCTURED EVIDENCE
→ Job runs, task status, error details, DQ results,
  schema observations, Delta history and lineage.
```

```text
UNSTRUCTURED KNOWLEDGE
→ Project-owned runbooks, data contracts,
  architecture notes and recovery procedures.
```

Structured evidence tells the agent what happened in this run. Runbooks tell the agent what the project procedure says should be inspected or considered.

The agent combines them. It does not treat a runbook as proof that a particular failure happened.

---

<h2 align="center">🔀 Lakeflow Jobs — Workflow Orchestration</h2>

Lakeflow Jobs coordinates the execution of the Zomato pipeline and its related operational tasks.

The job layer is responsible for ordering and invoking tasks. The pipeline itself remains responsible for the data transformations.

The simplified flow is:

```text
Lakeflow Job
     ↓
Zomato pipeline task
     ↓
  ┌──┴─────┐
  ▼        ▼
Success  Failure
  │        │
  ▼        ▼
Continue  Failure handler
normal        │
workflow      ▼
        Incident capture
```

### Why I use Lakeflow Jobs

The orchestration logic belongs in the Databricks environment where the pipeline runs. It avoids introducing another scheduler for this project.

The task graph provides a place to define dependencies and the failure-handler path. The job also supplies run and task context that the operations workflow can use when collecting evidence.

### Job status and task status must not be confused

An orchestrated job can have an overall result that is different from the status of an individual task. For incident investigation, the handler must inspect the relevant task-level execution and correlate it to the pipeline event. Looking only at a top-level job result can miss a failed child task in some execution patterns.

That is why the investigation code has a dedicated pipeline-event correlation module rather than treating the parent job status as the only source of truth.

---

<h2 align="center">❌ Failure Handler and Automatic Incident Capture</h2>

The failure-handler path connects a failed pipeline task to the Operations Intelligence Agent.

The intended sequence is:

```text
Pipeline task fails
       ↓
Failure-handler task executes
       ↓
Read job / run / task context
       ↓
Create stable incident identity
       ↓
Capture operational evidence
       ↓
Persist evidence in Delta
       ↓
Invoke investigation workflow
       ↓
Persist report and outcome
```

### Why evidence is persisted before invoking the model

A model endpoint is another service dependency. It can return an error, time out or be unavailable.

If the failure handler invokes the model first and only saves the incident after a successful response, then an AI outage can also erase the operational record of the pipeline failure.

The project uses the safer ordering:

```text
Capture evidence
      ↓
Persist evidence
      ↓
Invoke model / retrieval
      ↓
Persist investigation report
```

This keeps the incident record independent from the success or failure of the AI call.

### Incident context passed to the agent

The event path uses stable identifiers so that the investigation can correlate the run, the request, the trace and individual tool calls.

The identity contract includes:

| Identifier | Meaning |
|---|---|
| `incident_id` | Stable identity for the incident. The established Job-run identity pattern is `zomato-jobrun:<run_id>`. |
| `request_id` | Identity of one logical investigation request and its idempotency boundary. |
| `run_id` | Databricks Job run identifier. |
| `trace_id` | Identity of one actual traced execution. |
| `tool_call_id` | Identity of an individual logical tool call within an investigation. |
| `task_key` / task run ID | Identifies the task being investigated when available. |
| `pipeline_update_id` | Correlates the relevant pipeline update when available. |

The identifiers have different purposes. A request ID should not be regenerated for the same redelivered event, and a trace ID should not be reused across separate investigation executions.

### What happens if the handler or agent fails

The evidence record should remain queryable even when the investigation report is not available yet. The incident can then be retried or reviewed through a controlled process without losing the original run context.

A failure in the AI layer must not be reported as if the pipeline failure was solved. The operation status and the investigation status are separate facts.

---

<h2 align="center">🗄️ Incident Evidence and Operational Tables</h2>

The project stores operational investigation state in Delta tables rather than keeping it only inside a notebook variable or model conversation.

The established operational namespaces include:

```text
zomato_dev.ops_knowledge
zomato_dev.ops_evidence
```

The core evidence and persistence objects include:

| Object | Purpose |
|---|---|
| `ops_evidence.investigation_runs` | Stores investigation request and execution history. |
| `ops_evidence.incident_reports_current` | Holds the current report projection for an incident. |
| `ops_evidence.approval_requests` | Stores human approval requests and decisions. |
| `ops_knowledge.*` | Holds document, chunk and retrieval-related knowledge objects and views. |

The exact fully qualified names depend on the configured development catalog. The examples above use the project's development namespace.

### Why store both history and the current report

An investigation is an event that can happen more than once. An engineer may inspect an incident, receive new evidence and run another investigation later.

The system therefore needs to retain the request/execution history while also making the latest report easy to retrieve.

The history answers:

```text
What investigations were performed?
Which request triggered them?
What was the outcome?
```

The current projection answers:

```text
What is the latest investigation report for this incident?
```

This avoids treating the latest report as the only record of what happened.

---

<h2 align="center">📚 Operations Knowledge Base</h2>

The Operations Intelligence Agent needs project-specific operational knowledge. A generic model does not know the exact source contracts, table dependencies, quality rules or recovery practices in this repository.

I created a project-owned knowledge base containing operational documents such as:

- Pipeline architecture and component boundaries.
- Source data dictionary and source contracts.
- Data quality rules and quarantine behavior.
- Pipeline runbooks.
- Failure investigation and recovery procedures.
- Incremental processing and replay guidance.
- Deployment and troubleshooting notes.

The source documents are stored under a versioned S3 prefix:

```text
s3://databricks-zomato-ai-datalake/knowledge-base/source/kb_version=0.1.0/
```

The source prefix was verified to contain seven Markdown documents and a `document-manifest.json` file in the documented version.

### These are project runbooks, not official Zomato documentation

The documents describe the implementation built for this portfolio project. They are useful to the project's own agent because they explain the project's actual components and operating assumptions.

They must not be presented as official Zomato corporate procedures or as externally certified production runbooks.

The runbook lifecycle metadata is preserved. Project retrieval eligibility means a document may be used by this project's agent. It does not mean the document has passed an external production approval process.

### Why document provenance matters

The agent should be able to explain where a retrieved procedure came from. It should not return a sentence that looks authoritative while losing the source document and version behind it.

That is why document identity, version, content hash and section metadata are carried through parsing and chunking.

---

<h2 align="center">📄 Document Parsing and Provenance</h2>

The knowledge ingestion process converts source documents into structured records that can be inspected and searched.

The process is:

```text
Source PDF / DOCX / Markdown
             ↓
        Parse document
             ↓
      Preserve metadata
             ↓
       Parsed Delta data
             ↓
        Chunk sections
             ↓
       Search-ready data
```

The implementation is organized under:

```text
knowledge_ingestion/
├── 02_parse_markdown/
│   └── parse_markdown.py
├── 03_parse_pdf_docx/
│   └── parse_pdf.py
└── 06_chunk_documents/
    └── chunk_documents.py
```

### What is preserved during parsing

The parsed representation should retain as much source context as the parser can reliably provide:

- Document ID and title.
- Source URI or path.
- Document version.
- Content hash.
- Parsing status.
- Page information where available.
- Section or heading information where available.
- Extracted text.

Parsing is not considered successful simply because a file was read. The extracted content needs to be inspected for missing paragraphs, broken tables, malformed text and lost section boundaries.

### Why the manifest exists

The knowledge source includes a document manifest. It provides a controlled inventory of the documents and their version information. This helps distinguish the content that is currently expected from older or replaced documents.

The manifest is part of the source-management process. It is not a substitute for inspecting actual parsing output.

---

<h2 align="center">✂️ Chunking and Metadata</h2>

After parsing, the documents are divided into smaller sections for retrieval.

The project uses section-aware chunking where headings and document structure are available. A runbook section should remain meaningful when it is retrieved on its own.

The flow is:

```text
Parsed document
      ↓
Read headings / sections
      ↓
Split oversized sections where needed
      ↓
Attach document and section metadata
      ↓
Runbook chunk Delta table
```

### Chunk metadata

The search-ready records preserve metadata such as:

- `document_id`
- `chunk_id`
- `chunk_text`
- `source_uri`
- `page_number`, where available
- `section_title`
- `document_version`
- `content_hash`
- document type and lifecycle metadata

### Why metadata is as important as the chunk text

A chunk without a document reference is just a piece of text. It cannot reliably tell the investigator which runbook it came from or whether the document is current.

Metadata allows the retrieval layer and final response to preserve provenance. It also gives the project a way to identify changed or stale source content.

The project deliberately chunks operational documents. It does not create a separate customer-review RAG system. Review analytics are handled by the Lakehouse, AI enrichment and Genie.

---

<h2 align="center">🔎 Databricks AI Search</h2>

Databricks AI Search is used for one specific purpose in this project: retrieving operational runbook content for the Operations Intelligence Agent.

The index source is the runbook chunk Delta table. The selected index name is:

```text
operations_runbook_index
```

The retrieval path is:

```text
Runbook source documents
          ↓
Parsed document records
          ↓
Section-aware chunks
          ↓
Runbook chunk Delta table
          ↓
Databricks AI Search
          ↓
operations_runbook_index
          ↓
Relevant runbook chunks
```

### Why there is one operational index

The project does not need multiple vector databases or separate retrieval systems for every data domain.

The agent needs to retrieve procedures about pipeline stages, source contracts, schema changes, data quality and recovery. Those documents belong in one operational knowledge source with clear metadata.

### What AI Search does not do

AI Search does not know whether a specific Databricks Job run failed. It retrieves content from the indexed documents.

Live run metadata, failed task information, data quality results and schema evidence come from the operational tools. The agent combines those structured facts with the retrieved runbook content.

This distinction is central to the design:

```text
AI Search
→ What does the project runbook say?

Operational tools
→ What happened in this run?
```

---

<h2 align="center">🧠 RAG — Grounded Investigation Responses</h2>

Retrieval-augmented generation connects the retrieved runbook sections with the incident evidence.

The basic flow is:

```text
Incident evidence
      │
      ├──→ Read-only operational tools
      │
      └──→ Error category and observed symptoms
                          │
                          ▼
                    Runbook search
                          │
                          ▼
                 Retrieved chunks
                          │
                          ▼
                 Context assembly
                          │
                          ▼
                 Model response
                          │
                          ▼
                Citation / grounding checks
                          │
                          ▼
                 Structured report
```

### Why I do not send the whole knowledge base to the model

The model receives the relevant evidence and retrieved sections for the current investigation. It does not need every runbook or every row in the Lakehouse.

This reduces irrelevant context and makes it easier to check whether the answer is supported by the retrieved material.

### Grounding and abstention

The response generator checks that citations refer to chunks that were actually retrieved. A citation that was not in the retrieval result cannot be treated as valid evidence.

When the available runbook content does not support a safe answer, the system should return an insufficient-evidence outcome rather than invent a recovery procedure.

The same rule applies to operational evidence. A model-generated explanation is not a replacement for the actual job, task, schema or DQ result.

---

<h2 align="center">🤖 Operations Intelligence Agent</h2>

The Operations Intelligence Agent is the main AI component of the project.

It is a custom Python investigation workflow. It is not a generic chatbot and it is not designed to repair the pipeline automatically.

The agent receives a known incident or Job run identity. It reads the incident context, identifies the failure category, selects the evidence tools relevant to that category, searches the operational runbooks and produces a structured report.

The workflow is:

```text
Incident / Job run ID
        ↓
Load incident context
        ↓
Identify failed task and error category
        ↓
Select the relevant evidence tools
        ↓
Collect operational evidence
        ↓
Search the project runbooks
        ↓
Compare evidence with procedure
        ↓
Build structured investigation report
        ↓
Persist result and trace references
```

### The agent does not call every tool for every incident

Calling all available tools for every problem creates noise and wastes execution time. A missing source column needs schema evidence and the relevant source contract. A Delta conflict needs table-history evidence. A downstream impact question needs lineage or dependency evidence if that information is available.

The workflow therefore selects tools based on the incident and the evidence already collected.

### What the report must distinguish

The investigation report separates:

- **Confirmed facts:** values returned by operational tools or observed in the incident record.
- **Likely cause:** the explanation best supported by the available evidence.
- **Alternative causes:** other plausible explanations that are not ruled out.
- **Missing evidence:** information needed to strengthen or confirm the diagnosis.
- **Runbook references:** retrieved project documentation supporting the recommended procedure.
- **Safe next steps:** actions an engineer can inspect or consider.
- **Approval requirement:** whether a data-changing action must be reviewed by a human.

The agent must not present a hypothesis as a confirmed fact. When the evidence is partial, the report must say so.

### A real investigation example

The project was tested against a historical failed Databricks Job run:

```text
Job run ID: 748007191590550
Incident category: SCHEMA_DRIFT
Subtype: SOURCE_CONTRACT_VIOLATION_MISSING_REQUIRED_COLUMN
Missing column: user_id
Affected dataset: users
Evidence completeness: PARTIAL
```

The workflow correlated the failure with the `users` dataset and the missing `user_id` field. The evidence was explicitly marked partial. The agent did not claim that every possible cause had been ruled out.

This is the behavior I want from an operational agent: state what the evidence supports, identify what is missing and avoid pretending that a plausible explanation is certain.

---

<h2 align="center">🧰 Read-Only Operational Tools</h2>

The agent interacts with the Databricks environment through a small set of operational tools.

The registered tool set contains eight tools:

| Tool | Responsibility |
|---|---|
| `list_job_runs` | Lists relevant Databricks Job runs. |
| `get_job_run_details` | Reads job and task execution details. |
| `get_incident_context` | Loads the incident and its stored evidence context. |
| `get_data_quality_results` | Reads related data quality results. |
| `inspect_schema_changes` | Inspects expected-versus-observed schema evidence. |
| `get_delta_table_history` | Reads relevant Delta table history. |
| `search_runbooks` | Retrieves relevant chunks from the operational knowledge index. |
| `get_downstream_impact` | Reads available lineage or dependency evidence for downstream impact. |

The tools are implemented in the existing Operations modules rather than being copied into a second agent application.

### Why the tools are read-only

The purpose of the agent is to investigate and explain. It does not need permission to overwrite a Delta table, reset a checkpoint, change a schema, delete a record or restart a job in order to produce an incident report.

The boundary is:

```text
Agent
  ↓
Read operational evidence
  ↓
Explain and recommend
  ↓
Human review
  ↓
Human performs recovery
```

The model is not given a general-purpose SQL write tool or unrestricted workspace access.

### Tool failures are evidence too

A tool can return no result, a permission error, a timeout or a partial response. The investigation needs to preserve that outcome rather than quietly treating a failed tool as if it returned an empty successful result.

The report should make clear which evidence was collected and which evidence could not be retrieved.

---

<h2 align="center">🔍 Incident Investigation Workflow</h2>

The investigation workflow is the code that connects the tools, retrieval and response generation.

The sequence is deliberately bounded:

```text
1. Receive incident or run identity
             ↓
2. Load incident context
             ↓
3. Identify failed job / task / dataset
             ↓
4. Classify the observed failure
             ↓
5. Select relevant tools
             ↓
6. Retrieve structured evidence
             ↓
7. Search the runbook index
             ↓
8. Compare evidence with the procedure
             ↓
9. Validate the response structure and citations
             ↓
10. Persist the report and execution outcome
```

### Why the workflow has an explicit order

The agent should not start writing a root-cause explanation before it knows which task failed and what evidence is available.

The first steps establish identity and retrieve the structured facts. Tool selection is based on the observed failure. Retrieval supplies the relevant project procedure. Response generation comes after evidence collection, not before it.

### Failure classification

The workflow uses explicit categories and subtypes. This prevents every error from being treated as the same generic pipeline failure.

The investigation should preserve an `UNKNOWN` or `UNDETERMINED` outcome when the error does not support a reliable classification. It is safer to ask for the missing error class or pipeline event than to fabricate a diagnosis.

### Workflow termination

An investigation should stop when the required evidence has been collected or when the remaining evidence cannot be obtained. It should not keep calling tools without a clear reason.

The outcome also records whether evidence is complete, partial or insufficient. A completed workflow execution does not automatically mean the root cause is fully proven.

---

<h2 align="center">🔐 Idempotency, Persistence and Recovery Safety</h2>

The agent needs durable state because investigations can be retried, redelivered or run again by an engineer.

A model response stored only in notebook output is not an operational record. The project persists investigation requests, reports and approval state in Delta.

### Stable identities

The implementation keeps separate identifiers for separate concepts:

| Identifier | Why it exists |
|---|---|
| `incident_id` | Identifies the underlying incident. |
| `request_id` | Identifies one logical investigation request and the idempotency boundary. |
| `run_id` | Identifies the Databricks Job run being investigated. |
| `trace_id` | Identifies one execution trace. |
| `tool_call_id` | Identifies a logical tool call. |

For a Job-run incident, the established incident identity pattern is:

```python
incident_id = f"zomato-jobrun:{run_id}"
```

A redelivery of the same logical failure event must reuse the same request ID. A deliberately new manual investigation can use a new request ID while still referring to the same incident.

### What idempotent request handling does

The persistence layer reserves a logical request and records its result. If the same request is received again, the workflow can return the existing result rather than executing the same investigation again.

The intended behavior is:

```text
First request
     ↓
Reserve request identity
     ↓
Run investigation
     ↓
Persist report and request state
```

For a duplicate request:

```text
Same request ID arrives again
          ↓
Find existing request
          ↓
Return stored result / duplicate status
          ↓
Do not repeat the same investigation
```

### What has been validated

The Day 28 persistence verification showed:

- A first investigation executed and persisted.
- Replaying the same request ID returned the existing result.
- The duplicate request did not execute a second investigation.
- One history row existed for the request.
- One current incident projection existed.
- The request status was `COMPLETED`.

This proves the tested sequential idempotency path. It does **not** prove concurrent-writer safety, distributed locking or distributed exactly-once execution. Those are separate guarantees and are not claimed here.

### Why history and current state are both retained

The report history provides an audit trail. The current incident projection provides the latest state for an operator-facing view. Both are useful and should not be collapsed into one row that loses previous investigation outcomes.

---

<h2 align="center">✋ Human Approval and Recovery</h2>

The project separates an approval record from an actual recovery operation.

The approval workflow records a request and its decision. The agent remains read-only, including after an approval is recorded. A human or a separately controlled execution boundary must perform the recovery action.

The approval lifecycle is:

```text
Create approval request
          ↓
        PENDING
       /       \
      ▼         ▼
  APPROVED    REJECTED
      │         │
      ▼         ▼
Action may    Action is
pass approval not approved
check         
```

The project tests covered:

- Approval request creation.
- Repeated request behavior.
- `PENDING` does not authorize an action.
- `APPROVED` passes the approval gate for the matching action.
- `REJECTED` does not pass the action approval gate.
- Approval records persist during the tested workflow.
- No SQL repair, pipeline restart or other recovery action is executed by the agent.

### Important approval boundary

The approval module records the reviewer identity supplied to it. It does not independently authenticate the person or prove that the person is authorized. The application or API boundary must enforce identity and permissions before accepting a reviewer decision.

The recovery process remains human-controlled:

```text
Agent investigates
      ↓
Agent recommends
      ↓
Human reviews evidence
      ↓
Human approves or rejects
      ↓
Human performs the required change
      ↓
Pipeline is rerun and reconciled
```

This avoids giving a language model the ability to make destructive production changes.

---

<h2 align="center">📈 MLflow Tracing and Observability</h2>

I integrated MLflow tracing into the real Operations Intelligence investigation workflow rather than leaving tracing as an isolated example.

The trace is intended to make the agent execution inspectable:

```text
Investigation request
        ↓
Root workflow span
        ↓
Selected tool calls
        ↓
Operational tool results
        ↓
Runbook retrieval
        ↓
Model invocation
        ↓
Structured response parsing
        ↓
Investigation result
```

The tracing package is under:

```text
observability/
├── __init__.py
├── agent_tracing.py
├── config.py
├── tracing.py
└── test.py
```

### What tracing helps inspect

- Which investigation workflow ran.
- Which tools were selected.
- What retrieval step was executed.
- Whether a model call and response parser completed.
- Where an error or timeout occurred.
- How the investigation terminated.

### Recorded real-agent trace

The project recorded a successful MLflow trace for a real investigation against the historical failed Job run:

```text
MLflow experiment ID: 1460899472275992
Trace ID: tr-275eedbc9dd5a040975fcdce80769cd2
Job run ID: 748007191590550
```

The trace included the actual investigation workflow, selected governed tools, runbook retrieval, model call and structured response parsing.

### What I do not claim from tracing alone

A successful trace proves that the instrumented workflow executed and was recorded. It does not, by itself, prove a 100% root-cause accuracy rate or a measured improvement in agent quality.

Token usage and cost are included only when the underlying model response path exposes those values. If the workspace does not return them, they are not estimated or invented.

---

<h2 align="center">🧪 Testing and Validation</h2>

The project uses several different kinds of validation. A successful pipeline run, an idempotency test and an agent-grounding test prove different things.

### Data pipeline validation

The data platform needs checks for:

- Source-to-Bronze row counts and schema alignment.
- Presence of ingestion metadata.
- Silver type conversion and canonical schema alignment.
- Required-field behavior.
- Duplicate business-key behavior.
- Valid and quarantined record counts.
- Gold fact grain and dimension key uniqueness.
- Independent SQL reconciliation of analytical measures.
- Repeat processing and replay behavior.

### Operations agent validation

The incident workflow needs checks for:

- Correct incident identity.
- Correct Job and task correlation.
- Relevant tool selection.
- Tool failures and partial results.
- Schema evidence and DQ evidence.
- Runbook retrieval relevance.
- Citation membership in retrieved chunks.
- Structured report validation.
- Explicit missing-evidence reporting.
- Safe abstention for unknown or unsupported errors.
- No data-changing action from the agent.

### Day 28 reliability and approval evidence

The recorded Day 28 verification included:

| Test | Result recorded |
|---|---|
| Real investigation and persistence | PASS |
| Duplicate request replay | PASS |
| Investigation history and current incident projection | PASS |
| Approval request creation | PASS |
| Approval path | PASS |
| Rejection path | PASS |
| Approval persistence | PASS |
| No action execution by the agent | PASS |
| Unknown-error classification | PASS |
| Safe abstention for unknown error | PASS |
| Final persistence verification | PASS |

The persistence tests were sequential. They do not establish concurrent-write safety or distributed exactly-once execution.

### Historical failure scenario

One recorded investigation used a failed `users` dataset task with a missing `user_id` column. The workflow classified the incident as schema drift and reported partial evidence completeness.

This is a useful example because it ties the agent to an actual pipeline incident rather than a completely fabricated question.

---

<h2 align="center">🧭 The 30-Day Build Journey</h2>

The project was built in stages. The early days focused on the data platform. The later days added AI capabilities and then hardened the Operations Intelligence workflow.

The following is the project progression, not a list of unrelated tutorials.

### Phase 1 — Data platform foundation (Days 1–13)

| Day | What I worked on | Main result |
|---:|---|---|
| 1 | Defined the project problem, target user and responsibility of the Operations Intelligence Agent. | Project brief, scope boundaries and initial architecture. |
| 2 | Separated the data plane from the operations/control plane and defined the main component boundaries. | Architecture and incident-investigation sequence. |
| 3 | Profiled all seven CSV datasets, wrote source contracts and prepared the S3 raw landing layout. | Source inventory, row-count profile, data dictionary and source manifest. |
| 4 | Created the Lakeflow Declarative Pipeline project and configured the Unity Catalog target and source access. | A working pipeline foundation that can read from S3 and publish to the intended target. |
| 5 | Implemented the first Bronze ingestion pattern using the appropriate batch or Auto Loader mode. | One source-to-Bronze path with traceability metadata. |
| 6 | Extended the verified Bronze pattern to the remaining six datasets. | Seven Bronze datasets with their own source schemas. |
| 7 | Hardened Bronze for malformed files, unexpected columns, rescued data and source-version visibility. | Detectable and traceable source anomalies. |
| 8 | Implemented Silver canonical transformations for all seven datasets. | Typed, standardized Silver datasets driven by YAML contracts. |
| 9 | Added deterministic deduplication, business rules and relationship checks based on actual source keys. | Explicit record-selection rules and validated source relationships. |
| 10 | Added data quality expectations, valid-record handling and quarantine outputs. | DQ evidence and rejection reasons before Gold processing. |
| 11 | Tested incremental processing, idempotency, replay and recovery behavior. | Repeatable processing without duplicate business effects from the same delivery. |
| 12 | Built the applicable Gold dimensions, facts and analytical marts. | Curated analytical tables with documented grain and reconciled measures. |
| 13 | Connected the pipeline to Lakeflow Jobs and verified the end-to-end S3-to-Gold workflow and failure behavior. | An orchestrated and observable Databricks data pipeline. |

### Phase 2 — LLM and structured AI outputs (Days 14–17)

| Day | What I worked on | Main result |
|---:|---|---|
| 14 | Learned the LLM failure modes relevant to data operations and designed compact incident context. | A structured incident evidence payload rather than a raw-data prompt. |
| 15 | Built prompt/context rules and a versioned incident response contract. | Structured responses with facts, hypotheses and missing evidence separated. |
| 16 | Implemented AI-based review enrichment with a controlled output contract. | Review classification persisted separately from the original review data. |
| 17 | Configured natural-language analytics over Gold using Genie. | Business analytics kept separate from incident investigation. |

### Phase 3 — Operational knowledge and retrieval (Days 18–23)

| Day | What I worked on | Main result |
|---:|---|---|
| 18 | Designed the Operations Agent knowledge base and its document lifecycle. | Project-owned operational documents and manifest. |
| 19 | Parsed Markdown and supported PDF/DOCX sources while retaining provenance. | Parsed document records with source identity and processing metadata. |
| 20 | Chunked documents by heading/section and added version and content-hash metadata. | Search-ready operational chunks. |
| 21 | Designed embeddings and the Databricks AI Search index. | One runbook index as the retrieval source for incident procedures. |
| 22 | Built and queried the AI Search index. | A searchable operational knowledge base. |
| 23 | Evaluated retrieval and grounded RAG responses. | Citation checks, retrieval tests and safe abstention behavior. |

### Phase 4 — Agent, reliability and observability (Days 24–30)

| Day | What I worked on | Main result |
|---:|---|---|
| 24 | Selected the model execution path and designed the Operations Intelligence Agent. | One custom Python investigation workflow. |
| 25 | Implemented governed, read-only operational tools. | Eight narrow tools for job, incident, DQ, schema, history, runbook and impact evidence. |
| 26 | Connected tool selection, evidence collection, runbook retrieval and structured reporting. | A complete incident investigation workflow. |
| 27 | Added stable identities, request idempotency, bounded failure handling and durable persistence. | Repeatable investigation requests and durable investigation history. |
| 28 | Completed approval/rejection handling and unknown-error safety checks. | Human approval records, no autonomous action, and safe abstention on unknown errors. |
| 29 | Integrated MLflow tracing into the real agent workflow and tested it against a historical failed run. | A traceable real-agent investigation. |
| 30 | Connected the failure-trigger path to evidence capture, agent investigation, report persistence and final recovery demonstration. | The integrated Operations Intelligence workflow. |

### What changed across the 30 days

The project started as a data pipeline. It became a platform with two connected capabilities:

```text
First:
Build the data correctly.

Then:
Make the pipeline observable.

Then:
Capture evidence when something fails.

Then:
Use the evidence and the correct runbook to explain the failure.

Finally:
Keep recovery under human control.
```

---

<h2 align="center">🧰 Repository Structure</h2>

The repository keeps permanent implementation code in the module that owns that responsibility. The notebooks used for temporary diagnostics are not a substitute for maintainable project modules.

```text
zomato-databricks-ai-data-engineering-project/
│
├── ai_search/
│   ├── __init__.py
│   ├── config.py
│   ├── index.py
│   ├── project_production_setup.py
│   ├── retrieval_evaluation.py
│   ├── search.py
│   ├── source.py
│   └── testing_ai_search.py
│
├── knowledge_ingestion/
│   ├── 02_parse_markdown/
│   │   └── parse_markdown.py
│   ├── 03_parse_pdf_docx/
│   │   └── parse_pdf.py
│   └── 06_chunk_documents/
│       └── chunk_documents.py
│
├── observability/
│   ├── __init__.py
│   ├── agent_tracing.py
│   ├── config.py
│   ├── test.py
│   └── tracing.py
│
├── operations/
│   ├── __init__.py
│   ├── approvals.py
│   ├── agent_tools.py
│   ├── data_quality.py
│   ├── delta_history.py
│   ├── evidence_snapshots.py
│   ├── failure_handler.py
│   ├── incident_context.py
│   ├── investigate_pipeline_failure.py
│   ├── investigation.py
│   ├── investigation_dispatch.py
│   ├── investigation_job_entrypoint.py
│   ├── investigation_workflow.py
│   ├── jobs.py
│   ├── lineage.py
│   ├── persistence.py
│   ├── pipeline_events.py
│   ├── runbooks.py
│   └── schema_inspection.py
│
├── operations_console/
│   ├── __init__.py
│   ├── app.py
│   ├── app.yaml
│   ├── investigation_jobs.py
│   └── requirements.txt
│
├── rag/
│   ├── rag_smoke_test.py
│   └── response_generator.py
│
├── reviews_ai_enrichment/
│   └── zomato_reviews_enrich.py
│
├── schemas/
│   └── silver_canonical_schema/
│       ├── food.yml
│       ├── menu.yml
│       ├── order_items.yml
│       ├── orders.yml
│       ├── restaurant.yml
│       ├── reviews.yml
│       └── users.yml
│
├── transformations/
│   ├── bronze_ingestion/
│   │   ├── bronze_food.py
│   │   ├── bronze_menu.py
│   │   ├── bronze_order_items.py
│   │   ├── bronze_orders.py
│   │   ├── bronze_restaurant.py
│   │   ├── bronze_reviews.py
│   │   └── bronze_users.py
│   ├── gold_business_ready/
│   │   ├── dim_customer.py
│   │   ├── dim_date.py
│   │   ├── dim_food.py
│   │   ├── dim_restaurants.py
│   │   ├── fact_order_items.py
│   │   ├── fact_orders.py
│   │   ├── mart_daily_city_revenue.py
│   │   ├── mart_delivery_sla.py
│   │   └── mart_restaurant_performance.py
│   └── silver_cleaned/
│       ├── silver_food.py
│       ├── silver_menu.py
│       ├── silver_order_items.py
│       ├── silver_orders.py
│       ├── silver_restaurant.py
│       ├── silver_reviews.py
│       └── silver_users.py
│
├── .gitignore
├── LICENSE
└── README.md
```

### Why the code is organized this way

| Directory | Responsibility |
|---|---|
| `transformations/` | Bronze, Silver and Gold dataset definitions. |
| `schemas/` | Canonical Silver schema contracts. |
| `reviews_ai_enrichment/` | Review classification and enrichment logic. |
| `knowledge_ingestion/` | Parsing and chunking of project-owned operational documents. |
| `ai_search/` | Runbook index creation, retrieval configuration and retrieval evaluation. |
| `rag/` | Evidence-grounded response generation and citation validation. |
| `operations/` | Incident context, evidence tools, workflow, persistence, approvals and failure handling. |
| `observability/` | MLflow tracing configuration and instrumentation. |
| `operations_console/` | Operator-facing console source and its configuration. |

The repository structure is intentionally kept small enough to understand. It does not create another backend or duplicate agent implementation for the same incident workflow.

---

<h2 align="center">🧑‍💻 Technology Stack</h2>

| Technology | Where it is used |
|---|---|
| Python | Project modules, ingestion helpers, operations workflow and agent code. |
| PySpark / Apache Spark | Distributed data transformations. |
| Databricks | Main data engineering and AI execution platform. |
| AWS S3 | Raw Zomato files and project knowledge source documents. |
| Lakeflow Declarative Pipelines | Bronze, Silver and Gold data pipeline definitions. |
| Lakeflow Jobs | Pipeline orchestration and failure-handler execution. |
| Delta Lake | Data, quality, knowledge and operations tables. |
| Unity Catalog | Catalog, schema, table and governed-function access. |
| Auto Loader | Incremental file discovery where the delivery pattern requires it. |
| Databricks AI Functions | Review classification where supported by the workspace. |
| Databricks Genie | Natural-language analytics over Gold data. |
| Databricks AI Search | Operational runbook retrieval. |
| RAG | Grounding investigation responses in retrieved project procedures. |
| MLflow | Agent tracing and execution observability. |
| Pydantic / structured validation | Validating incident response contracts where used by the workflow. |
| Git / GitHub | Version control and source history. |

The project deliberately does not add a second vector database, multiple agents, Kafka, Airflow, dbt or an external orchestration stack.

---

<h2 align="center">⚙️ Running the Project</h2>

This repository contains source code for a Databricks workspace. It is not a standalone local application that can reproduce the entire system with one `python main.py` command.

The pipeline depends on Databricks resources, S3 access, Unity Catalog objects, model/search services and workspace-specific configuration.

### Prerequisites

A target workspace needs the relevant capabilities and permissions before the project can run:

- A Databricks workspace with the required Lakeflow pipeline and Jobs capabilities.
- Access to the Zomato raw source prefix in Amazon S3.
- A configured Unity Catalog catalog and schemas.
- Delta table permissions for the pipeline and operational modules.
- A supported model endpoint or AI Function for the AI workload being executed.
- A supported AI Search endpoint and index for runbook retrieval.
- An MLflow experiment for agent tracing.
- Required Python packages for the selected execution path.

Feature availability depends on the workspace edition, region, quota and permissions. Do not assume a capability exists just because it appears in a general Databricks feature guide.

### Configuration before execution

Workspace-specific settings must be supplied through the approved configuration and secret mechanisms for the target environment.

Do not commit:

- Databricks tokens or `.databrickscfg` files.
- AWS access keys or secret keys.
- Model-serving credentials.
- Workspace-specific secrets.
- Private customer data or unredacted sensitive records.
- Local checkpoints or runtime state.

The `.gitignore` excludes common local credential and runtime paths, but `.gitignore` is not a substitute for reviewing files before committing.

### Recommended execution order

```text
1. Configure S3 access and Unity Catalog.
2. Validate source paths and canonical schemas.
3. Publish / run Bronze ingestion.
4. Run Silver transformations.
5. Inspect DQ and quarantine results.
6. Run Gold transformations and reconcile measures.
7. Validate the review enrichment workload.
8. Validate Genie against approved Gold tables.
9. Ingest and parse operational documents.
10. Build / synchronize the runbook chunks index.
11. Test retrieval and grounded RAG responses.
12. Run a read-only incident investigation.
13. Verify persistence and approval records.
14. Validate the failure-handler path in the target workspace.
```

The operational agent should not be tested against a live destructive recovery action. Use a controlled test failure and keep recovery under human control.

### Important deployment distinction

Pushing this repository to GitHub publishes source code and preserves version history. It does not automatically create or update Databricks pipelines, Lakeflow Jobs, Unity Catalog objects, AI Search indexes, model endpoints, experiments or Apps.

Those workspace resources need to be configured and deployed through the appropriate Databricks workflow for the target environment.

---

<h2 align="center">🧑‍💻 How a New Developer Uses This Repository</h2>

A new developer should first understand the data flow and the operations flow before editing code.

The recommended reading order is:

```text
README.md
   ↓
transformations/ and schemas/
   ↓
operations/pipeline_events.py
   ↓
operations/evidence_snapshots.py
   ↓
operations/agent_tools.py
   ↓
operations/investigation_workflow.py
   ↓
operations/persistence.py and operations/approvals.py
   ↓
rag/ and ai_search/
   ↓
observability/
```

The exact code path to inspect depends on the task. For a Bronze schema change, start with the relevant transformation and schema file. For an incident-correlation defect, start with the pipeline event and investigation modules. For a retrieval problem, inspect the document ingestion, chunking, AI Search and RAG modules.

### Before making a change

1. Read the module responsible for the behavior.
2. Check its callers and the contract it exposes.
3. Identify the tables and workspace resources it reads or writes.
4. Understand the current failure behavior.
5. Make the smallest change that solves the actual problem.
6. Run the relevant validation before changing another module.

Permanent code belongs in the existing project modules. Temporary diagnostics belong in clearly labelled scratch notebooks or temporary scripts and should not become a second implementation path.

---

<h2 align="center">🛠️ Changing a Databricks Transformation</h2>

A transformation change should be made in the appropriate layer.

For a Bronze source change:

```text
Inspect source contract
        ↓
Update Bronze ingestion definition
        ↓
Validate source metadata and rescued data
        ↓
Run the relevant pipeline
        ↓
Compare source and Bronze output
```

For a Silver schema or type change:

```text
Inspect source change
        ↓
Review canonical YAML schema
        ↓
Update Silver transformation
        ↓
Test casting / required fields / deduplication
        ↓
Inspect quarantine and DQ results
        ↓
Validate downstream Gold dependencies
```

For a Gold model change:

```text
Define the target grain
        ↓
Inspect Silver source relationships
        ↓
Update fact / dimension / mart definition
        ↓
Run the relevant pipeline
        ↓
Reconcile output with independent SQL
```

A change should not be considered complete because the pipeline task turned green. The output also needs to satisfy the data contract and expected business grain.

---

<h2 align="center">🚫 What a New Developer Should Not Do</h2>

There are a few boundaries that should remain clear when working on this project.

Do not:

- Put credentials, tokens or private workspace configuration in source files.
- Change the source schema without updating the canonical contract and downstream validation.
- Hide failed casts by silently converting every invalid value to null.
- Drop invalid records without preserving the rejection reason where quarantine is required.
- Treat Auto Loader checkpointing as business-key deduplication.
- Change a fact table's grain without checking how it affects every downstream measure.
- Build a second custom Text-to-SQL agent when Genie is the selected Gold analytics interface.
- Add a second vector database or a duplicate AI Search index without a real requirement.
- Put customer reviews into the operational runbook knowledge base.
- Treat retrieved runbook text as live evidence that a particular pipeline event occurred.
- Claim a draft project runbook is an official Zomato procedure.
- Give the Operations Intelligence Agent write access to production data or pipeline configuration.
- Let the agent restart jobs, reset checkpoints, repair tables or change schemas on its own.
- Treat `APPROVED` in an approval table as proof that the agent may execute the action.
- Claim concurrent exactly-once semantics from sequential idempotency tests.
- Claim measured model quality, latency or cost improvements without recorded measurements.
- Claim that GitHub push deploys Databricks resources automatically.

The goal is to keep the project understandable and safe, not to add more services for the sake of making the architecture look larger.

---

<h2 align="center">🔄 Normal Development Workflow</h2>

The repository uses GitHub for source control. Databricks remains the execution environment for the data and AI workloads.

The normal code workflow is:

```text
Developer
    ↓
Inspect the relevant module
    ↓
Make a focused change
    ↓
Run local / notebook checks
    ↓
Review diff and staged files
    ↓
Git commit
    ↓
Git push
    ↓
Review repository state
    ↓
Deploy / sync to Databricks through the selected workflow
    ↓
Run workspace validation
    ↓
Inspect data and operational results
```

For a data transformation change, validate the affected dataset and downstream outputs.

For an operations workflow change, validate identifiers, tool outcomes, evidence completeness, persistence behavior and the read-only boundary.

For a knowledge ingestion change, validate parsing, provenance, content hashes, chunk metadata and retrieval behavior.

For an observability change, check that the trace still captures the actual agent execution and that identifiers are correlated correctly.

### Keep code review focused

A single pull request or commit should ideally solve one coherent problem. This makes it easier to understand what changed and how to roll it back if the new behavior is wrong.

---

<h2 align="center">🔁 How Rollback Works</h2>

The source repository gives every code change a Git commit. If a new change introduces a defect, the code can be compared with a known earlier commit and the required files can be restored or reverted.

The basic process is:

```text
Known working commit
        ↓
New code change
        ↓
Validation detects a defect
        ↓
Inspect the Git diff
        ↓
Revert the faulty commit or restore the known file version
        ↓
Sync the reviewed version to Databricks
        ↓
Run validation again
```

For data, rollback is a separate concern from code rollback. A Git revert does not restore a Delta table or undo a pipeline run. Data recovery must use the appropriate table history, source snapshot, backfill or approved recovery procedure.

For operational records, do not delete the incident evidence just because the code that generated the report needs to be corrected. Preserve the original evidence and create a corrected investigation record through the controlled workflow.

---

<h2 align="center">✅ What GitHub and Databricks Together Give This Project</h2>

GitHub and Databricks have different responsibilities.

```text
GitHub
→ Stores the source code, documentation, license and change history.
```

```text
Databricks
→ Runs the pipelines, manages Delta tables and executes the AI / operations workflows.
```

Together:

```text
                 Git Repository
                      │
                      ▼
              Reviewed source changes
                      │
                      ▼
             Databricks workspace sync
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
    Data platform             Operations platform
          │                        │
          ▼                        ▼
 Bronze / Silver / Gold      Evidence / Agent / RAG
          │                        │
          ▼                        ▼
 Analytics / Genie           Incident report / MLflow
```

This gives the project a clear boundary between source management and runtime execution.

The repository is private and the source code is version-controlled on the `main` branch. The initial project source and MIT license history were preserved when the repository was connected to GitHub.

### What this project demonstrates

- A seven-dataset Databricks Lakehouse.
- Bronze, Silver and Gold data layers.
- Canonical schemas and data quality handling.
- Quarantine and traceability for invalid records.
- Incremental processing and idempotent rerun patterns.
- Fact, dimension and analytical mart transformations.
- AI review enrichment stored separately from source reviews.
- Natural-language analytics over Gold data through Genie.
- An operational knowledge base with document provenance and chunk metadata.
- Databricks AI Search and grounded RAG for runbook retrieval.
- One Operations Intelligence Agent using read-only evidence tools.
- Durable incident report persistence and request-level idempotency.
- Human approval records and human-controlled recovery.
- MLflow tracing integrated into the real investigation workflow.

The strongest part of the project is not that it includes an LLM. It is that the LLM sits behind a data platform and an evidence workflow that define what it can inspect, what it can recommend and what it cannot change.

---

<h2 align="center">⚠️ Platform Limits and Engineering Boundaries</h2>

This project was built in a Databricks Free Edition environment. Free Edition is useful for learning and portfolio work, but it is not an enterprise production environment with a service-level agreement.

The available compute, concurrency, model endpoints, AI Search capacity, Apps behavior and feature availability can be limited by the edition, workspace and region.

For that reason:

- Workspace-specific resource names are configuration, not portable defaults.
- A feature should be smoke-tested before another component depends on it.
- If an integration is simulated, it must be labelled as simulated.
- A successful notebook run is not proof of a deployed service.
- A successful MLflow trace is not a full agent accuracy benchmark.
- Sequential idempotency tests are not proof of concurrent exactly-once processing.
- Approval records do not independently authenticate or authorize the reviewer.
- A portfolio prototype is not automatically production-ready for a real customer workload.

These are engineering boundaries, not reasons to hide the implementation. A good project explains both what works and where the evidence ends.

---

<h2 align="center">📄 License</h2>

This project is licensed under the MIT License. See [LICENSE](LICENSE) for the full license text.

---

<div align="center">

<h3 align="center">🌟 About Me</h3>

Hi there! I'm **Abdul Khadir**, a Computer Science Engineering graduate building practical Data Engineering projects with Databricks, Spark, AWS and Python.

---

<h3 align="center">📬 Connect With Me</h3>

If you'd like to discuss Data Engineering, share feedback or connect, feel free to reach out.

<p align="center">
  <a href="https://www.linkedin.com/in/abdul-khadir-44876735a" target="_blank">
    <img src="https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn"/>
  </a>
  &nbsp;&nbsp;
  <a href="https://www.instagram.com/abdulkm_63" target="_blank">
    <img src="https://img.shields.io/badge/Instagram-E4405F?style=for-the-badge&logo=instagram&logoColor=white" alt="Instagram"/>
  </a>
</p>

</div>
