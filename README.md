# Zomato Lakehouse & Operations Intelligence Agent

A Databricks-based data engineering portfolio project combining a multi-layer Lakehouse pipeline with an AI-assisted pipeline incident investigation system.

The project demonstrates data ingestion, transformation, data quality management, operational observability, retrieval-augmented generation (RAG), and human-controlled incident response.

## Architecture

### 1. Lakehouse Data Pipeline

The pipeline processes seven Zomato datasets:

- Food
- Menu
- Order Items
- Orders
- Restaurant
- Reviews
- Users

Data flows through three layers:

**Bronze**
- Ingestion from AWS S3 using Databricks Auto Loader.
- Preserves ingestion metadata.
- Captures rescued data for malformed or unexpected records.

**Silver**
- Applies canonical schemas.
- Performs data validation and cleaning.
- Handles invalid records and deduplication.

**Gold**
- Builds analytical fact and dimension tables.
- Produces business-ready datasets and analytical marts.

### 2. Operations Intelligence Agent

The project includes an AI-assisted system for investigating pipeline incidents.

Core capabilities include:

- Collecting pipeline and job execution evidence.
- Inspecting schema changes and data quality results.
- Retrieving Delta table history and downstream impact.
- Searching operational runbooks using Databricks AI Search.
- Generating structured, evidence-grounded investigation reports.
- Persisting investigation information.
- Supporting human approval before recovery or data-changing actions.

The investigation system is designed around read-only operational tools. It does not autonomously execute destructive recovery actions.

### 3. Observability

MLflow tracing is integrated into the agent workflow to support investigation of model and agent execution.

## Technology Stack

- Databricks
- PySpark
- Spark Structured Data Processing
- Lakeflow Declarative Pipelines
- Delta Lake
- AWS S3
- Databricks Auto Loader
- Databricks AI Search
- RAG
- MLflow
- Python
- SQL
- YAML

## Repository Structure

| Directory | Purpose |
|---|---|
| `transformations/` | Bronze, Silver and Gold transformations |
| `schemas/` | Canonical Silver schemas |
| `operations/` | Incident investigation, evidence collection, persistence and approval logic |
| `operations_console/` | Operations investigation interface |
| `rag/` | Grounded response generation |
| `ai_search/` | Search index and retrieval logic |
| `knowledge_ingestion/` | Runbook and document ingestion |
| `observability/` | Agent tracing and observability |
| `reviews_ai_enrichment/` | AI-based review enrichment |
| `setup/` | Project setup resources |
| `utilities/` | Utility resources |

## Environment and Setup

This project is designed to run in a configured Databricks workspace.

Execution requires the relevant workspace resources, permissions, data access, and service configuration.

Depending on the components being executed, these include:

- AWS S3 access.
- Databricks compute and pipeline resources.
- Unity Catalog permissions and resources.
- Databricks AI Search configuration.
- Model serving endpoints.
- MLflow experiment configuration.

Workspace-specific identifiers, resource names, and credentials must be configured in the target environment. Credentials must never be committed to this repository.

The source repository alone does not deploy or provision these resources.

## Operational Safety

The Operations Intelligence Agent is intended to investigate incidents and recommend safe next steps.

Evidence collection and investigation are separated from recovery execution. Human review and approval are required before data-changing recovery actions.

Runbook retrieval and recommendations must remain grounded in available operational evidence. Missing evidence should be explicitly reported rather than replaced with assumptions.

## Project Limitations

This is a portfolio engineering project, not a certified production service.

Some integrations and execution capabilities depend on the Databricks workspace edition and available resources. Databricks Free Edition limitations may affect deployment, scheduling, compute, and service availability.

Lakeflow Jobs failure-handler integration and end-to-end recovery behavior must be validated in the target workspace before being represented as fully deployed.

## Important

- Do not commit credentials, tokens, private configuration files, checkpoints, or private customer data.
- Workspace resources must be provisioned and configured separately.
- Successful execution in one workspace does not guarantee deployment in another.
