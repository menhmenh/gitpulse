# Implementation Plan: GitPulse

**Branch**: `001-gitpulse` | **Date**: 2026-08-26 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-gitpulse/spec.md`

## Summary

GitPulse is a compact AWS-native data pipeline that ingests public GitHub repository metadata, preserves it in a raw S3 layer, moves it through a reliable EventBridge-to-SQS workflow, transforms it with AWS Glue into Parquet, and makes it queryable in Athena. The design deliberately focuses on minimal production-inspired reliability, observability, and security while staying within the scope of an OJT demonstration.

## Technical Context

**Language/Version**: Python 3.12 for Lambda processing and Glue-compatible Python jobs

**Primary Dependencies**: boto3, requests or urllib for GitHub API calls, Terraform, AWS Glue, Amazon Athena, Amazon S3, EventBridge, SQS, CloudWatch

**Storage**: Amazon S3 raw bucket, Amazon S3 processed bucket, AWS Glue Data Catalog, Athena table metadata

**Testing**: Python unit tests for validation logic, targeted integration checks for S3/EventBridge/SQS flow, and smoke validation for Athena queries

**Target Platform**: AWS managed services running via Terraform on a single account and region

**Project Type**: Data pipeline / serverless analytics application

**Performance Goals**: Process a moderate, scheduled list of public repositories within a few minutes per run; keep retry behavior bounded and observable

**Constraints**: Small scope, low operational complexity, public-source metadata only, no enterprise-grade multi-region or streaming architecture

**Scale/Scope**: OJT demo with fixed repository targets and a minimal data lake, not a broad multi-tenant or multi-account deployment

## Constitution Check

*GATE: Must pass before implementation. The proposed design complies with the GitPulse constitution by preserving least-privilege access, explicit reliability and DLQ handling, CloudWatch visibility, Terraform reproducibility, and a deliberately small scope.*

## Project Structure

### Documentation (this feature)

```text
specs/001-gitpulse/
├── spec.md
├── plan.md
├── tasks.md
├── research.md          # Optional future design note if added later
├── data-model.md        # Optional future schema note if added later
├── quickstart.md        # Optional future validation guide if added later
└── contracts/
```

### Source Code (repository root)

```text
src/
├── extractor/
│   ├── github_ingest_lambda.py
│   ├── validators.py
│   ├── s3_helpers.py
│   └── logger.py
├── glue/
│   └── transform_repo_data.py
└── tests/
    ├── unit/
    └── integration/

data/
├── raw/
└── processed/

docs/

tests/

terraform/
├── provider.tf
├── main.tf
├── s3.tf
├── iam.tf
├── kms.tf
├── variables.tf
├── outputs.tf
├── versions.tf
└── terraform.tfstate
```

**Structure Decision**: The implementation will keep Terraform infrastructure in the existing `terraform/` directory, Lambda and ETL code in `src/`, and validation checks in `tests/` so the repository stays aligned with the current project layout and the AWS data pipeline architecture.

## Architecture Mapping

GitHub REST API
↓
AWS Lambda
↓
S3 Raw
↓
EventBridge
↓
SQS + DLQ
↓
AWS Glue ETL
↓
S3 Processed / Parquet
↓
Glue Data Catalog
↓
Athena

## Component Responsibilities

- GitHub REST API: Source of repository metadata for public repositories.
- AWS Lambda: Calls the GitHub API, validates payloads, writes raw JSON to S3, and emits the event metadata used by the downstream queue.
- S3 Raw: Durable landing area for original JSON snapshots before transformation.
- EventBridge: Receives S3 object notifications or a scheduled event and routes them to the queue for decoupled reliability handling.
- SQS + DLQ: Stores processing events as durable work items, supports retry with backoff, and preserves failed messages for investigation.
- AWS Glue ETL: Reads the raw JSON from S3, normalizes fields, handles validation errors, and writes Parquet to the processed bucket.
- S3 Processed / Parquet: Analytics-ready, queryable data store separated from raw ingestion data.
- Glue Data Catalog: Publishes the processed table definition so Athena can query it.
- Athena: Executes SQL queries for repository ranking, language distribution, and freshness analysis.
- Terraform: Creates and manages all AWS resources in a repeatable, version-controlled way.

## Reliability Flow Details

The reliability flow is central to the design and must not be treated as a generic step.

- What event triggers processing? A raw S3 write event or a scheduled EventBridge rule emits an event after a new object lands in the raw bucket. This event contains the repository identifier, source object key, and ingestion timestamp.
- What message is sent through SQS? The queue receives a structured message containing the repository metadata reference, raw object key, dataset name, and event metadata needed to rehydrate the work item.
- What component consumes the SQS message? AWS Glue ETL is the consumer of the SQS message, reading the referenced raw object and transforming it into the processed output.
- What happens when processing succeeds? Glue writes Parquet output to the processed S3 bucket, updates the Glue Catalog table metadata, and records success metrics to CloudWatch.
- What happens when processing temporarily fails? The message is retried according to the SQS retry policy with a bounded attempt count and backoff schedule.
- When is a message retried? Retried when the Glue job fails due to transient issues such as service throttling, temporary network issues, or partial file inconsistencies.
- When is a message moved to the DLQ? When the retry limit is exhausted or when the message is permanently invalid and cannot be processed safely.
- How can the failed event be investigated? Operators can inspect the DLQ message content and CloudWatch logs to identify the raw file, repository ID, failure stage, and exact reason for the failure.

## Security Plan

- IAM roles will be scoped to the minimum resources each service needs; Lambda needs only GitHub API read access, S3 write access to the raw bucket, and S3 object read access to the raw bucket for processing, while Glue needs access to raw and processed buckets and the Glue Catalog.
- KMS keys will protect S3 data and any sensitive operational artifacts. The key policy must allow only required services and principals.
- GitHub API credentials, if added, will be passed through environment variables or AWS Secrets Manager/SSM Parameter Store rather than being committed to Terraform or source code.
- Separate roles and permissions will prevent a single component from having broad read/write access to the entire data lake.
- Buckets will enforce restricted access patterns and avoid public access settings.

## Observability Plan

- Lambda logs: repository metadata retrieval success/failure, payload validation Events, S3 raw write result, and API error codes.
- Glue job logs: input read status, row validation, transformation diagnostics, output write status, and failure details.
- SQS failure visibility: queue depth, processing failures, retries, and DLQ counts from CloudWatch metrics.
- DLQ monitoring: alert or metric on messages accumulating in the DLQ; each message retains trace data for investigation.
- Useful pipeline metrics: Lambda duration, errors, invocation count, S3 object writes, Glue job success rate, rows processed, and DLQ age/depth.
- Failure investigation workflow: examine the failed message or DLQ payload, inspect CloudWatch logs for the Lambda or Glue run, and trace the raw S3 object key back to the originating repository data set.

## Data Validation and Quality Rules

- Validate that required fields are present and correctly typed before writing a processed record.
- Reject empty or partial GitHub responses instead of synthesizing missing values.
- Persist raw JSON for every successful ingest so the original payload remains available for reprocessing.
- Ensure processed Parquet rows are consistent and conform to a normalized schema before catalog registration.
- Record malformed responses with metadata to support root-cause analysis without silently discarding information.

## Athena Evaluation and Example Queries

The project will validate the output with simple Athena queries that demonstrate the expected analytics layer, including:

- repositories with the highest number of stars
- repositories with the highest number of forks
- most common programming languages
- recently updated repositories
- repository counts by programming language

These queries are evaluation examples for the OJT project and are intentionally lightweight.

## Risk and Edge Cases

- GitHub API rate limits can cause ingestion failures; the Lambda should record errors and let the queue and retry flow handle the backlog.
- Invalid or incomplete API payloads must be handled without creating a misleading processed record.
- A DLQ event should be traceable to its raw object and original request context for investigation.
- When the pipeline is down, the queue remains a durable decoupling point rather than losing messages.

## Complexity Tracking

No constitution violations require special justification for this project because the plan stays within the defined AWS data pipeline scope and avoids unnecessary architecture complexity.
