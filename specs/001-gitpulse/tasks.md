# Tasks: GitPulse

**Input**: Design documents from `specs/001-gitpulse/`

**Prerequisites**: plan.md (required), spec.md (required for user stories)

## Phase 1: Terraform Foundation

- [ ] T001 Create the Terraform base configuration for the AWS account, provider settings, and resource naming conventions in `terraform/`
- [ ] T002 Define the base network and shared deployment configuration needed for the data pipeline in `terraform/main.tf` and related files
- [ ] T003 Configure Terraform variables, outputs, and environment-specific defaults for repeatable deployment in `terraform/variables.tf` and `terraform/outputs.tf`

## Phase 2: S3 Data Lake and Security

- [ ] T004 Create the raw and processed S3 buckets with lifecycle and access controls in `terraform/s3.tf`
- [ ] T005 Define least-privilege IAM roles and policies for Lambda, Glue, and storage access in `terraform/iam.tf`
- [ ] T006 Configure KMS encryption and the key policies required for secure data-at-rest protection in `terraform/kms.tf`

## Phase 3: GitHub Ingestion Lambda

- [ ] T007 Implement the GitHub ingestion Lambda in `src/extractor/github_ingest_lambda.py` to call the GitHub REST API and normalize repository payloads
- [ ] T008 Add validation and error handling for malformed or incomplete GitHub responses in `src/extractor/validators.py`
- [ ] T009 Write raw repository JSON objects to the S3 raw bucket and emit traceable metadata for downstream processing in `src/extractor/s3_helpers.py`

## Phase 4: EventBridge, SQS, and DLQ

- [ ] T010 Configure EventBridge scheduling and event routing from the raw S3 landing point to the SQS work queue in `terraform/main.tf` and related config
- [ ] T011 Implement SQS queue and DLQ configuration with retry limits and message retention policy in `terraform/main.tf` or queue-specific Terraform definitions

## Phase 5: Glue ETL and Processed Data

- [ ] T012 Implement the Glue ETL job in `src/glue/transform_repo_data.py` to read raw JSON, validate schema, write Parquet to the processed bucket, and update the data quality state
- [ ] T013 Register the processed Parquet dataset in the Glue Data Catalog and validate Athena-compatible table structure for analytics queries
- [ ] T013a Write unit tests for the Glue transformation/normalization logic and a smoke test that validates an Athena query against a sample processed table in `src/tests/integration/`

## Phase 6: Analytics, Observability, and Validation

- [ ] T014 Create Athena query validation examples for stars, forks, languages, update recency, and language counts using the Glue Catalog table
- [ ] T015 Configure CloudWatch logging and basic metrics for Lambda, Glue, SQS queue health, and DLQ accumulation
- [ ] T016 Perform reliability testing by (a) temporarily revoking Glue's S3 read permission on a raw object to simulate a transient failure and confirm SQS retry behavior, and (b) uploading a malformed/incomplete raw JSON file to confirm it fails validation and lands in the DLQ after retries are exhausted
- [ ] T017 Run end-to-end validation to confirm the full pipeline from GitHub API through Athena works within the project’s OJT scope

## Dependencies and Execution Order

- Terraform foundation must be complete before resource-based components are added.
- S3 buckets and IAM/KMS must exist before Lambda and Glue are configured.
- Lambda ingestion must run before EventBridge and SQS-driven processing can be validated.
- Glue ETL and the processed S3 layer must be complete before catalog registration and Athena queries.
- Observability and reliability tests should run after the end-to-end path is operational.

## Traceability

Each task maps directly to the GitPulse specification and implementation plan, covering the required architecture components: Terraform, S3, IAM/KMS, Lambda ingestion, EventBridge, SQS/DLQ, Glue ETL, Parquet output, Glue Catalog, Athena, CloudWatch, reliability testing, and end-to-end validation.
