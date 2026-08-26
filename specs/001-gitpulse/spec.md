# Feature Specification: GitPulse

**Feature Branch**: `001-gitpulse`

**Created**: 2026-08-26

**Status**: Draft

**Input**: User description: "GitPulse — AWS Data Engineering Mock Project"

## User Scenarios & Testing

### User Story 1 - Automated GitHub repository ingestion (Priority: P1)

As a data engineer, I want the project to retrieve selected GitHub repository metadata on a scheduled basis so that a reliable raw dataset is produced for downstream processing.

**Why this priority**: The ingestion path is the foundation of the pipeline. Without a dependable raw layer, no subsequent transformation, cataloging, or analysis can be trusted.

**Independent Test**: A scheduled Lambda run can fetch repository metadata from the GitHub REST API and store validated JSON in the raw S3 location, demonstrating that ingestion works independently of the rest of the pipeline.

**Acceptance Scenarios**:

1. **Given** a configured AWS environment and repository target list, **When** the ingest trigger runs, **Then** the Lambda retrieves repository metadata and writes raw JSON objects to the raw S3 bucket.
2. **Given** an API response that is missing required fields, **When** the Lambda processes the result, **Then** it records a validation failure and avoids creating a misleading raw payload.

---

### User Story 2 - Reliable processing and dead-letter handling (Priority: P1)

As an operator, I want the raw data to flow through a durable queue and DLQ so that transient failures do not lose the event and failed work can be investigated later.

**Why this priority**: Reliability is a core project objective. The pipeline must tolerate temporary problems without silently discarding work or blocking the full data flow.

**Independent Test**: A simulated failure in the ETL step can be retried and then moved to the DLQ after the configured retry threshold, showing that the operational handling matches the design.

**Acceptance Scenarios**:

1. **Given** a valid raw S3 object and a healthy Glue processing job, **When** the EventBridge-to-SQS event is delivered, **Then** the job processes the file and writes analytics-ready Parquet rows.
2. **Given** a temporary ETL or downstream processing issue, **When** retries are exhausted, **Then** the original event is moved to the DLQ and remains available for investigation.

---

### User Story 3 - Queryable analytics in Athena (Priority: P2)

As a business user or analyst, I want the processed dataset to be exposed through the AWS Glue Catalog and queryable in Athena so that repository insights can be answered with standard SQL.

**Why this priority**: Queryability is the value-delivery goal for the project. It turns raw operational data into usable analytics data for simple repository metrics.

**Independent Test**: Athena can query the published Parquet dataset for repository counts by language, stars, forks, and recent updates without manual data preparation.

**Acceptance Scenarios**:

1. **Given** processed Parquet data and a Glue Catalog table, **When** an analyst runs an Athena query, **Then** the results reflect repository metadata in a structured, queryable format.
2. **Given** repository updates or ingest runs spanning multiple dates, **When** queries are run for updated repositories or star counts, **Then** the results include the most recent valid records.

---

### User Story 4 - Secure and observable AWS deployment (Priority: P2)

As a platform engineer, I want the pipeline to run with least-privilege permissions, encryption, and CloudWatch visibility so that the system is secure and operationally diagnosable.

**Why this priority**: Security and observability are explicitly required and are necessary to make the project production-inspired without excessive complexity.

**Independent Test**: CloudWatch logs and metrics show that the Lambda, Glue ETL job, and queue operations ran successfully or failed with enough context to identify the root cause.

**Acceptance Scenarios**:

1. **Given** a deployment with IAM roles and KMS keys, **When** the pipeline executes, **Then** each component has only the minimum access necessary to perform its role.
2. **Given** a failed processing run, **When** the operator checks CloudWatch logs and DLQ messages, **Then** they can identify the failed repository record and the processing stage that failed.

---

### Edge Cases

- What happens when the GitHub API rate limit is reached or a request times out? The system must record the failure, respect retry behavior, and avoid producing partial data.
- What happens when a repository payload is malformed, missing required fields, or contains null values? The system must fail validation and retain a traceable record rather than silently writing corrupted data.
- What happens when the Glue transformation encounters a bad raw file? The system must fail that message or file, retry within the configured threshold, and move the event to the DLQ when retry limits are exhausted.
- What happens when the queue is temporarily unavailable or the ETL job is delayed? The message remains durable in SQS until processing succeeds or the retry policy is exhausted.

## Requirements

### Functional Requirements

- **FR-001**: The system MUST ingest GitHub repository metadata from the public GitHub REST API for a defined set of repositories on a scheduled or trigger-based basis.
- **FR-002**: The system MUST write raw GitHub repository data to an S3 raw data lake without overwriting the original payload, preserving distinct object keys for each ingestion event.
- **FR-003**: The system MUST emit a processing event from the raw S3 landing point to an EventBridge-driven workflow that enqueues a durable message for downstream processing.
- **FR-004**: The system MUST process queued events through AWS Glue ETL to normalize raw JSON into analytical Parquet output.
- **FR-005**: The system MUST register the resulting processed dataset in the AWS Glue Data Catalog so it can be queried through Athena.
- **FR-006**: The system MUST perform basic validation of required GitHub fields such as repository ID, name, owner, URL, language, stars, forks, creation date, and last update date before promoting data to a trusted downstream state.
- **FR-007**: The system MUST support retry logic for transient failures and a dead-letter queue for permanent or repeatedly failed events.
- **FR-008**: The system MUST log execution metadata and publish metrics to Amazon CloudWatch for Lambda, Glue, SQS, and DLQ visibility.
- **FR-009**: The system MUST securely access AWS resources using least-privilege IAM permissions and KMS encryption where appropriate.
- **FR-010**: The system MUST preserve raw data separately from processed and analytical data so that downstream users can investigate or reprocess historical ingests.
- **FR-011**: The system MUST allow Athena queries that answer common repository questions such as top repositories by stars, top repositories by forks, language distribution, recent updates, and repository counts by language.
- **FR-012**: The system MUST be deployable with Terraform from a repeatable, version-controlled infrastructure definition.

### Data Requirements

- **DR-001**: The pipeline MUST capture repository metadata including repository ID, repository name, owner, description, URL, primary language, star count, fork count, open issue count, created date, and last updated date.
- **DR-002**: Raw data MUST be stored in raw JSON form and remain immutable once written to the raw bucket for traceability.
- **DR-003**: Processed data MUST be represented in Parquet format and structured for Athena queries.
- **DR-004**: Events and messages MUST include enough metadata to trace a repository record back to the originating raw S3 object, processing timestamp, and failure context.
- **DR-005**: Malformed, incomplete, or empty API responses MUST be rejected or quarantined rather than silently written as valid processed data.

### Reliability and Failure Handling Requirements

- **RR-001**: The system MUST tolerate transient API, queue, or ETL failures by retrying messages with bounded retry behavior.
- **RR-002**: The system MUST move a message to the DLQ when retry attempts are exhausted or when the payload is determined to be permanently unprocessable.
- **RR-003**: The system MUST ensure each failed event remains inspectable with metadata that identifies the repository, failing stage, and time of failure.
- **RR-004**: The system MUST avoid duplicate or partial processing by using deterministic object keys and consistent event identifiers where practical.

### Security Requirements

- **SR-001**: The system MUST use AWS IAM roles with least privilege for Lambda, Glue, S3, and EventBridge/SQS interactions.
- **SR-002**: Sensitive data MUST be encrypted at rest using KMS-managed keys, including S3 objects and any operational resources that store encrypted sensitive state.
- **SR-003**: GitHub API credentials, if required, MUST be supplied through secure configuration mechanisms instead of hardcoded values in source control.
- **SR-004**: Resource permissions MUST be separated so that ingestion, transformation, and analytics components do not share broader privileges than necessary.

### Observability Requirements

- **OR-001**: Lambda executions MUST emit structured logs showing start time, repository targets, API result status, S3 writes, and errors.
- **OR-002**: Glue job logs MUST capture source read, transformation steps, output writes, and job failure details.
- **OR-003**: SQS and DLQ metrics MUST provide visibility into queue depth, processing failures, and dead-letter accumulation.
- **OR-004**: CloudWatch alarms or basic metric checks MUST help identify failed runs, queue backlog, or repeated ingestion errors.

### Query and Analytics Requirements

- **AR-001**: The processed dataset MUST support basic analytical queries for repository popularity, language distribution, recency, and issue counts.
- **AR-002**: The project MUST provide example Athena queries that demonstrate the end-to-end workflow and validate the resulting data readiness.

### Key Entities

- **GitHubRepository**: Represents a single repository record retrieved from GitHub, including identity, metadata, and timestamps.
- **RawDataObject**: Represents the persisted JSON payload stored in the raw S3 bucket before transformation.
- **ProcessingEvent**: Represents the event message delivered through EventBridge and SQS, containing the metadata needed to rehydrate the work item.
- **ProcessedRepositoryRecord**: Represents the transformed row in Parquet format suited for Athena analysis.
- **PipelineRun**: Represents the execution context for any ingestion or ETL invocation, including status, timing, and failure details.

## Success Criteria

### Measurable Outcomes

- **SC-001**: The system can run a scheduled ingestion and persist raw GitHub metadata into S3 without manual intervention.
- **SC-002**: The processed Parquet dataset is available in the Glue Catalog and can be queried successfully in Athena.
- **SC-003**: At least five example analytical queries can be executed successfully against the processed dataset, including stars, forks, languages, and recency.
- **SC-004**: A simulated ETL failure is retried according to policy and ultimately lands in the DLQ, with the failure observable in CloudWatch.
- **SC-005**: The project deploys via Terraform in a repeatable way without requiring unmanaged manual AWS configuration.

## Assumptions

- The project targets a small, fixed list of public GitHub repositories rather than broad repository discovery across the entire GitHub ecosystem.
- Authentication is optional for this OJT project; unauthenticated GitHub REST API access is acceptable unless rate limits or access requirements justify a token-based setup.
- S3 raw events trigger the processing workflow through EventBridge/SQS rather than directly invoking Glue so that the queue acts as the reliability and decoupling layer.
- The project scope includes a single data lake pattern with raw and processed layers, not a multi-region or multi-account design.
- The processed dataset is meant for simple repository analytics and does not require ML, complex feature engineering, or user-facing dashboards.
