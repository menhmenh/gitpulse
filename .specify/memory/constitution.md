# GitPulse Constitution

## Core Principles

### I. Security by Default
All AWS resources and data flows MUST follow least-privilege IAM, encryption at rest, and explicit access boundaries. Secrets and credentials must never be hardcoded; credential material must be supplied through AWS-native mechanisms such as environment variables, parameter store, or IAM role assumptions. The system must minimize shared access and separate permissions by function and data layer.

### II. Reliable Data Processing
The pipeline MUST prioritize durability, idempotent processing, and explicit failure handling. Data state transitions must be traceable, retries must be bounded and observable, and failed work must be isolated in a dead-letter queue for investigation instead of being silently lost.

### III. Observable Operation
Every run of the ingestion and transformation flow must emit logs and metrics that support incident analysis. The team must be able to determine whether the pipeline succeeded, where it failed, what messages were retried, and what happened to the failed data without guesswork.

### IV. Infrastructure as Code and Reproducibility
Terraform MUST define the deployment model for all AWS infrastructure. Infrastructure changes must be repeatable, versioned, and capable of rebuilding the same environment in a predictable way. The project must stay within a compact, demonstrable scope suitable for an OJT exercise.

### V. Simplicity Over Scope
The implementation MUST stay focused on a minimal production-inspired AWS data pipeline: fetch repository metadata, preserve raw data, transform it to a queryable format, and validate the end-to-end flow. No unnecessary AWS services, multi-region designs, or operational complexity should be introduced without strong justification.

## Technical Constraints

GitPulse MUST use an AWS-native architecture that matches the project goal: GitHub API ingestion through Lambda, raw data landing in S3, durable event flow through EventBridge and SQS, ETL processing in AWS Glue, processed Parquet output, Glue Catalog exposure, and Athena-based analytics. Terraform must own the infrastructure layer. The implementation must favor simple, readable patterns over enterprise-scale complexity.

Security requirements are non-negotiable: least-privilege IAM roles for Lambda, Glue, and storage access; KMS encryption for sensitive or operationally important storage; restricted bucket access; and no embedded secrets in code or Terraform variables that are committed to the repository. Access must be scoped to the minimal set of resources needed by each component.

## Development Workflow

The implementation must follow the Spec Kit workflow: project constitution, feature specification, technical plan, and tasks. Requirements must be testable, traceable, and aligned to the architecture. Design decisions must explicitly cover reliability, security, observability, data quality, and reproducibility. Any scope change must be reflected in the specification and plan before implementation begins.

## Governance

This constitution governs the GitPulse project and supersedes ad hoc implementation choices that conflict with it. Any material change to the architecture, security posture, or data-flow model must be documented in the specification and plan before implementation. Complexity must be justified by a requirement, and simpler alternatives must be considered before introducing additional services or patterns.

**Version**: 1.0.0 | **Ratified**: 2026-08-20 | **Last Amended**: 2026-08-26
