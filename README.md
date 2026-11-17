# GitPulse — Serverless GitHub Engineering & DORA Analytics Lakehouse on AWS & Snowflake

GitPulse is an event-driven data engineering pipeline provisioned via Terraform and containerized via Docker. It ingests metadata across **8 GitHub REST API resource types**, stores raw immutable JSON in an S3 Data Lake, transforms records with explicit PySpark schemas into date-partitioned **Apache Iceberg** lakehouse tables and Parquet datasets alongside daily **DORA proxy metrics**, and exposes analytics via **Snowflake External Tables** (with S3 auto-refresh) and dbt.

---

## ⚡ Quickstart: Apache Iceberg & PySpark Verification Demo (One Command)

> [!TIP]
> **Zero Credentials Needed**: You can reproduce the full PySpark + Apache Iceberg pipeline, late-arriving merge, time-travel snapshot query, and contract enforcement offline on any laptop using Docker without needing AWS credentials or cloud spend.

```bash
# 1. Clone and enter repository
git clone https://github.com/your-username/gitpulse.git && cd gitpulse

# 2. Run the complete end-to-end verification demo with 1 command:
docker compose up gitpulse-demo
```

Or run directly with Bash / PowerShell if Java 17+ and Python 3.11 are installed:
```bash
bash scripts/run_demo.sh
```

### What the Automated Demo Verifies (Core DoD):
1. **Cryptographic Data Contracts**: Verifies raw JSON pages and SHA-256 checksums from `manifest.json`. Injects 5 corruption scenarios (missing page, checksum mismatch, malformed JSON, missing mandatory keys, invalid timestamp) and proves the pipeline aborts with exit code 1 before touching table state.
2. **Idempotent MERGE INTO**: Loads `batch_1` baseline, updates existing `in_progress` runs to `completed` in `batch_2`, routes late-arriving records (run 99) to historical partition `2026-09-30`, and verifies replaying `batch_3` leaves record count and table snapshot identical.
3. **Multi-Repo Key Isolation**: Preserves identical workflow IDs (ID `101`) appearing across different repositories (`apache/spark` and `apache/iceberg`) as independent records without collision.
4. **Time-Travel Snapshot Query**: Audits table state before vs after late-arriving ingestion using Iceberg metadata snapshot IDs (`query_snapshot_time_travel_comparison`).
5. **Analytical SQL Views**: Generates `v_workflow_run_reliability` reporting success rates, failure counts, and latency-to-completion with safe division handling zero completed runs.

### 💼 Portfolio & CV Highlight Bullets (MoMo Data Engineer Trainee)
- **PySpark & Apache Iceberg Lakehouse**: Architected idempotent `MERGE INTO` pipeline on Apache Iceberg partitioned by `days(created_at)`, resolving late-arriving workflow telemetry and version conflicts (`updated_at` > `run_attempt`) with 100% replay reproducibility.
- **Data Quality & Contract Enforcement**: Engineered a 2-phase manifest validation layer with SHA-256 cryptographic checksums, mandatory schema contracts, and atomic rollback, guaranteeing corrupted raw files never pollute lakehouse tables.
- **Analytical Reliability & Time Travel**: Designed analytical views for workflow failure trends and flaky job detection, demonstrating Apache Iceberg snapshot time-travel to verify historical delta states before and after late-arriving data reconciliations.

*Engineering Note on AI-Assisted Implementation*: Automated scaffolding suggestions were rigorously verified against Apache Iceberg 1.7.1 copy-on-write table specs, adding explicit timestamp type casting and version ordering guards to prevent edge-case state regressions during out-of-order retransmissions.

---

## Table of Contents

1. [Architecture & Module Layout](#1-architecture--module-layout)
2. [Prerequisites](#2-prerequisites)
3. [Deployment Guide (From Scratch)](#3-deployment-guide-from-scratch)
4. [Triggering the Pipeline Manually](#4-triggering-the-pipeline-manually)
5. [Step Functions Transform Tail](#5-step-functions-transform-tail)
6. [DORA Metrics & Failure-Signal Proxy](#6-dora-metrics--failure-signal-proxy)
7. [Running the Sample Analytics Queries (Snowflake & SQL)](#7-running-the-sample-analytics-queries-snowflake--sql)
8. [Failure Simulation & DLQ / SNS Verification](#8-failure-simulation--dlq--sns-verification)
9. [Security & IAM Audit Verification](#9-security--iam-audit-verification)
10. [Teardown (`terraform destroy`)](#10-teardown-terraform-destroy)

---

## 1. Architecture & Module Layout

```text
EventBridge Scheduler (cron)
   │
   ▼
Producer Lambda (gitpulse-producer)
   │  Enqueues 1 message per (repo, resource_type) with shared batch run_ts
   ▼
SQS Job Queue (gitpulse-job-queue) ──(maxReceiveCount = 3)──► SQS DLQ (gitpulse-dlq)
   │                                                              │
   │ (batch_size = 1, max_concurrency = 5)                        ▼
   ▼                                                     CloudWatch DLQ Alarm
Ingestion Lambda (gitpulse-github-ingestion)             (gitpulse-dlq-depth)
   │  Reads GitHub PAT from Secrets Manager at runtime            │
   │  Paginates + writes atomic JSON pages (rollback on error)    │
   ▼                                                              ▼
S3 Raw Zone (s3://gitpulse-raw-<env>-<account_id>/raw/github/...) │
   │                                                              │
   │  SQS Queue Drained Alarm (Visible + NotVisible == 0 for 5m)  │
   ▼                                                              ▼
EventBridge Rule (gitpulse-sqs-drained-to-sfn)           Shared SNS Topic
   │                                                     (gitpulse-pipeline-alerts)
   ▼                                                              ▲
Step Functions State Machine (gitpulse-transform-tail)            │
   ├─► 1. CrawlRaw (gitpulse-raw-crawler) ───────(Catch/Fail)─────┤
   ├─► 2. TransformGlueJob (gitpulse-transform-job) ─(Catch/Fail)─┤
   └─► 3. CrawlProcessed (gitpulse-processed-crawler) ─(Catch)────┘
          │
          ▼
S3 Processed Zone (Parquet, partitioned by dt=YYYY-MM-DD)
   ├─► AWS Glue Data Catalog (gitpulse_processed)
   └─► Snowflake Storage Integration (IAM Role + External Stage)
          ├─► S3 Object Notification -> Snowflake SQS (Auto-refresh)
          └─► Snowflake External Tables & dbt Models (GITPULSE_DB.PROCESSED)
```

### Repository Structure

| Path | Purpose |
| :--- | :--- |
| `terraform/versions.tf`, `provider.tf`, `main.tf` | Terraform `>= 1.15.0` and AWS Provider `~> 6.0` setup |
| `terraform/variables.tf`, `outputs.tf` | Configurable input variables (repos, schedules, DORA proxies, alert email) and stack outputs |
| `terraform/kms.tf`, `s3.tf`, `secrets.tf` | Customer-Managed KMS key (`alias/gitpulse-data-lake`), 2 KMS-encrypted S3 buckets (`raw`, `processed`), Secrets Manager GitHub token shell |
| `terraform/sqs.tf`, `lambda.tf` | SQS `job_queue` + `dlq` (with redrive allow policy), Producer & Ingestion Lambda functions |
| `terraform/glue.tf`, `glue_scripts/transform_and_dora.py` | Separate `gitpulse_raw` & `gitpulse_processed` Glue databases, crawlers, and Python Shell ETL + DORA job |
| `terraform/snowflake_integration.tf` | Snowflake IAM storage integration role, scoped S3 + KMS access, and S3 bucket auto-refresh notifications |
| `terraform/sfn.tf` | Step Functions transform tail (`gitpulse-transform-tail`), shared SNS topic (`gitpulse-pipeline-alerts`), topic policy, and email subscription |
| `terraform/eventbridge.tf` | Producer schedule, primary SQS-drained-to-SFN rule, and optional time-delayed fallback rule |
| `terraform/alarms.tf` | CloudWatch metric alarms (DLQ depth, SQS drained metric math, Lambda errors/duration, Glue & SFN failures) |
| `terraform/iam.tf` | Least-privilege IAM roles and scoped inline policies for every component |
| `snowflake/setup.sql`, `snowflake/queries.sql` | Snowflake database, warehouse, external stage, external tables with auto-refresh, and analytical queries |
| `dbt/` | dbt project with source definitions and staging models |
| `tests/queries.sql` | Sample SQL queries (commits, issues, PRs, DORA metrics, partition pruning) |
| `tests/simulate_failures_m5_3.py` | Automated local + AWS end-to-end failure simulation suite (M5-3) |
| `tests/security_audit_m5_4.py` | Automated least-privilege IAM & credential hygiene verification suite (M5-4) |
| `tests/test_workflow_ingestion.py` | Ingestion bounded window and timeout regression test suite |
| `tests/test_transform_regression.py` | Multi-repo deduplication and deterministic partition date fallback regression test suite |
| `requirements.txt`, `requirements-dev.txt` | Core and development dependencies declaration |

---

## 2. Prerequisites

Before deploying from scratch, ensure the following tools and credentials are installed on your machine:

1. **Terraform CLI**: `>= 1.15.0`
   ```bash
   terraform version
   ```
2. **AWS CLI v2**: Configured with a named profile (`gitpulse` by default, or pass `-var="aws_profile=<your-profile>"`) with permissions to provision S3, KMS, IAM, Lambda, SQS, SNS, Glue, Step Functions, EventBridge, Athena, Secrets Manager, and CloudWatch.
   ```bash
   aws configure --profile gitpulse
   aws sts get-caller-identity --profile gitpulse
   ```
3. **GitHub Personal Access Token (PAT)**:
   - Generate a GitHub Personal Access Token (classic token with `public_repo` scope, or fine-grained token with read-only access to public repositories).
   - **Never commit this token to Git or Terraform files.** You will inject it directly into AWS Secrets Manager after `terraform apply` (Step 3.3).
4. **Python `>= 3.11`** *(optional, for running verification scripts)*:
   ```bash
   python --version
   ```

---

## 3. Deployment Guide (From Scratch)

### Step 3.1 — Initialize Terraform

```bash
cd terraform
terraform init
```

### Step 3.2 — Review Plan & Apply Stack

Pass your email address via `-var="alert_email=..."` so SNS subscribes your inbox to pipeline failure alerts:

```bash
terraform plan -var="alert_email=your-email@example.com"
terraform apply -var="alert_email=your-email@example.com"
```

*(Optional)* You can also customize target repositories or AWS profile/region via variables:
```bash
terraform apply \
  -var="aws_profile=gitpulse" \
  -var="aws_region=ap-southeast-1" \
  -var="alert_email=your-email@example.com" \
  -var='github_repos=["vercel/next.js"]'
```

### Step 3.3 — Populate the GitHub Token in AWS Secrets Manager

Terraform creates the KMS-encrypted secret `gitpulse/github-token` with a placeholder value (`REPLACE_ME_VIA_CLI`) and `lifecycle { ignore_changes = [secret_string] }` so subsequent `terraform apply` runs never overwrite your secret.

Set your real GitHub token via AWS CLI:

```bash
aws secretsmanager put-secret-value \
  --profile gitpulse \
  --region ap-southeast-1 \
  --secret-id gitpulse/github-token \
  --secret-string '{"token":"<YOUR_GITHUB_PAT_TOKEN>"}'
```

### Step 3.4 — Confirm the SNS Email Subscription

Check the inbox of `your-email@example.com` for an email from **AWS Notifications** with subject `AWS Notification - Subscription Confirmation` and click **Confirm subscription**.

---

## 4. Triggering the Pipeline Manually

You do not need to wait for the daily EventBridge cron (`cron(0 0 * * ? *)`) to run the pipeline.

### Step 4.1 — Trigger the Producer Lambda

Invoke `gitpulse-producer` to enumerate all 8 resource types for each configured repository and enqueue messages into `gitpulse-job-queue`:

```bash
aws lambda invoke \
  --profile gitpulse \
  --region ap-southeast-1 \
  --function-name gitpulse-producer \
  --payload '{}' \
  producer_out.json && cat producer_out.json
```

Expected response:
```json
{"messages_enqueued": 8, "run_ts": "20260925T041500Z"}
```

### Step 4.2 — Observe SQS Ingestion & S3 Raw Landing

`gitpulse-github-ingestion` automatically consumes messages from `gitpulse-job-queue` (concurrency capped at `5` to respect GitHub API rate limits) and writes raw JSON pages to S3:

```bash
# Check queue status
aws sqs get-queue-attributes \
  --profile gitpulse \
  --region ap-southeast-1 \
  --queue-url $(aws sqs get-queue-url --profile gitpulse --region ap-southeast-1 --queue-name gitpulse-job-queue --query QueueUrl --output text) \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

# List ingested raw files in S3
RAW_BUCKET=$(terraform output -raw s3_bucket_names | python -c "import sys, json; print(json.load(sys.stdin)['raw'])")
aws s3 ls "s3://${RAW_BUCKET}/raw/github/" --recursive --profile gitpulse
```

---

## 5. Step Functions Transform Tail

### 5.1 Purpose & State Machine Workflow

The Step Functions state machine **`gitpulse-transform-tail`** (`terraform/sfn.tf`) orchestrates the post-ingestion transformation pipeline in strict sequence so downstream steps never run on incomplete data:

1. **`CrawlRaw`**: Starts `gitpulse-raw-crawler` and polls (`WaitRaw` 30s → `GetCrawlRaw` → `CheckRawRunning` → `CheckRawResult`) until `State == "READY"` and `LastCrawl.Status == "SUCCEEDED"`.
2. **`TransformGlueJob`**: Runs the Glue Python Shell job `gitpulse-transform-job` synchronously (`arn:aws:states:::glue:startJobRun.sync`) to deduplicate raw JSON pages, flatten nested fields, write partitioned Parquet files (`dt=YYYY-MM-DD`), and compute daily DORA metrics (`dora_metrics/dt=YYYY-MM-DD/`).
3. **`CrawlProcessed`**: Starts `gitpulse-processed-crawler` and polls until completion so new Parquet tables and `dt=` partitions are immediately registered in the `gitpulse_processed` Glue catalog database.
4. **`FailNotify` (Unified Error Channel)**: Every `Catch` and failed `Choice` branch across all 3 stages routes through `NotifyCrawlRawFailed` / `NotifyTransformFailed` / `NotifyCrawlProcessedFailed` into `FailNotify`, which publishes to the **shared SNS topic (`gitpulse-pipeline-alerts`)** with the failing stage name and SFN Execution ARN before entering `PipelineFailed`.

### 5.2 Primary Trigger: SQS Drained Signal

Instead of relying on a brittle fixed delay between ingestion and transformation:
- CloudWatch metric math alarm **`gitpulse-job-queue-drained`** (`aws_cloudwatch_metric_alarm.sqs_drained` in `terraform/alarms.tf`) computes `m1 + m2` (`ApproximateNumberOfMessagesVisible + ApproximateNumberOfMessagesNotVisible`).
- When the Producer enqueues work, `m1 + m2 > 0` pushes the alarm into the `ALARM` state (`datapoints_to_alarm = 1`).
- Once all messages are processed and `m1 + m2 == 0` is sustained for **`var.sqs_drained_sustained_minutes` (default `5` minutes)**, the alarm transitions from `ALARM` to `OK`.
- EventBridge rule **`gitpulse-sqs-drained-to-sfn`** (`terraform/eventbridge.tf`) catches the `ALARM → OK` state change and automatically starts `gitpulse-transform-tail`.

### 5.3 How to Re-run the Transform Tail Manually

To trigger `gitpulse-transform-tail` on demand without waiting for the SQS drained alarm transition:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --profile gitpulse --query Account --output text)
SFN_ARN="arn:aws:states:ap-southeast-1:${ACCOUNT_ID}:stateMachine:gitpulse-transform-tail"

EXEC_ARN=$(aws stepfunctions start-execution \
  --profile gitpulse \
  --region ap-southeast-1 \
  --state-machine-arn "${SFN_ARN}" \
  --input '{}' \
  --query executionArn --output text)

echo "Started execution: ${EXEC_ARN}"

# Check execution status
aws stepfunctions describe-execution \
  --profile gitpulse \
  --region ap-southeast-1 \
  --execution-arn "${EXEC_ARN}" \
  --query "{status: status, startDate: startDate, stopDate: stopDate}"
```

### 5.4 Where the Fallback Time-Delay Trigger Lives

A backup cron-based EventBridge rule **`gitpulse-time-delay-to-sfn`** (`aws_cloudwatch_event_rule.time_delay_to_sfn`) is defined in **`terraform/eventbridge.tf` (lines 57–72)** and controlled by two variables in **`terraform/variables.tf` (lines 80–90)**:
- `enable_transform_tail_fallback`: `false` by default (keeping `sqs_drained` as the single active trigger).
- `transform_tail_fallback_schedule_expression`: `"cron(45 0 * * ? *)"` (45 minutes after `producer_schedule_expression`).

To enable the time-delay fallback alongside or instead of the drained signal:
```bash
terraform apply -var="enable_transform_tail_fallback=true"
```

---

## 6. DORA Metrics & Failure-Signal Proxy

> [!NOTE]
> **Engineering Proxy Notice:** The GitHub REST API does not provide automated end-to-end incident-to-deployment trace linking. Consequently, the metrics below are **DORA heuristic proxies** rather than certified CD telemetry. They provide actionable engineering health signals while clearly documenting underlying assumptions.

### 6.1 How the Four DORA Proxy Metrics Map to GitPulse Tables

`terraform/glue_scripts/transform_and_dora.py` (`build_dora_metrics()`) computes all four DORA proxy metrics per `(repo, environment, dt)` across a continuous daily date spine and writes partitioned Parquet files to `s3://<processed-bucket>/dora_metrics/dt=YYYY-MM-DD/` (exposed via Snowflake external tables and AWS Glue Data Catalog):

| DORA Metric | Column(s) in `dora_metrics` | Source Tables in GitPulse | Proxy Calculation Logic & Assumptions |
| :--- | :--- | :--- | :--- |
| **1. Deployment Frequency (DF)** | `deploy_count` (`INT`) | `deployments` *(primary)*<br>`releases` *(fallback)* | Count of deployments per `(repo, environment, dt)` where `environment` matches `var.dora_prod_environments`. If a repo has no production deployments, falls back to published releases (`releases.published_at`). Zero-deployment days are explicitly populated with `deploy_count = 0` via the calendar date spine. |
| **2. Lead Time for Changes (LT)** | `lead_time_p50` (`DOUBLE`)<br>`lead_time_p95` (`DOUBLE`) | `pull_requests` (`merged_at`)<br>`deployments` / `releases` | **Proxy**: Median (`p50`) and 95th percentile (`p95`) hours elapsed from nearest prior merged PR to the production deployment timestamp on date `dt` (nearest-prior-PR heuristic). |
| **3. Change Failure Rate (CFR)** | `change_failure_rate` (`DOUBLE`) | `deployments` / `releases`<br>+ `issues` & `pull_requests` *(failure proxy)* | **Proxy**: `(failure_count / deploy_count) * 100` rounded to 2 decimal places (`0.0` to `100.0`). Returns `NULL` (`NaN`) on days with 0 deployments. Incidents are detected via failure label keywords and revert PR title prefixes. |
| **4. Mean Time to Restore (MTTR)** | `mttr_minutes` (`DOUBLE`) | `issues` (`created_at`, `closed_at`, `labels`) | **Proxy**: Average duration in minutes (`(closed_at - created_at) / 60`) for resolved failure/incident issues opened/resolved on date `dt`. |

### 6.2 The Failure-Signal Proxy (Label List + Revert-Prefix Matching)

Because public GitHub repositories do not expose an external incident management feed out-of-the-box, GitPulse uses two configurable GitHub-native proxies in `transform_and_dora.py`:

1. **Failure/Incident Issue Labels (`var.dora_failure_labels`)**:
   - Any issue whose `labels` list contains a label (or substring, case-insensitive) in `dora_failure_labels` is classified as a failure incident (`failure_count` and `mttr_minutes`).
   - Default list: `["bug", "incident", "hotfix", "template: bug", "type: bug", "kind/bug"]`.
2. **Revert / Hotfix Merged Pull Requests (`var.dora_revert_prefixes`)**:
   - Any merged PR (`merged_at IS NOT NULL`) whose `title` starts with a prefix in `dora_revert_prefixes` (case-insensitive) is counted as a failure remediation event on its merge date.
   - Default prefixes: `["revert", "hotfix:"]`.

### 6.3 Configuring the DORA Proxy in Terraform

Override any of the three DORA variables in `terraform/variables.tf` via CLI or `terraform.tfvars`:

```hcl
# terraform.tfvars
dora_prod_environments = ["production", "prod", "release-canary"]
dora_failure_labels    = ["bug", "incident", "hotfix", "sev-1", "sev-2"]
dora_revert_prefixes   = ["revert", "hotfix:", "rollback:"]
```

Terraform passes these directly into `aws_glue_job.transform.default_arguments` (`--DORA_PROD_ENVIRONMENTS`, `--DORA_FAILURE_LABELS`, `--DORA_REVERT_PREFIXES`) in `terraform/glue.tf`.

### 6.4 How to Swap in a Real Incident Source Later

To replace the GitHub label/revert proxy with a dedicated incident system (e.g., **PagerDuty**, **Opsgenie**, **Jira Service Management**, or **GitHub Deployment Statuses**):

1. **Land raw incident records in S3**: Write incident payloads to `s3://<raw-bucket>/raw/incidents/<service>/run_ts=<ts>/page_0.json` containing `incident_id`, `repo`, `environment`, `created_at` (detected timestamp), and `resolved_at` (restored timestamp).
2. **Load incident records in `transform_and_dora.py`**: Inside `build_dora_metrics()` (`terraform/glue_scripts/transform_and_dora.py`), replace the proxy `failure_dates` / `mttr_by_date` blocks with an aggregation over `incidents_df`:
   - `failure_count`: count of incidents per `(repo, environment, dt)` where `dt = to_date_str(incidents_df["created_at"])`.
   - `mttr_minutes`: mean `(resolved_at - created_at).dt.total_seconds() / 60.0` grouped by `(repo, environment, dt)`.
3. **No downstream schema changes required**: Because `dora_metrics` retains the exact same Parquet schema (`dt`, `repo`, `environment`, `deploy_count`, `lead_time_p50`, `lead_time_p95`, `change_failure_rate`, `mttr_minutes`), external tables in Snowflake and dbt models continue to work without modification.

---

## 7. Running the Sample Analytics Queries (Snowflake & Athena)

Analytics queries are provided in both **Snowflake SQL** (`snowflake/queries.sql`) and **Athena/Trino SQL** (`tests/queries.sql`).

### Running in Snowflake (Primary Lakehouse Engine)

Connect to Snowflake, use the provisioned warehouse and schema, and query the auto-refresh external tables:

```sql
USE WAREHOUSE GITPULSE_WH;
USE SCHEMA GITPULSE_DB.PROCESSED;

-- 1. Commits by week
SELECT repo,
       DATE_TRUNC('week', commit_author_date) AS week,
       COUNT(*) AS commit_count
FROM commits
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo, DATE_TRUNC('week', commit_author_date)
ORDER BY week DESC;

-- 2. Issue resolution trend by state
SELECT repo,
       state,
       DATE_TRUNC('week', created_at) AS week_opened,
       COUNT(*) AS issue_count,
       AVG(DATEDIFF('hour', created_at, closed_at)) AS avg_hours_to_close
FROM issues
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo, state, DATE_TRUNC('week', created_at)
ORDER BY week_opened DESC;

-- 3. PRs and merged counts by repository
SELECT repo,
       COUNT(*) AS pull_request_count,
       COUNT_IF(merged_at IS NOT NULL) AS merged_count
FROM pull_requests
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo
ORDER BY pull_request_count DESC;

-- 4. DORA Proxy Metrics
SELECT repo, environment, dt, deploy_count, lead_time_p50, lead_time_p95, change_failure_rate, mttr_minutes
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
ORDER BY dt DESC;
```

### Running in AWS Athena (Optional / Alternative)

Run queries from `tests/queries.sql` in the **Amazon Athena Console** or via AWS CLI:

```bash
QID=$(aws athena start-query-execution \
  --profile gitpulse \
  --region ap-southeast-1 \
  --work-group gitpulse-workgroup \
  --query-execution-context Database=gitpulse_processed \
  --query-string "SELECT repo, environment, dt, deploy_count, lead_time_p50, lead_time_p95, change_failure_rate, mttr_minutes FROM dora_metrics ORDER BY dt DESC LIMIT 10;" \
  --query QueryExecutionId --output text)

aws athena get-query-results --profile gitpulse --region ap-southeast-1 --query-execution-id "${QID}"
```
  --region ap-southeast-1 \
  --work-group gitpulse-workgroup \
  --query-execution-context Database=gitpulse_processed \
  --query-string "SELECT repo, environment, dt, deploy_count, lead_time_p50, lead_time_p95, change_failure_rate, mttr_minutes FROM dora_metrics ORDER BY dt DESC LIMIT 10;" \
  --query QueryExecutionId --output text)

aws athena get-query-results --profile gitpulse --region ap-southeast-1 --query-execution-id "${QID}"
```

---

## 8. Failure Simulation & DLQ / SNS Verification

The project includes an automated verification script **`tests/simulate_failures_m5_3.py`** covering all three reliability chains:

1. **Ingestion Lambda Failure → SQS Retry (`ApproximateReceiveCount` 1→3) → DLQ (`gitpulse-dlq`) → CloudWatch Alarm (`gitpulse-dlq-depth`) → Shared SNS (`gitpulse-pipeline-alerts`) + Zero Partial S3 Data**.
2. **Glue Transform Job Failure (`--SIMULATE_FAILURE=true`) → Step Functions `Catch` → `FailNotify` → Shared SNS (`gitpulse-pipeline-alerts`)**.
3. **Glue Crawler Failure → Step Functions `Catch`/`Choice` → `FailNotify` → Shared SNS (`gitpulse-pipeline-alerts`)**.

### Run Local Unit Simulation (No AWS Credentials Required)

```bash
python tests/simulate_failures_m5_3.py --mode unit
```

### Run Live End-to-End AWS Failure Simulation

```bash
python tests/simulate_failures_m5_3.py --mode aws --profile gitpulse --region ap-southeast-1
```

### Manual CLI Failure Simulation (Ingestion → DLQ)

To manually enqueue a single failing job message and inspect the DLQ:

```bash
QUEUE_URL=$(aws sqs get-queue-url --profile gitpulse --region ap-southeast-1 --queue-name gitpulse-job-queue --query QueueUrl --output text)
DLQ_URL=$(aws sqs get-queue-url --profile gitpulse --region ap-southeast-1 --queue-name gitpulse-dlq --query QueueUrl --output text)

# Enqueue a message that simulates a mid-write failure and rolls back page_0.json
aws sqs send-message \
  --profile gitpulse \
  --region ap-southeast-1 \
  --queue-url "${QUEUE_URL}" \
  --message-body '{"repo":"vercel/next.js","resource_type":"repository","since":"2026-09-24T00:00:00Z","run_ts":"20260925T999999Z_manual_fail","simulate_failure":"partial_write_rollback"}'

# Allow roughly 54 minutes for 3 receives with the default 1080s VisibilityTimeout:
aws sqs get-queue-attributes \
  --profile gitpulse \
  --region ap-southeast-1 \
  --queue-url "${DLQ_URL}" \
  --attribute-names ApproximateNumberOfMessages
```

---

### Workflow Runs Timeout Recovery

Workflow ingestion filters GitHub Actions requests with `created=since..run_ts`.
The end of the window comes from the original Producer batch, so redriving an old
DLQ message does not fetch all newer runs. Searches exceeding GitHub's 1,000-result
limit are split into smaller time windows before pagination. Each API page logs
`github_page_start` and `github_page_complete` with its duration.

The ingestion Lambda timeout is 180 seconds; the job queue visibility timeout is
six times that value (1080 seconds). Terraform updates queue visibility before
increasing the Lambda timeout.

Run the offline regression tests before deploying:

```bash
python -m unittest discover -s tests -p test_workflow_ingestion.py -v
terraform -chdir=terraform validate
```

Deploy with the Terraform plan/apply workflow described above. Then retry one
failed workflow job and verify `ingestion_complete` in CloudWatch before starting
a DLQ redrive. Do not purge the DLQ to recover data.

## 9. Security & IAM Audit Verification

Run the automated security audit suite to verify least-privilege IAM policies, zero wildcard (`*`) actions in role policies, zero hardcoded GitHub tokens across the repository, runtime Secrets Manager audit logging, S3 customer-managed KMS encryption, and Snowflake integration role least-privilege permissions:

```bash
python tests/security_audit_m5_4.py
```

---

## 10. Teardown (`terraform destroy`)

Both S3 buckets (`raw`, `processed`) are configured with `force_destroy = true`, so Terraform cleanly empties and deletes all resources in a single command:

```bash
cd terraform
terraform destroy -var="alert_email=your-email@example.com"
```
