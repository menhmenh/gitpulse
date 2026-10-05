-- ============================================================
-- GitPulse [M6] - Snowflake serving layer setup
-- Replaces Athena. Reads the SSE-KMS Parquet in the S3 processed zone.
-- Run top to bottom; section 3 needs the Terraform apply done first.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Warehouse, database, schema
-- ------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS GITPULSE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 900;   -- replaces Athena's bytes_scanned_cutoff guardrail

CREATE DATABASE IF NOT EXISTS GITPULSE_DB;
CREATE SCHEMA IF NOT EXISTS GITPULSE_DB.PROCESSED;

USE WAREHOUSE GITPULSE_WH;
USE SCHEMA GITPULSE_DB.PROCESSED;

-- ------------------------------------------------------------
-- 2. Storage integration  (RUN THIS BEFORE terraform apply)
--    The role does not need to exist yet; Snowflake only records the ARN.
-- ------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS gitpulse_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::755905325039:role/gitpulse-snowflake-integration-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://gitpulse-processed-dev-755905325039/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into terraform.tfvars:
--   snowflake_storage_aws_iam_user_arn = "..."
--   snowflake_storage_aws_external_id  = "..."
DESC INTEGRATION gitpulse_s3_integration;

-- >>> now run `terraform apply` <<<

-- ------------------------------------------------------------
-- 3. File format + external stage
--    No ENCRYPTION clause: it is only needed when UNLOADING to S3. For reads,
--    S3 decrypts transparently as long as the role has kms:Decrypt.
-- ------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT gitpulse_parquet_format TYPE = PARQUET;

CREATE OR REPLACE STAGE gitpulse_processed_stage
  URL = 's3://gitpulse-processed-dev-755905325039/'
  STORAGE_INTEGRATION = gitpulse_s3_integration
  FILE_FORMAT = gitpulse_parquet_format;

LIST @gitpulse_processed_stage;   -- AC: must succeed

-- ------------------------------------------------------------
-- 4. External tables (9). dt comes from the S3 path (METADATA$FILENAME).
--    Typed columns cover what the queries need; VALUE holds the full row, so
--    any other column is reachable as VALUE:column_name. To see every column:
--      SELECT * FROM TABLE(INFER_SCHEMA(LOCATION=>'@gitpulse_processed_stage/commits/',
--                                       FILE_FORMAT=>'gitpulse_parquet_format'));
--    Timestamps go through TRY_TO_TIMESTAMP_NTZ(...::VARCHAR) so they parse whether
--    the Parquet timestamp surfaces as an ISO string or an epoch number.
-- ------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE commits (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  sha VARCHAR AS (VALUE:sha::VARCHAR),
  commit_author_date TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:commit_author_date::VARCHAR)),
  commit_author_name VARCHAR AS (VALUE:commit_author_name::VARCHAR),
  commit_message VARCHAR AS (VALUE:commit_message::VARCHAR)
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/commits/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE issues (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  number NUMBER AS (VALUE:number::NUMBER),
  title VARCHAR AS (VALUE:title::VARCHAR),
  state VARCHAR AS (VALUE:state::VARCHAR),
  labels VARCHAR AS (VALUE:labels::VARCHAR),
  user_login VARCHAR AS (VALUE:user_login::VARCHAR),
  created_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:created_at::VARCHAR)),
  updated_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:updated_at::VARCHAR)),
  closed_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:closed_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/issues/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE pull_requests (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  number NUMBER AS (VALUE:number::NUMBER),
  title VARCHAR AS (VALUE:title::VARCHAR),
  state VARCHAR AS (VALUE:state::VARCHAR),
  labels VARCHAR AS (VALUE:labels::VARCHAR),
  user_login VARCHAR AS (VALUE:user_login::VARCHAR),
  created_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:created_at::VARCHAR)),
  updated_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:updated_at::VARCHAR)),
  closed_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:closed_at::VARCHAR)),
  merged_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:merged_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/pull_requests/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE releases (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  tag_name VARCHAR AS (VALUE:tag_name::VARCHAR),
  name VARCHAR AS (VALUE:name::VARCHAR),
  prerelease BOOLEAN AS (VALUE:prerelease::BOOLEAN),
  created_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:created_at::VARCHAR)),
  published_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:published_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/releases/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE contributors (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  login VARCHAR AS (VALUE:login::VARCHAR),
  contributions NUMBER AS (VALUE:contributions::NUMBER)
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/contributors/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE deployments (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  sha VARCHAR AS (VALUE:sha::VARCHAR),
  ref VARCHAR AS (VALUE:ref::VARCHAR),
  environment VARCHAR AS (VALUE:environment::VARCHAR),
  created_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:created_at::VARCHAR)),
  updated_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:updated_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/deployments/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE workflow_runs (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  name VARCHAR AS (VALUE:name::VARCHAR),
  event VARCHAR AS (VALUE:event::VARCHAR),
  status VARCHAR AS (VALUE:status::VARCHAR),
  conclusion VARCHAR AS (VALUE:conclusion::VARCHAR),
  run_number NUMBER AS (VALUE:run_number::NUMBER),
  created_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:created_at::VARCHAR)),
  updated_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:updated_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/workflow_runs/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE repository (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  id NUMBER AS (VALUE:id::NUMBER),
  full_name VARCHAR AS (VALUE:full_name::VARCHAR),
  stargazers_count NUMBER AS (VALUE:stargazers_count::NUMBER),
  forks_count NUMBER AS (VALUE:forks_count::NUMBER),
  open_issues_count NUMBER AS (VALUE:open_issues_count::NUMBER),
  updated_at TIMESTAMP_NTZ AS (TRY_TO_TIMESTAMP_NTZ(VALUE:updated_at::VARCHAR))
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/repository/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

CREATE OR REPLACE EXTERNAL TABLE dora_metrics (
  dt DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
  repo VARCHAR AS (VALUE:repo::VARCHAR),
  environment VARCHAR AS (VALUE:environment::VARCHAR),
  deploy_count NUMBER AS (VALUE:deploy_count::NUMBER),
  lead_time_p50 FLOAT AS (VALUE:lead_time_p50::FLOAT),
  lead_time_p95 FLOAT AS (VALUE:lead_time_p95::FLOAT),
  change_failure_rate FLOAT AS (VALUE:change_failure_rate::FLOAT),
  mttr_minutes FLOAT AS (VALUE:mttr_minutes::FLOAT)
)
PARTITION BY (dt)
LOCATION = @gitpulse_processed_stage/dora_metrics/
AUTO_REFRESH = TRUE
PATTERN = '.*[.]parquet'
FILE_FORMAT = (FORMAT_NAME = 'GITPULSE_DB.PROCESSED.GITPULSE_PARQUET_FORMAT');

-- ------------------------------------------------------------
-- 5. Load the existing backlog ONCE (auto-refresh only sees new S3 events),
--    then get the SQS ARN for Terraform.
-- ------------------------------------------------------------
ALTER EXTERNAL TABLE commits REFRESH;
ALTER EXTERNAL TABLE issues REFRESH;
ALTER EXTERNAL TABLE pull_requests REFRESH;
ALTER EXTERNAL TABLE releases REFRESH;
ALTER EXTERNAL TABLE contributors REFRESH;
ALTER EXTERNAL TABLE deployments REFRESH;
ALTER EXTERNAL TABLE workflow_runs REFRESH;
ALTER EXTERNAL TABLE repository REFRESH;
ALTER EXTERNAL TABLE dora_metrics REFRESH;

-- Copy the notification_channel value into terraform.tfvars as
--   snowflake_notification_channel_arn = "arn:aws:sqs:..."   then terraform apply again
SHOW EXTERNAL TABLES IN SCHEMA GITPULSE_DB.PROCESSED;

-- ------------------------------------------------------------
-- 6. Analyst access
-- ------------------------------------------------------------
CREATE ROLE IF NOT EXISTS GITPULSE_ANALYST;
GRANT USAGE ON WAREHOUSE GITPULSE_WH TO ROLE GITPULSE_ANALYST;
GRANT USAGE ON DATABASE GITPULSE_DB TO ROLE GITPULSE_ANALYST;
GRANT USAGE ON SCHEMA GITPULSE_DB.PROCESSED TO ROLE GITPULSE_ANALYST;
GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA GITPULSE_DB.PROCESSED TO ROLE GITPULSE_ANALYST;
GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA GITPULSE_DB.PROCESSED TO ROLE GITPULSE_ANALYST;

-- ------------------------------------------------------------
-- 7. Sanity checks
-- ------------------------------------------------------------
SELECT $1 FROM @gitpulse_processed_stage/commits/ LIMIT 3;   -- proves read + KMS decrypt
SELECT dt, commit_author_date FROM commits LIMIT 5;          -- if NULL / 1970: timestamp expression needs adjusting
SELECT 'commits' AS t, COUNT(*) FROM commits
UNION ALL SELECT 'dora_metrics', COUNT(*) FROM dora_metrics;

-- ------------------------------------------------------------
-- OPTIONAL: continuous daily date spine for dora_metrics, built at query time.
-- Use this if the Glue job stays untouched (the table itself is sparse: days
-- with no deployments have no row). Query dora_metrics_daily instead.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW dora_metrics_daily AS
WITH bounds AS (
  SELECT repo, environment, MIN(dt) AS min_dt, MAX(dt) AS max_dt
  FROM dora_metrics
  GROUP BY repo, environment
),
spine AS (
  SELECT b.repo, b.environment, DATEADD('day', n.seq, b.min_dt) AS dt
  FROM bounds b
  JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 3660))) n
    ON DATEADD('day', n.seq, b.min_dt) <= b.max_dt
)
SELECT sp.dt, sp.repo, sp.environment,
       COALESCE(m.deploy_count, 0) AS deploy_count,
       m.lead_time_p50, m.lead_time_p95, m.change_failure_rate, m.mttr_minutes
FROM spine sp
LEFT JOIN dora_metrics m
  ON m.dt = sp.dt AND m.repo = sp.repo AND m.environment = sp.environment;
