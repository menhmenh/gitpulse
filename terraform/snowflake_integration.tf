variable "snowflake_storage_aws_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from `DESC INTEGRATION gitpulse_s3_integration` (Snowflake's IAM user)"
  type        = string
}

variable "snowflake_storage_aws_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from `DESC INTEGRATION gitpulse_s3_integration`"
  type        = string
  sensitive   = true
}

variable "snowflake_notification_channel_arn" {
  description = "notification_channel from `SHOW EXTERNAL TABLES` (Snowflake-managed SQS ARN). Leave empty until the external tables exist; setting it enables S3 -> Snowflake auto-refresh."
  type        = string
  default     = ""
}

locals {
  # The 9 processed table prefixes: 8 GitHub resources + dora_metrics
  snowflake_table_prefixes = concat(local.github_resource_types, ["dora_metrics"])
}

resource "aws_iam_role" "snowflake_storage_integration" {
  name = "${var.project_name}-snowflake-integration-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_storage_aws_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = var.snowflake_storage_aws_external_id }
      }
    }]
  })

  tags = { Name = "GitPulse Snowflake Integration Role" }
}

resource "aws_iam_role_policy" "snowflake_storage_integration_s3" {
  name = "${var.project_name}-snowflake-integration-s3"
  role = aws_iam_role.snowflake_storage_integration.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Snowflake calls this to resolve the bucket's region
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = aws_s3_bucket.this["processed"].arn
      },
      {
        # "" allows the root-level LIST that `LIST @stage` performs; only the
        # 9 table prefixes exist in this bucket.
        Sid      = "ListTablePrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.this["processed"].arn
        Condition = {
          StringLike = {
            "s3:prefix" = concat([""], [for p in local.snowflake_table_prefixes : "${p}/*"])
          }
        }
      },
      {
        Sid    = "ReadTableObjects"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = [
          for p in local.snowflake_table_prefixes : "${aws_s3_bucket.this["processed"].arn}/${p}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "snowflake_storage_integration_kms" {
  name = "${var.project_name}-snowflake-integration-kms"
  role = aws_iam_role.snowflake_storage_integration.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

# Auto-refresh: S3 events on the processed bucket -> Snowflake-managed SQS.
# Only files created AFTER this exists are picked up - run
# `ALTER EXTERNAL TABLE <t> REFRESH` once for the existing backlog.

resource "aws_s3_bucket_notification" "processed_snowflake_refresh" {
  count  = var.snowflake_notification_channel_arn == "" ? 0 : 1
  bucket = aws_s3_bucket.this["processed"].id

  queue {
    queue_arn = var.snowflake_notification_channel_arn
    events    = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }
}

output "snowflake_storage_integration_role_arn" {
  description = "Use as STORAGE_AWS_ROLE_ARN in the Snowflake storage integration"
  value       = aws_iam_role.snowflake_storage_integration.arn
}
