locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region

  producer_lambda_name   = "${var.project_name}-producer"
  ingestion_lambda_name  = "${var.project_name}-github-ingestion"
  job_queue_name         = "${var.project_name}-job-queue"
  github_secret_prefix   = "${var.project_name}/github-token"
  glue_database_name     = var.project_name
  glue_raw_crawler_name  = "${var.project_name}-raw-crawler"
  glue_proc_crawler_name = "${var.project_name}-processed-crawler"
  glue_job_name          = "${var.project_name}-transform-job"
  athena_workgroup_name  = "${var.project_name}-workgroup"

  sqs_job_queue_arn = "arn:aws:sqs:${local.region}:${local.account_id}:${local.job_queue_name}"
}
# 1. Producer Lambda role
# Enumerates work, enqueues SQS messages only, no S3/API access.


resource "aws_iam_role" "producer_lambda" {
  name = "${var.project_name}-producer-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse Producer Lambda Role" }
}

resource "aws_iam_role_policy" "producer_lambda_logs" {
  name = "${var.project_name}-producer-lambda-logs"
  role = aws_iam_role.producer_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.producer_lambda_name}:*"
    }]
  })
}

resource "aws_iam_role_policy" "producer_lambda_sqs" {
  name = "${var.project_name}-producer-lambda-sqs"
  role = aws_iam_role.producer_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = local.sqs_job_queue_arn
    }]
  })
}

resource "aws_iam_role_policy" "producer_lambda_kms" {
  name = "${var.project_name}-producer-lambda-kms"
  role = aws_iam_role.producer_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:GenerateDataKey"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

# 2. Ingestion Lambda role
# SQS-triggered; writes raw JSON to S3; reads GitHub token from Secrets Manager.

resource "aws_iam_role" "github_ingestion_lambda" {
  name = "${var.project_name}-github-ingestion-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse GitHub Ingestion Lambda Role" }
}

resource "aws_iam_role_policy" "github_ingestion_lambda_logs" {
  name = "${var.project_name}-lambda-logs"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.ingestion_lambda_name}:*"
    }]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_s3" {
  name = "${var.project_name}-lambda-s3"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:DeleteObject"] # DeleteObject needed for rollback on partial-write failure
      Resource = "${aws_s3_bucket.this["raw"].arn}/raw/github/*"
    }]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_kms" {
  name = "${var.project_name}-lambda-kms"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:GenerateDataKey", "kms:Decrypt"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_sqs" {
  name = "${var.project_name}-lambda-sqs"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]
      Resource = local.sqs_job_queue_arn
    }]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_secrets" {
  name = "${var.project_name}-lambda-secrets"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${local.github_secret_prefix}-*"
    }]
  })
}

# 3. Glue Crawler role
# Reads raw & processed zones, registers schema in the Data Catalog only.

resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse Glue Crawler Role" }
}

resource "aws_iam_role_policy" "glue_crawler_logs" {
  name = "${var.project_name}-glue-crawler-logs"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/crawlers:*"
    }]
  })
}


resource "aws_iam_role_policy" "glue_crawler_s3" {
  name = "${var.project_name}-glue-crawler-s3"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.this["raw"].arn,
        "${aws_s3_bucket.this["raw"].arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "glue_crawler_kms" {
  name = "${var.project_name}-glue-crawler-kms"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

resource "aws_iam_role_policy" "glue_crawler_catalog" {
  name = "${var.project_name}-glue-crawler-catalog"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:CreateDatabase",
        "glue:GetTable", "glue:CreateTable", "glue:UpdateTable",
        "glue:BatchCreatePartition", "glue:GetPartitions", "glue:BatchGetPartition"
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database_name}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database_name}/*"
      ]
    }]
  })
}


# 4. Glue Python Shell job role
# Reads raw, writes processed (Parquet), updates catalog.

resource "aws_iam_role" "glue_job" {
  name = "${var.project_name}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse Glue Python Shell Job Role" }
}

resource "aws_iam_role_policy" "glue_job_logs" {
  name = "${var.project_name}-glue-job-logs"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/python-jobs/*:*"
    }]
  })
}

resource "aws_iam_role_policy" "glue_job_s3" {
  name = "${var.project_name}-glue-job-s3"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.this["raw"].arn, "${aws_s3_bucket.this["raw"].arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.this["processed"].arn, "${aws_s3_bucket.this["processed"].arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_job_kms" {
  name = "${var.project_name}-glue-job-kms"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

resource "aws_iam_role_policy" "glue_job_catalog" {
  name = "${var.project_name}-glue-job-catalog"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetTable", "glue:GetDatabase", "glue:GetPartitions",
        "glue:BatchCreatePartition", "glue:UpdateTable"
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database_name}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database_name}/*"
      ]
    }]
  })
}

# 5. EventBridge Scheduler role
# Only allowed to invoke the Producer Lambda on schedule.

resource "aws_iam_role" "eventbridge_scheduler" {
  name = "${var.project_name}-eventbridge-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse EventBridge Scheduler Role" }
}

resource "aws_iam_role_policy" "eventbridge_scheduler_invoke" {
  name = "${var.project_name}-eventbridge-invoke-producer"
  role = aws_iam_role.eventbridge_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.producer_lambda_name}"
    }]
  })
}

# 6. Athena analyst role
# Assumed by Data Analysts to run queries against the processed zone.

resource "aws_iam_role" "athena_analyst" {
  name = "${var.project_name}-athena-analyst-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse Athena Analyst Role" }
}

resource "aws_iam_role_policy" "athena_analyst_query" {
  name = "${var.project_name}-athena-query"
  role = aws_iam_role.athena_analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "athena:StartQueryExecution", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ]
      Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/${local.athena_workgroup_name}"
    }]
  })
}

resource "aws_iam_role_policy" "athena_analyst_catalog" {
  name = "${var.project_name}-athena-catalog"
  role = aws_iam_role.athena_analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database_name}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database_name}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "athena_analyst_s3" {
  name = "${var.project_name}-athena-s3"
  role = aws_iam_role.athena_analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.this["processed"].arn, "${aws_s3_bucket.this["processed"].arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.this["athena_results"].arn, "${aws_s3_bucket.this["athena_results"].arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_analyst_kms" {
  name = "${var.project_name}-athena-kms"
  role = aws_iam_role.athena_analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}
