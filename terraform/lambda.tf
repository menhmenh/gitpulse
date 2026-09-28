locals {
  ingestion_timeout_seconds = 180
}

data "archive_file" "producer" {
  type        = "zip"
  source_dir  = "${path.module}/producer_src"
  output_path = "${path.module}/.build/producer.zip"
}

resource "aws_lambda_function" "producer" {
  function_name = local.producer_lambda_name
  role          = aws_iam_role.producer_lambda.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.producer.output_path
  source_code_hash = data.archive_file.producer.output_base64sha256

  environment {
    variables = {
      JOB_QUEUE_URL  = aws_sqs_queue.job_queue.url
      GITHUB_REPOS   = join(",", var.github_repos)
      LOOKBACK_HOURS = "24"
    }
  }

  tags = { Name = "GitPulse Producer Lambda" }
}

data "archive_file" "github_ingestion" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src"
  output_path = "${path.module}/.build/github_ingestion.zip"
}

resource "aws_lambda_function" "github_ingestion" {
  function_name = local.ingestion_lambda_name
  role          = aws_iam_role.github_ingestion_lambda.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  timeout       = local.ingestion_timeout_seconds
  memory_size   = 256

  # Increase queue visibility before increasing Lambda timeout on existing deployments.
  depends_on = [aws_sqs_queue.job_queue]

  filename         = data.archive_file.github_ingestion.output_path
  source_code_hash = data.archive_file.github_ingestion.output_base64sha256

  environment {
    variables = {
      GITHUB_SECRET_ARN = aws_secretsmanager_secret.github_token.arn
      RAW_BUCKET        = aws_s3_bucket.this["raw"].bucket
    }
  }

  tags = { Name = "GitPulse GitHub Ingestion Lambda" }
}
