############################################
# Dead Letter Queue
############################################

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq"
  message_retention_seconds = 1209600 # 14 days — max, gives time to investigate failures

  kms_master_key_id                 = aws_kms_key.data_lake.id
  kms_data_key_reuse_period_seconds = 300

  tags = { Name = "GitPulse DLQ" }
}

# Restrict the DLQ so only the job queue below can redrive into it
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.job_queue.arn]
  })
}

############################################
# Job Queue (sits in front of the Ingestion Lambda)
############################################

resource "aws_sqs_queue" "job_queue" {
  name = local.job_queue_name

  # Must exceed the Ingestion Lambda's timeout (60s, see lambda.tf) so a message
  # can't reappear and be double-processed while the Lambda is still running.
  visibility_timeout_seconds = 90

  message_retention_seconds = 345600 # 4 days

  kms_master_key_id                 = aws_kms_key.data_lake.id
  kms_data_key_reuse_period_seconds = 300

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = { Name = "GitPulse Job Queue" }
}

############################################
# Event source mapping: SQS -> Ingestion Lambda
# Concurrency capped to protect GitHub's per-token rate limit (NFR-4)
############################################

resource "aws_lambda_event_source_mapping" "ingestion" {
  event_source_arn = aws_sqs_queue.job_queue.arn
  function_name    = aws_lambda_function.github_ingestion.arn
  batch_size       = 1 # 1 invocation = 1 resource for 1 repo (FR-5)
  enabled          = true

  scaling_config {
    maximum_concurrency = 5 # cap — tune based on GitHub's 5,000 req/hr token limit
  }
}
