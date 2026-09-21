resource "aws_cloudwatch_log_group" "glue_job_error" {
  name              = "/aws-glue/python-jobs/error"
  retention_in_days = 30
  tags              = { Name = "GitPulse Glue Job Error Logs" }
}

resource "aws_cloudwatch_log_group" "glue_crawlers" {
  name              = "/aws-glue/crawlers"
  retention_in_days = 30
  tags              = { Name = "GitPulse Glue Crawler Logs" }
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${var.project_name}-dlq-depth"
  alarm_description   = "Fires when any message lands in the DLQ (maxReceiveCount exceeded)"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse DLQ Depth Alarm" }
}

resource "aws_cloudwatch_metric_alarm" "producer_lambda_errors" {
  alarm_name          = "${var.project_name}-producer-lambda-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.producer.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Producer Lambda Errors" }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_errors" {
  alarm_name          = "${var.project_name}-ingestion-lambda-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.github_ingestion.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Ingestion Lambda Errors" }
}

resource "aws_cloudwatch_metric_alarm" "producer_lambda_duration" {
  alarm_name          = "${var.project_name}-producer-lambda-duration"
  alarm_description   = "Producer approaching its 30s timeout (threshold = 80%)"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  dimensions          = { FunctionName = aws_lambda_function.producer.function_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 24000 # 80% of 30,000ms
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Producer Lambda Duration" }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_duration" {
  alarm_name          = "${var.project_name}-ingestion-lambda-duration"
  alarm_description   = "Ingestion approaching its 60s timeout (threshold = 80%)"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  dimensions          = { FunctionName = aws_lambda_function.github_ingestion.function_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 48000 # 80% of 60,000ms
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Ingestion Lambda Duration" }
}

resource "aws_cloudwatch_log_metric_filter" "glue_job_errors" {
  name           = "${var.project_name}-glue-job-error-filter"
  log_group_name = aws_cloudwatch_log_group.glue_job_error.name
  pattern        = "ERROR"

  metric_transformation {
    name          = "TransformJobErrorCount"
    namespace     = "GitPulse/Glue"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${var.project_name}-glue-job-failure"
  namespace           = "GitPulse/Glue"
  metric_name         = "TransformJobErrorCount"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Glue Transform Job Failure" }
}

resource "aws_cloudwatch_log_metric_filter" "glue_crawler_errors" {
  name           = "${var.project_name}-glue-crawler-error-filter"
  log_group_name = aws_cloudwatch_log_group.glue_crawlers.name
  pattern        = "ERROR"

  metric_transformation {
    name          = "CrawlerErrorCount"
    namespace     = "GitPulse/Glue"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_crawler_failure" {
  alarm_name          = "${var.project_name}-glue-crawler-failure"
  namespace           = "GitPulse/Glue"
  metric_name         = "CrawlerErrorCount"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse Glue Crawler Failure" }
}

resource "aws_cloudwatch_metric_alarm" "sfn_transform_tail_failure" {
  alarm_name          = "${var.project_name}-sfn-transform-tail-failure"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.transform_tail.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = { Name = "GitPulse SFN Transform Tail Failure" }
}

resource "aws_cloudwatch_metric_alarm" "sqs_drained" {
  alarm_name          = "${var.project_name}-job-queue-drained"
  alarm_description   = "Job queue is empty (0 visible messages) — candidate trigger signal for the transform tail. NOT WIRED YET: see the SQS-drained-alarm follow-up ticket for the actual EventBridge -> SFN integration."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.job_queue.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2 # requires 2 consecutive empty checks to avoid flapping mid-drain
  threshold           = 0
  comparison_operator = "LessThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  # alarm_actions intentionally omitted 

  tags = { Name = "GitPulse Job Queue Drained - stub unwired" }
}
