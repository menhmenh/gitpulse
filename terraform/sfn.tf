############################################
# Shared SNS topic — also intended for reuse by the M5 DLQ-depth alarm,
# so failure notifications from both the ingestion path and this transform
# tail land in one place.
############################################

resource "aws_sns_topic" "pipeline_alerts" {
  name              = "${var.project_name}-pipeline-alerts"
  kms_master_key_id = aws_kms_key.data_lake.id

  tags = { Name = "GitPulse Pipeline Alerts" }
}

resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn = aws_sns_topic.pipeline_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowAccountAccess"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action = [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish"
        ]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
      {
        Sid       = "AllowCloudWatchAlarmsPublish"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.pipeline_alerts.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = local.account_id
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "pipeline_alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

############################################
# SFN execution role — least-priv per ticket spec: exactly the 4 Glue
# actions + sns:Publish on the one topic, nothing else.
############################################

resource "aws_iam_role" "sfn_transform_tail" {
  name = "${var.project_name}-sfn-transform-tail-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse SFN Transform Tail Role" }
}

resource "aws_iam_role_policy" "sfn_transform_tail_glue" {
  name = "${var.project_name}-sfn-transform-tail-glue"
  role = aws_iam_role.sfn_transform_tail.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartCrawler", "glue:GetCrawler"]
        Resource = [aws_glue_crawler.raw.arn, aws_glue_crawler.processed.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun"]
        Resource = aws_glue_job.transform.arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_transform_tail_sns" {
  name = "${var.project_name}-sfn-transform-tail-sns"
  role = aws_iam_role.sfn_transform_tail.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sns:Publish"]
      Resource = aws_sns_topic.pipeline_alerts.arn
    }]
  })
}

############################################
# State machine
#
# NOTE on `glue:startCrawler.sync`: this integration does not exist — Glue
# Crawlers have no native "run to completion" Step Functions integration
# (unlike Glue Jobs, which genuinely support `glue:startJobRun.sync`).
# CrawlRaw/CrawlProcessed below implement the standard poll-loop pattern
# (Start -> Wait -> GetCrawler -> Choice) to achieve the same synchronous
# wait-for-completion behavior the ticket intends.
############################################

resource "aws_sfn_state_machine" "transform_tail" {
  name     = "${var.project_name}-transform-tail"
  role_arn = aws_iam_role.sfn_transform_tail.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "GitPulse transform tail: crawl raw -> transform job -> crawl processed"
    StartAt = "CrawlRaw"
    States = {

      # --- Stage 1: CrawlRaw (poll-loop, see note above) ---
      CrawlRaw = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = { Name = aws_glue_crawler.raw.name }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyCrawlRawFailed"
        }]
        Next = "WaitRaw"
      }
      WaitRaw = { Type = "Wait", Seconds = 30, Next = "GetCrawlRaw" }
      GetCrawlRaw = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = { Name = aws_glue_crawler.raw.name }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyCrawlRawFailed"
        }]
        Next = "CheckRawRunning"
      }
      CheckRawRunning = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.State"
          StringEquals = "READY"
          Next         = "CheckRawResult"
        }]
        Default = "WaitRaw"
      }
      CheckRawResult = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.LastCrawl.Status"
          StringEquals = "SUCCEEDED"
          Next         = "TransformGlueJob"
        }]
        Default = "NotifyCrawlRawFailed"
      }
      NotifyCrawlRawFailed = {
        Type       = "Pass"
        Parameters = { FailedStage = "CrawlRaw" }
        ResultPath = "$.failure"
        Next       = "FailNotify"
      }

      # --- Stage 2: TransformGlueJob (genuine .sync integration) ---
      TransformGlueJob = {
        Type       = "Task"
        Resource   = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.transform.name }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyTransformFailed"
        }]
        Next = "CrawlProcessed"
      }
      NotifyTransformFailed = {
        Type       = "Pass"
        Parameters = { FailedStage = "TransformGlueJob" }
        ResultPath = "$.failure"
        Next       = "FailNotify"
      }

      # --- Stage 3: CrawlProcessed (same poll-loop pattern as CrawlRaw) ---
      CrawlProcessed = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = { Name = aws_glue_crawler.processed.name }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyCrawlProcessedFailed"
        }]
        Next = "WaitProcessed"
      }
      WaitProcessed = { Type = "Wait", Seconds = 30, Next = "GetCrawlProcessed" }
      GetCrawlProcessed = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = { Name = aws_glue_crawler.processed.name }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyCrawlProcessedFailed"
        }]
        Next = "CheckProcessedRunning"
      }
      CheckProcessedRunning = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.State"
          StringEquals = "READY"
          Next         = "CheckProcessedResult"
        }]
        Default = "WaitProcessed"
      }
      CheckProcessedResult = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.LastCrawl.Status"
          StringEquals = "SUCCEEDED"
          Next         = "PipelineSucceeded"
        }]
        Default = "NotifyCrawlProcessedFailed"
      }
      NotifyCrawlProcessedFailed = {
        Type       = "Pass"
        Parameters = { FailedStage = "CrawlProcessed" }
        ResultPath = "$.failure"
        Next       = "FailNotify"
      }

      PipelineSucceeded = { Type = "Succeed" }

      # --- Shared failure notifier: every Catch/Choice-failure path lands here ---
      FailNotify = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('GitPulse transform tail failed at stage: {}. Execution: {}', $.failure.FailedStage, $$.Execution.Id)"
          Subject     = "GitPulse transform tail FAILED"
        }
        Next = "PipelineFailed"
      }
      PipelineFailed = { Type = "Fail", Error = "TransformTailFailed" }
    }
  })

  tags = { Name = "GitPulse Transform Tail State Machine" }
}

resource "aws_iam_role_policy" "sfn_transform_tail_kms" {
  name = "${var.project_name}-sfn-transform-tail-kms"
  role = aws_iam_role.sfn_transform_tail.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}