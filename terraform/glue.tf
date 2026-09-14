locals {
  # FR-1 (revised, PRD §11.3): 8 resource types, including DORA additions.
  github_resource_types = [
    "repository", "commits", "issues", "pull_requests", "releases",
    "contributors", "deployments", "workflow_runs",
  ]
}

resource "aws_glue_catalog_database" "gitpulse" {
  name = local.glue_database_name
}

resource "aws_glue_crawler" "raw" {
  name          = local.glue_raw_crawler_name
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.gitpulse.name

  # One explicit target per resource type — table boundaries are already
  # correctly defined by these separate targets, so schema-grouping across
  # targets must stay OFF (see `configuration` below) or Glue can merge
  # structurally-similar resource types (e.g. issues + pull_requests share
  # id/user/state/created_at/updated_at/labels/assignees) into one table.
  dynamic "s3_target" {
    for_each = local.github_resource_types
    content {
      path = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/github/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG" # never auto-delete tables — raw history must be preserved (FR-10)
  }

  # NOTE: Grouping.TableGroupingPolicy = "CombineCompatibleSchemas" was
  # intentionally removed here. It opts INTO cross-target schema merging,
  # which directly contradicts the per-resource-type explicit s3_targets
  # above and risked collapsing distinct resource types into fewer tables.
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Name = "GitPulse Raw Crawler" }
}

############################################
# Delay-based trigger: runs after ingestion is expected to complete.
# KNOWN LIMITATION (flagged in review): Producer uses rate(24 hours) from
# apply time, while this crawler uses a fixed daily cron — they can drift
# out of alignment, risking a crawl mid-ingestion. This entire delay-based
# trigger (schedule + role below) is expected to be REMOVED once the PRD
# §12 Step Functions task lands (drained-SQS-alarm trigger replaces it).
# Do not extend this mechanism further — fix the alignment properly only
# if the SFN task is delayed and this becomes the long-term trigger.
############################################

resource "aws_iam_role" "eventbridge_crawler_trigger" {
  name = "${var.project_name}-eventbridge-crawler-trigger-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse EventBridge Crawler Trigger Role" }
}

resource "aws_iam_role_policy" "eventbridge_crawler_trigger_start" {
  name = "${var.project_name}-eventbridge-start-crawler"
  role = aws_iam_role.eventbridge_crawler_trigger.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartCrawler"]
      Resource = aws_glue_crawler.raw.arn
    }]
  })
}

resource "aws_scheduler_schedule" "raw_crawler" {
  name       = "${var.project_name}-raw-crawler-schedule"
  group_name = "default"

  schedule_expression          = var.crawler_schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startCrawler"
    role_arn = aws_iam_role.eventbridge_crawler_trigger.arn

    input = jsonencode({ Name = aws_glue_crawler.raw.name })

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}
