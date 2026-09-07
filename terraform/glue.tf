locals {
  github_resource_types = ["repository", "commits", "issues", "pull_requests", "releases", "contributors"]
}

resource "aws_glue_catalog_database" "gitpulse" {
  name = local.glue_database_name
}

resource "aws_glue_crawler" "raw" {
  name          = local.glue_raw_crawler_name
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.gitpulse.name

  # One explicit target per resource type — avoids Glue's schema-grouping
  # heuristic merging distinct resource types into a single table, which
  # happened when a single "raw/github/" root was crawled.
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

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Name = "GitPulse Raw Crawler" }
}

############################################
# Delay-based trigger: runs after ingestion is expected to complete.
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
