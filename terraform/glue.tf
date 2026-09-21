locals {
  # FR-1 (revised, PRD §11.3): 8 resource types, including DORA additions.
  github_resource_types = [
    "repository", "commits", "issues", "pull_requests", "releases",
    "contributors", "deployments", "workflow_runs",
  ]
}

# Two separate databases — raw and processed tables must NEVER share a
# (database, table_name) pair, since a Glue table's identity is
# (database, name), not tied to its S3 Location. Sharing one database
# would let the raw and processed crawlers overwrite each other's
# schema/location on every alternating run.

resource "aws_glue_catalog_database" "gitpulse_raw" {
  name = "${local.glue_database_name}_raw"
}

resource "aws_glue_catalog_database" "gitpulse_processed" {
  name = "${local.glue_database_name}_processed"
}

resource "aws_glue_crawler" "raw" {
  name          = local.glue_raw_crawler_name
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.gitpulse_raw.name

  dynamic "s3_target" {
    for_each = local.github_resource_types
    content {
      path = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/github/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  # Grouping.TableGroupingPolicy intentionally omitted — it opts INTO
  # cross-target schema merging, which risks collapsing structurally
  # similar resource types (e.g. issues + pull_requests) into one table.
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Name = "GitPulse Raw Crawler" }
}

# Glue Python Shell transform job (FR-13/14/15, DORA FR-29 co-deliverable)

resource "aws_s3_object" "transform_script" {
  bucket = aws_s3_bucket.this["raw"].id
  key    = "scripts/transform_and_dora.py"
  source = "${path.module}/glue_scripts/transform_and_dora.py"
  etag   = filemd5("${path.module}/glue_scripts/transform_and_dora.py")
}

resource "aws_glue_job" "transform" {
  name     = local.glue_job_name
  role_arn = aws_iam_role.glue_job.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.this["raw"].bucket}/scripts/transform_and_dora.py"
    python_version  = "3.9"
  }

  max_capacity = 1
  glue_version = "3.0"
  timeout      = 30

  default_arguments = {
    "--library-set"                      = "analytics"
    "--RAW_BUCKET"                       = aws_s3_bucket.this["raw"].bucket
    "--PROCESSED_BUCKET"                 = aws_s3_bucket.this["processed"].bucket
    "--REPOS"                            = join(",", var.github_repos)
    "--DORA_FAILURE_LABELS"              = join(",", var.dora_failure_labels)
    "--DORA_REVERT_PREFIXES"             = join(",", var.dora_revert_prefixes)
    "--DORA_PROD_ENVIRONMENTS"           = join(",", var.dora_prod_environments)
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = { Name = "GitPulse Transform + DORA Job" }
}

# Processed-zone Crawler (FR-15) — own database, own dedicated IAM role

resource "aws_iam_role" "glue_processed_crawler" {
  name = "${var.project_name}-glue-processed-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "GitPulse Glue Processed Crawler Role" }
}

resource "aws_iam_role_policy" "glue_processed_crawler_logs" {
  name = "${var.project_name}-glue-processed-crawler-logs"
  role = aws_iam_role.glue_processed_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/crawlers:*"
    }]
  })
}

resource "aws_iam_role_policy" "glue_processed_crawler_s3" {
  name = "${var.project_name}-glue-processed-crawler-s3"
  role = aws_iam_role.glue_processed_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.this["processed"].arn,
        "${aws_s3_bucket.this["processed"].arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "glue_processed_crawler_kms" {
  name = "${var.project_name}-glue-processed-crawler-kms"
  role = aws_iam_role.glue_processed_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = aws_kms_key.data_lake.arn
    }]
  })
}

resource "aws_glue_crawler" "processed" {
  name          = local.glue_proc_crawler_name
  role          = aws_iam_role.glue_processed_crawler.arn
  database_name = aws_glue_catalog_database.gitpulse_processed.name

  dynamic "s3_target" {
    for_each = concat(local.github_resource_types, ["dora_metrics"])
    content {
      path = "s3://${aws_s3_bucket.this["processed"].bucket}/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Name = "GitPulse Processed Crawler" }
}
