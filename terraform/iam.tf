resource "aws_iam_role" "github_ingestion_lambda" {
  name = "${var.project_name}-github-ingestion-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "lambda.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "GitPulse GitHub Ingestion Lambda Role"
  }
}

resource "aws_iam_role_policy" "github_ingestion_lambda_logs" {
  name = "${var.project_name}-lambda-logs"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]

        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_s3" {
  name = "${var.project_name}-lambda-s3"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "s3:PutObject"
        ]

        Resource = "${aws_s3_bucket.this["raw"].arn}/raw/github/*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "github_ingestion_lambda_kms" {
  name = "${var.project_name}-lambda-kms"
  role = aws_iam_role.github_ingestion_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "kms:GenerateDataKey",
          "kms:Decrypt"
        ]

        Resource = aws_kms_key.data_lake.arn
      }
    ]
  })
}