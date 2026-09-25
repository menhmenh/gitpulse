resource "aws_kms_key" "data_lake" {
  description             = "KMS key for GitPulse S3 data lake"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableIAMUserPermissions"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowCloudWatchAlarmsForSNS"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name = "GitPulse Data Lake KMS Key"
  }
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/gitpulse-data-lake"
  target_key_id = aws_kms_key.data_lake.key_id
}