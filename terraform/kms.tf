resource "aws_kms_key" "data_lake" {
  description             = "KMS key for GitPulse S3 data lake"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "GitPulse Data Lake KMS Key"
  }
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/gitpulse-data-lake"
  target_key_id = aws_kms_key.data_lake.key_id
}