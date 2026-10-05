locals {
  s3_buckets = {
    raw = {
      name = "gitpulse-raw-${var.environment}-${data.aws_caller_identity.current.account_id}"
    }
    processed = {
      name = "gitpulse-processed-${var.environment}-${data.aws_caller_identity.current.account_id}"
    }
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.s3_buckets

  bucket        = each.value.name
  force_destroy = true

  tags = {
    Name    = each.value.name
    Project = "gitpulse"
    Zone    = each.key
  }
}

# Block all public access on every bucket
resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning (protects raw/processed history, supports safe reprocessing)
resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-KMS encryption on every bucket, using the shared customer-managed key
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake.arn
      sse_algorithm     = "aws:kms"
    }

    bucket_key_enabled = true
  }
}

# Deny any unencrypted / non-TLS access (defense-in-depth for FR-25)
resource "aws_s3_bucket_policy" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          each.value.arn,
          "${each.value.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}
