output "s3_bucket_names" {
  description = "GitPulse S3 bucket names by zone (raw, processed, athena_results)"
  value       = { for k, b in aws_s3_bucket.this : k => b.bucket }
}

output "s3_bucket_arns" {
  description = "GitPulse S3 bucket ARNs by zone (raw, processed, athena_results)"
  value       = { for k, b in aws_s3_bucket.this : k => b.arn }
}

output "github_ingestion_lambda_role_arn" {
  description = "IAM role ARN for GitHub ingestion Lambda"
  value       = aws_iam_role.github_ingestion_lambda.arn
}