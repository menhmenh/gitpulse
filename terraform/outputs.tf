output "data_lake_bucket_name" {
  description = "GitPulse S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "GitPulse S3 data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "github_ingestion_lambda_role_arn" {
  description = "IAM role ARN for GitHub ingestion Lambda"
  value       = aws_iam_role.github_ingestion_lambda.arn
}