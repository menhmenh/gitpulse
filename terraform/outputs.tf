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

output "producer_lambda_role_arn" {
  description = "IAM role ARN for Producer Lambda"
  value       = aws_iam_role.producer_lambda.arn
}

output "glue_crawler_role_arn" {
  description = "IAM role ARN for Glue Crawler"
  value       = aws_iam_role.glue_crawler.arn
}

output "glue_job_role_arn" {
  description = "IAM role ARN for Glue Python Shell job"
  value       = aws_iam_role.glue_job.arn
}

output "eventbridge_scheduler_role_arn" {
  description = "IAM role ARN for EventBridge Scheduler"
  value       = aws_iam_role.eventbridge_scheduler.arn
}

output "athena_analyst_role_arn" {
  description = "IAM role ARN for Athena analyst access"
  value       = aws_iam_role.athena_analyst.arn
}