resource "aws_athena_workgroup" "gitpulse" {
  name  = local.athena_workgroup_name
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena_results"].bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.data_lake.arn
      }
    }
  }

  tags = { Name = "GitPulse Athena Workgroup" }
}
