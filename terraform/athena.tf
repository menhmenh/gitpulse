resource "aws_athena_workgroup" "gitpulse" {
  name          = local.athena_workgroup_name
  state         = "ENABLED"
  force_destroy = true 

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff_per_query

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
