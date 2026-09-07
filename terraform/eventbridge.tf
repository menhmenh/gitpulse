resource "aws_scheduler_schedule" "producer" {
  name       = "${var.project_name}-producer-schedule"
  group_name = "default"

  # Configurable, not hardcoded see variables.tf (default: every 24h)
  schedule_expression          = var.producer_schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF" # fires at the exact scheduled time
  }

  target {
    arn      = aws_lambda_function.producer.arn
    role_arn = aws_iam_role.eventbridge_scheduler.arn

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}
