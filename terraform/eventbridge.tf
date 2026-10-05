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

# Primary: SQS Drained Trigger for Transform Tail (PRD FR-33, Architecture §12.3)

resource "aws_cloudwatch_event_rule" "sqs_drained_to_sfn" {
  name        = "${var.project_name}-sqs-drained-to-sfn"
  description = "Trigger SFN transform tail when SQS job queue drained alarm transitions to OK"

  event_pattern = jsonencode({
    source      = ["aws.cloudwatch"]
    detail-type = ["CloudWatch Alarm State Change"]
    resources   = [aws_cloudwatch_metric_alarm.sqs_drained.arn]
    detail = {
      state = {
        value = ["OK"]
      }
    }
  })

  tags = { Name = "GitPulse SQS Drained to SFN Rule" }
}

resource "aws_cloudwatch_event_target" "sqs_drained_to_sfn" {
  rule      = aws_cloudwatch_event_rule.sqs_drained_to_sfn.name
  target_id = "TransformTailStateMachine"
  arn       = aws_sfn_state_machine.transform_tail.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

# Fallback: Time-delayed Trigger for Transform Tail (producer_time + 45min, variable-gated)

resource "aws_cloudwatch_event_rule" "time_delay_to_sfn" {
  name                = "${var.project_name}-time-delay-to-sfn"
  description         = "Fallback time-delayed trigger for SFN transform tail (producer_time + 45min, Architecture §12.3). Disabled by default; primary trigger is sqs_drained alarm."
  schedule_expression = var.transform_tail_fallback_schedule_expression
  is_enabled          = var.enable_transform_tail_fallback

  tags = { Name = "GitPulse Time Delay Fallback to SFN Rule" }
}

resource "aws_cloudwatch_event_target" "time_delay_to_sfn" {
  rule      = aws_cloudwatch_event_rule.time_delay_to_sfn.name
  target_id = "TransformTailStateMachineFallback"
  arn       = aws_sfn_state_machine.transform_tail.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
  input     = jsonencode({ trigger = "time_delay_fallback" })
}

