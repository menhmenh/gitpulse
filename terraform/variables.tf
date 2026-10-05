variable "aws_region" {
  description = "AWS region for GitPulse"
  type        = string
  default     = "ap-southeast-1"
}

variable "aws_profile" {
  description = "AWS CLI profile used by Terraform"
  type        = string
  default     = "gitpulse"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "gitpulse"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "github_repos" {
  description = "Target GitHub repos to ingest, as 'owner/repo' strings"
  type        = list(string)
  default     = ["vercel/next.js"]
}

variable "producer_schedule_expression" {
  description = "EventBridge Scheduler expression for the Producer Lambda. Fixed cron (not rate()) so the trigger time is deterministic and alignable with crawler_schedule_expression below — rate() fires relative to apply-time, which drifts unpredictably across environments/redeploys."
  type        = string
  default     = "cron(0 0 * * ? *)"
}

variable "crawler_schedule_expression" {
  description = "EventBridge Scheduler expression for the raw-zone Glue Crawler. Must be set comfortably after producer_schedule_expression + expected ingestion duration — this is a manually-maintained offset, not computed, since this delay-based trigger is scheduled for removal once the SFN drained-signal task (PRD §12) lands."
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "processed_crawler_schedule_expression" {
  description = "EventBridge Scheduler expression for the processed-zone Glue Crawler. Should run after the transform job (gitpulse-transform-job) is expected to complete — see crawler_schedule_expression comment re: temporary delay-based mechanism pending the SFN task (PRD §12)."
  type        = string
  default     = "cron(0 3 * * ? *)"
}

variable "dora_failure_labels" {
  description = "FR-31 failure-signal proxy: issue labels (or substrings) treated as an incident/failure"
  type        = list(string)
  # Extended default labels to match common GitHub prefix conventions (e.g. template: bug, type: bug, kind/bug)
  default = ["bug", "incident", "hotfix", "template: bug", "type: bug", "kind/bug"]
}

variable "dora_revert_prefixes" {
  description = "FR-31 failure-signal proxy: merged PR title prefixes treated as a revert/hotfix"
  type        = list(string)
  default     = ["revert", "hotfix:"]
}

variable "dora_prod_environments" {
  description = "Deployment 'environment' values treated as production for DORA metrics"
  type        = list(string)
  default     = ["production", "prod", "release-canary"]
}

variable "sqs_drained_sustained_minutes" {
  description = "Number of sustained minutes SQS queue messages (visible + not visible) must remain 0 before alarm transitions to OK (PRD FR-33, Architecture §12.3)"
  type        = number
  default     = 5
}

variable "enable_transform_tail_fallback" {
  description = "Enable time-delayed EventBridge rule fallback for transform tail (Architecture §12.3 / PRD FR-33). Disabled by default in favor of primary SQS drained alarm trigger."
  type        = bool
  default     = false
}

variable "transform_tail_fallback_schedule_expression" {
  description = "Fallback schedule expression for the transform tail (producer_time + 45min). Used only when enable_transform_tail_fallback is true."
  type        = string
  default     = "cron(45 0 * * ? *)"
}

variable "alert_email" {
  description = "Email address subscribed to the shared pipeline-alerts SNS topic (receives DLQ alarm and Step Functions failure notifications). Set to empty string to skip subscription."
  type        = string
  default     = "alerts@example.com"
}


