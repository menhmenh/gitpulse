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
  default     = ["bug", "incident", "hotfix", "template: bug", "type: bug", "kind/bug"]
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

variable "athena_bytes_scanned_cutoff_per_query" {
  description = "Max bytes a single Athena query can scan before being cancelled — cost-control guardrail against unoptimized/unpartitioned queries"
  type        = number
  default     = 10737418240 # 10 GB
}
