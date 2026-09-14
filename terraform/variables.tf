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
  default     = ["apache/airflow"]
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
