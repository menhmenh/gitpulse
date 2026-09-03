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