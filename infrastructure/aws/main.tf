terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (production, staging, development)"
  type        = string
}

variable "db_host" {
  description = "Aurora PostgreSQL cluster endpoint"
  type        = string
}

variable "db_password_write" {
  description = "Database password for write access (graph_admin)"
  type        = string
  sensitive   = true
}

variable "db_password_read" {
  description = "Database password for read access (hyperdrive_reader)"
  type        = string
  sensitive   = true
}

# Outputs
output "cloudwatch_access_key_id" {
  description = "CloudWatch logger access key ID"
  value       = aws_iam_access_key.cloudwatch_logger.id
}

output "cloudwatch_secret_access_key" {
  description = "CloudWatch logger secret access key"
  value       = aws_iam_access_key.cloudwatch_logger.secret
  sensitive   = true
}

output "database_write_secret_arn" {
  description = "ARN of database write credentials secret"
  value       = aws_secretsmanager_secret.database_write.arn
}

output "database_read_secret_arn" {
  description = "ARN of database read credentials secret"
  value       = aws_secretsmanager_secret.database_read.arn
}

output "dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.distributed_loaders.dashboard_name}"
}
