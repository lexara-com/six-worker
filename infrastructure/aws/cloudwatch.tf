# =============================================
# AWS CloudWatch Configuration
# For Distributed Loader System Logging
# =============================================

# =============================================
# Log Group for Distributed Loaders
# =============================================

resource "aws_cloudwatch_log_group" "distributed_loaders" {
  name              = "/lexara/distributed-loaders"
  retention_in_days = 30 # Retain logs for 30 days

  tags = {
    Environment = var.environment
    Purpose     = "distributed-loader-logging"
    System      = "lexara-six-worker"
  }
}

# =============================================
# Log Streams (created automatically by workers)
# =============================================
# Workers will create their own log streams:
# - coordinator-{environment}
# - worker-{worker_id}
# - queue-consumer-{environment}

# =============================================
# CloudWatch Metric Filters
# =============================================

# Track error rate
resource "aws_cloudwatch_log_metric_filter" "error_rate" {
  name           = "loader-error-rate"
  log_group_name = aws_cloudwatch_log_group.distributed_loaders.name
  pattern        = "{ $.level = \"ERROR\" }"

  metric_transformation {
    name      = "LoaderErrors"
    namespace = "Lexara/DistributedLoaders"
    value     = "1"
    unit      = "Count"
  }
}

# Track records processed
resource "aws_cloudwatch_log_metric_filter" "records_processed" {
  name           = "records-processed"
  log_group_name = aws_cloudwatch_log_group.distributed_loaders.name
  pattern        = "{ $.metadata.records_processed = * }"

  metric_transformation {
    name      = "RecordsProcessed"
    namespace = "Lexara/DistributedLoaders"
    value     = "$.metadata.records_processed"
    unit      = "Count"
  }
}

# Track processing velocity
resource "aws_cloudwatch_log_metric_filter" "processing_velocity" {
  name           = "processing-velocity"
  log_group_name = aws_cloudwatch_log_group.distributed_loaders.name
  pattern        = "{ $.metadata.velocity = * }"

  metric_transformation {
    name      = "ProcessingVelocity"
    namespace = "Lexara/DistributedLoaders"
    value     = "$.metadata.velocity"
    unit      = "Count/Minute"
  }
}

# Track job completions
resource "aws_cloudwatch_log_metric_filter" "job_completions" {
  name           = "job-completions"
  log_group_name = aws_cloudwatch_log_group.distributed_loaders.name
  pattern        = "{ $.message = \"Job completed\" }"

  metric_transformation {
    name      = "JobCompletions"
    namespace = "Lexara/DistributedLoaders"
    value     = "1"
    unit      = "Count"
  }
}

# Track data quality issues
resource "aws_cloudwatch_log_metric_filter" "data_quality_issues" {
  name           = "data-quality-issues"
  log_group_name = aws_cloudwatch_log_group.distributed_loaders.name
  pattern        = "{ $.message = \"Data quality issue\" }"

  metric_transformation {
    name      = "DataQualityIssues"
    namespace = "Lexara/DistributedLoaders"
    value     = "1"
    unit      = "Count"
  }
}

# =============================================
# CloudWatch Alarms
# =============================================

# Alarm: High error rate
resource "aws_cloudwatch_metric_alarm" "high_error_rate" {
  alarm_name          = "${var.environment}-loader-high-error-rate"
  alarm_description   = "Alert when loader error rate exceeds threshold"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "LoaderErrors"
  namespace           = "Lexara/DistributedLoaders"
  period              = 300 # 5 minutes
  statistic           = "Sum"
  threshold           = 10 # Alert if >10 errors in 5 minutes
  treat_missing_data  = "notBreaching"

  alarm_actions = [] # Add SNS topic ARN for notifications

  tags = {
    Environment = var.environment
    Severity    = "warning"
  }
}

# Alarm: No jobs completing (stalled system)
resource "aws_cloudwatch_metric_alarm" "no_job_completions" {
  alarm_name          = "${var.environment}-loader-no-completions"
  alarm_description   = "Alert when no jobs have completed in 30 minutes"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobCompletions"
  namespace           = "Lexara/DistributedLoaders"
  period              = 1800 # 30 minutes
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching" # Alert if no data

  alarm_actions = [] # Add SNS topic ARN for notifications

  tags = {
    Environment = var.environment
    Severity    = "critical"
  }
}

# Alarm: Low processing velocity
resource "aws_cloudwatch_metric_alarm" "low_velocity" {
  alarm_name          = "${var.environment}-loader-low-velocity"
  alarm_description   = "Alert when processing velocity drops below threshold"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ProcessingVelocity"
  namespace           = "Lexara/DistributedLoaders"
  period              = 300 # 5 minutes
  statistic           = "Average"
  threshold           = 30 # Alert if <30 records/min
  treat_missing_data  = "notBreaching"

  alarm_actions = [] # Add SNS topic ARN for notifications

  tags = {
    Environment = var.environment
    Severity    = "warning"
  }
}

# =============================================
# CloudWatch Dashboard
# =============================================

resource "aws_cloudwatch_dashboard" "distributed_loaders" {
  dashboard_name = "${var.environment}-distributed-loaders"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          title   = "Error Rate"
          metrics = [
            ["Lexara/DistributedLoaders", "LoaderErrors", { stat = "Sum" }]
          ]
          period = 300
          region = data.aws_region.current.name
          yAxis = {
            left = { label = "Errors" }
          }
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Processing Velocity"
          metrics = [
            ["Lexara/DistributedLoaders", "ProcessingVelocity", { stat = "Average" }]
          ]
          period = 300
          region = data.aws_region.current.name
          yAxis = {
            left = { label = "Records/Minute" }
          }
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Records Processed"
          metrics = [
            ["Lexara/DistributedLoaders", "RecordsProcessed", { stat = "Sum" }]
          ]
          period = 300
          region = data.aws_region.current.name
          yAxis = {
            left = { label = "Records" }
          }
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Job Completions"
          metrics = [
            ["Lexara/DistributedLoaders", "JobCompletions", { stat = "Sum" }]
          ]
          period = 300
          region = data.aws_region.current.name
          yAxis = {
            left = { label = "Jobs" }
          }
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Data Quality Issues"
          metrics = [
            ["Lexara/DistributedLoaders", "DataQualityIssues", { stat = "Sum" }]
          ]
          period = 300
          region = data.aws_region.current.name
          yAxis = {
            left = { label = "Issues" }
          }
        }
      },
      {
        type = "log"
        properties = {
          title  = "Recent Error Logs"
          region = data.aws_region.current.name
          query  = <<-EOT
            SOURCE '/lexara/distributed-loaders'
            | fields @timestamp, @message, level, job_id, worker_id
            | filter level = "ERROR"
            | sort @timestamp desc
            | limit 20
          EOT
        }
      }
    ]
  })
}

# =============================================
# Log Insights Saved Queries
# =============================================

resource "aws_cloudwatch_query_definition" "job_performance" {
  name = "${var.environment}/loader-job-performance"

  log_group_names = [
    aws_cloudwatch_log_group.distributed_loaders.name
  ]

  query_string = <<-EOT
    fields @timestamp, job_id, metadata.records_processed, metadata.velocity, metadata.success_rate
    | filter message = "Progress Report" or message = "Job completed"
    | sort @timestamp desc
    | limit 100
  EOT
}

resource "aws_cloudwatch_query_definition" "data_quality_summary" {
  name = "${var.environment}/data-quality-issues"

  log_group_names = [
    aws_cloudwatch_log_group.distributed_loaders.name
  ]

  query_string = <<-EOT
    fields @timestamp, metadata.issue_type, metadata.field_name, metadata.invalid_value
    | filter message = "Data quality issue"
    | stats count() by metadata.issue_type
  EOT
}

resource "aws_cloudwatch_query_definition" "worker_activity" {
  name = "${var.environment}/worker-activity"

  log_group_names = [
    aws_cloudwatch_log_group.distributed_loaders.name
  ]

  query_string = <<-EOT
    fields @timestamp, worker_id, message
    | filter message like /claimed|completed|failed/
    | sort @timestamp desc
    | limit 50
  EOT
}

# =============================================
# Outputs
# =============================================

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.distributed_loaders.name
}

output "log_group_arn" {
  description = "CloudWatch log group ARN"
  value       = aws_cloudwatch_log_group.distributed_loaders.arn
}

output "dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#dashboards:name=${aws_cloudwatch_dashboard.distributed_loaders.dashboard_name}"
}
