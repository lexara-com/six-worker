# Status Reporting Guide - Iowa Business Loader

## Overview

This guide defines the processes and procedures for monitoring and reporting the status of the Iowa Business Loader job. It covers real-time monitoring, scheduled reports, alerting, and performance metrics.

## Status Reporting Levels

### 1. Real-Time Status (During Execution)

#### Console Output
The loader provides real-time status updates every 1,000 records:
```
2024-01-15 10:30:45 - IowaBusinessLoader - INFO - Processed 5,000 records (4,850 successful, 100 failed, 50 skipped) Rate: 72.3/min
```

#### Database Status
```sql
-- Real-time loader status
SELECT 
    source_id,
    status,
    records_processed,
    records_imported,
    records_failed,
    records_skipped,
    ROUND(100.0 * records_imported / NULLIF(records_processed, 0), 2) as success_rate,
    ROUND(records_processed::float / 
          EXTRACT(EPOCH FROM (NOW() - import_started_at)) * 60, 1) as current_rate,
    updated_at,
    NOW() - updated_at as last_update
FROM sources
WHERE source_type = 'iowa_gov_database'
  AND status = 'processing'
ORDER BY created_at DESC
LIMIT 1;
```

### 2. Summary Status (Post-Execution)

#### Completion Report
Generated automatically when job completes:

```markdown
# Iowa Business Loader - Completion Summary

**Job ID:** 01K6GN0A81AR204MWXJ6P1VC6K
**Status:** Completed
**Duration:** 87 minutes

## Results
- Total Records: 312,456
- Successfully Imported: 310,234 (99.3%)
- Failed: 1,822 (0.6%)
- Skipped: 400 (0.1%)

## Entities Created
- Companies: 285,123
- Persons: 45,678
- Addresses: 298,456
- Relationships: 623,890

## Performance
- Average Rate: 59.8 records/minute
- Peak Rate: 125 records/minute
- Checkpoints Saved: 63
```

### 3. Daily Status Report

Run daily at 6 AM to summarize previous day's activities:

```bash
#!/bin/bash
# Daily status report generator
# Add to crontab: 0 6 * * * /path/to/daily_status.sh

cat << 'SQL' | psql -h $DB_HOST -U $DB_USER -d graph_db > daily_report.txt
-- Daily loader summary
WITH daily_runs AS (
    SELECT 
        source_id,
        source_version,
        status,
        import_started_at,
        import_completed_at,
        records_in_file,
        records_imported,
        records_failed,
        records_skipped,
        EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/60 as duration_min
    FROM sources
    WHERE source_type = 'iowa_gov_database'
      AND import_started_at > NOW() - INTERVAL '24 hours'
)
SELECT 
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE status = 'completed') as successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_runs,
    SUM(records_imported) as total_imported,
    SUM(records_failed) as total_failed,
    AVG(duration_min)::int as avg_duration_min,
    MIN(import_started_at) as first_run,
    MAX(import_completed_at) as last_run
FROM daily_runs;

-- Entity growth
SELECT 
    node_type,
    COUNT(*) as created_today,
    (SELECT COUNT(*) FROM nodes n2 
     WHERE n2.node_type = n.node_type 
       AND n2.created_at > NOW() - INTERVAL '7 days') as created_week,
    (SELECT COUNT(*) FROM nodes n3 
     WHERE n3.node_type = n.node_type) as total_count
FROM nodes n
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY node_type
ORDER BY created_today DESC;

-- Error analysis
SELECT 
    error_type,
    COUNT(*) as error_count,
    MIN(created_at) as first_occurrence,
    MAX(created_at) as last_occurrence
FROM failed_records
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY error_type
ORDER BY error_count DESC
LIMIT 10;
SQL
```

## Status Monitoring Procedures

### 1. Health Check Monitoring

```python
#!/usr/bin/env python3
"""
Health check monitor for Iowa Business Loader
Run every 5 minutes during active loading
"""

import psycopg2
import os
import sys
from datetime import datetime, timedelta

def check_loader_health():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database='graph_db',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    with conn.cursor() as cur:
        # Check for active loaders
        cur.execute("""
            SELECT 
                source_id,
                status,
                records_processed,
                updated_at,
                NOW() - updated_at as time_since_update
            FROM sources
            WHERE source_type = 'iowa_gov_database'
              AND status = 'processing'
        """)
        
        result = cur.fetchone()
        
        if not result:
            print("✓ No active loader")
            return "IDLE"
        
        source_id, status, records, updated_at, time_since = result
        
        # Check if stalled
        if time_since > timedelta(minutes=10):
            print(f"⚠ STALLED: No update for {time_since}")
            return "STALLED"
        
        # Check processing rate
        cur.execute("""
            SELECT 
                records_processed::float / 
                EXTRACT(EPOCH FROM (NOW() - import_started_at)) * 60 as rate
            FROM sources
            WHERE source_id = %s
        """, (source_id,))
        
        rate = cur.fetchone()[0]
        
        if rate < 10:
            print(f"⚠ SLOW: Processing at {rate:.1f} records/min")
            return "SLOW"
        
        print(f"✓ HEALTHY: {records} processed at {rate:.1f}/min")
        return "HEALTHY"

if __name__ == "__main__":
    status = check_loader_health()
    sys.exit(0 if status in ["IDLE", "HEALTHY"] else 1)
```

### 2. Progress Tracking

```sql
-- Detailed progress tracking
WITH progress_data AS (
    SELECT 
        source_id,
        records_in_file,
        records_processed,
        records_imported,
        records_failed,
        records_skipped,
        import_started_at,
        NOW() - import_started_at as elapsed
    FROM sources
    WHERE source_type = 'iowa_gov_database'
      AND status = 'processing'
    ORDER BY created_at DESC
    LIMIT 1
),
estimates AS (
    SELECT 
        *,
        records_processed::float / EXTRACT(EPOCH FROM elapsed) as records_per_sec,
        (records_in_file - records_processed)::float / 
            (records_processed::float / EXTRACT(EPOCH FROM elapsed)) / 60 as minutes_remaining
    FROM progress_data
)
SELECT 
    records_processed || '/' || records_in_file as progress,
    ROUND(100.0 * records_processed / records_in_file, 2) || '%' as percent_complete,
    ROUND(records_per_sec * 60, 1) || ' rec/min' as current_rate,
    ROUND(minutes_remaining, 0) || ' minutes' as eta,
    TO_CHAR(NOW() + (minutes_remaining || ' minutes')::interval, 'HH24:MI') as estimated_completion
FROM estimates;
```

### 3. Performance Metrics

```sql
-- Performance analysis over time
WITH time_windows AS (
    SELECT 
        DATE_TRUNC('hour', import_started_at) as hour,
        AVG(records_imported::float / 
            EXTRACT(EPOCH FROM (import_completed_at - import_started_at)) * 60) as avg_rate,
        COUNT(*) as runs,
        AVG(EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/60) as avg_duration
    FROM sources
    WHERE source_type = 'iowa_gov_database'
      AND status = 'completed'
      AND import_started_at > NOW() - INTERVAL '7 days'
    GROUP BY DATE_TRUNC('hour', import_started_at)
)
SELECT 
    hour,
    ROUND(avg_rate, 1) as avg_records_per_min,
    runs,
    ROUND(avg_duration, 0) as avg_duration_min
FROM time_windows
ORDER BY hour DESC;
```

## Alert Triggers

### Critical Alerts (Immediate Action Required)

| Condition | Threshold | Action |
|-----------|-----------|---------|
| Loader Stalled | No update >15 min | Check process, database locks |
| High Failure Rate | >10% failures | Review DLQ, check data quality |
| Database Connection Lost | Connection error | Verify credentials, network |
| Disk Space Low | <1GB free | Clean logs, expand storage |

### Warning Alerts (Monitor Closely)

| Condition | Threshold | Action |
|-----------|-----------|---------|
| Slow Processing | <20 rec/min | Check resources, database load |
| Moderate Failures | 5-10% failures | Review error patterns |
| Long Running | >3 hours | Check for issues |
| Memory Usage High | >80% | Consider reducing batch size |

## Status Reporting Formats

### 1. JSON Status (Machine-Readable)

```json
{
  "job_id": "01K6GN0A81AR204MWXJ6P1VC6K",
  "job_name": "Iowa Business Loader",
  "status": "processing",
  "started_at": "2024-01-15T10:00:00Z",
  "current_time": "2024-01-15T11:30:00Z",
  "progress": {
    "total": 312456,
    "processed": 156228,
    "successful": 155000,
    "failed": 1000,
    "skipped": 228,
    "percent_complete": 50.0
  },
  "performance": {
    "current_rate": 72.3,
    "average_rate": 68.5,
    "estimated_completion": "2024-01-15T12:30:00Z"
  },
  "health": "HEALTHY"
}
```

### 2. Slack/Email Notification

```markdown
**Iowa Business Loader Update**
Status: ✅ Processing
Progress: 156,228/312,456 (50%)
Rate: 72.3 records/min
ETA: 12:30 PM
Health: HEALTHY
```

### 3. Dashboard Metrics

Key metrics for monitoring dashboards:

```yaml
metrics:
  - name: loader_records_processed_total
    type: counter
    labels: [source_type, status]
    
  - name: loader_processing_rate
    type: gauge
    unit: records_per_minute
    
  - name: loader_error_rate
    type: gauge
    unit: percent
    
  - name: loader_duration_seconds
    type: histogram
    buckets: [60, 300, 600, 1800, 3600, 7200]
```

## Status Check Commands

### Quick Status
```bash
./run_job.sh status
```

### Detailed Database Status
```sql
\x on  -- Extended display
SELECT * FROM sources
WHERE source_type = 'iowa_gov_database'
ORDER BY created_at DESC
LIMIT 1;
```

### DLQ Status
```bash
python3 src/loaders/dlq_handler.py --action stats
```

### System Resources
```bash
# Memory and CPU
top -n 1 | head -10

# Disk space
df -h | grep -E "Filesystem|postgres|lexara"

# Database connections
psql -c "SELECT count(*) FROM pg_stat_activity WHERE state != 'idle'"
```

## Scheduled Reports

### Daily Report (6 AM)
```cron
0 6 * * * /path/to/jobs/iowa_business_loader/scripts/daily_report.sh
```

### Weekly Summary (Monday 8 AM)
```cron
0 8 * * 1 /path/to/jobs/iowa_business_loader/scripts/weekly_summary.sh
```

### Monthly Statistics (First of month)
```cron
0 0 1 * * /path/to/jobs/iowa_business_loader/scripts/monthly_stats.sh
```

## Custom Report Generation

Generate ad-hoc reports:

```bash
# Generate current report
./run_job.sh report

# Custom date range report
psql -h $DB_HOST -U $DB_USER -d graph_db << SQL
SELECT 
    DATE(import_started_at) as date,
    COUNT(*) as runs,
    SUM(records_imported) as total_imported,
    AVG(records_imported) as avg_per_run,
    SUM(records_failed) as total_failed,
    AVG(EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/60) as avg_duration_min
FROM sources
WHERE source_type = 'iowa_gov_database'
  AND import_started_at BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY DATE(import_started_at)
ORDER BY date;
SQL
```

## Integration Points

### 1. Monitoring Systems
- Prometheus metrics endpoint: `:9090/metrics`
- CloudWatch custom metrics
- Datadog APM integration

### 2. Notification Channels
- Email: admin@example.com
- Slack: #data-loaders channel
- PagerDuty: critical alerts only

### 3. Logging Systems
- Local logs: `jobs/iowa_business_loader/logs/`
- Centralized: ELK stack / Splunk
- CloudWatch Logs (AWS)

## Best Practices

1. **Regular Monitoring**
   - Check status at start of each run
   - Monitor first 15 minutes closely
   - Review completion summary

2. **Proactive Alerts**
   - Set up alerts before issues become critical
   - Monitor trends, not just thresholds
   - Alert on unusual patterns

3. **Documentation**
   - Document all manual interventions
   - Track recurring issues
   - Update runbooks regularly

4. **Performance Baselines**
   - Establish normal processing rates
   - Track performance over time
   - Investigate degradation

5. **Report Distribution**
   - Send daily summaries to stakeholders
   - Escalate issues appropriately
   - Archive reports for trend analysis