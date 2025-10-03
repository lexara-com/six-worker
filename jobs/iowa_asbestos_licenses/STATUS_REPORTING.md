# Status Reporting Guide - Iowa Asbestos Licenses Loader

## Overview

This guide defines status reporting procedures for the Iowa Asbestos Licenses Loader. Given the small dataset size (~2,600 records), monitoring is straightforward and processing is typically complete within 5-10 minutes.

## Key Metrics

### Expected Outcomes
- **Total Records**: ~2,600 active license holders
- **Person Nodes**: ~2,600 (one per license holder)
- **License Types**: 5 categories
  - Worker (~1,500-1,800)
  - Inspector (~300-400)
  - Contractor/Supervisor (~200-300)
  - Management Planner (~100-150)
  - Project Designer (~100-150)
- **Processing Time**: 5-10 minutes
- **Success Rate**: >98%

## Real-Time Monitoring

### During Execution
```bash
# Watch log file
tail -f logs/asbestos_loader_*.log

# Key indicators to watch for:
# - "Processed 500 records" (every 500)
# - "Checkpoint saved" (every 1000)
# - "Source marked as complete"
```

### Progress Query
```sql
-- Real-time progress
SELECT 
    source_type,
    status,
    records_processed || '/' || records_in_file as progress,
    ROUND(100.0 * records_processed / NULLIF(records_in_file, 0), 2) || '%' as percent_complete,
    records_imported || ' imported' as success,
    records_failed || ' failed' as failures,
    NOW() - updated_at as last_update
FROM sources
WHERE source_type = 'iowa_asbestos_database'
  AND status = 'processing'
ORDER BY created_at DESC
LIMIT 1;
```

## Post-Load Verification

### Summary Statistics
```sql
-- Load summary
SELECT 
    source_version,
    status,
    records_in_file,
    records_imported,
    records_failed,
    records_skipped,
    ROUND(100.0 * records_imported / NULLIF(records_processed, 0), 2) as success_rate,
    import_completed_at - import_started_at as duration
FROM sources
WHERE source_type = 'iowa_asbestos_database'
ORDER BY created_at DESC
LIMIT 1;
```

### License Distribution
```sql
-- License type breakdown
SELECT 
    attribute_value as license_type,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percent
FROM attributes
WHERE attribute_type = 'asbestos_license_type'
GROUP BY attribute_value
ORDER BY count DESC;

-- Expected distribution:
-- Worker: 60-70%
-- Inspector: 10-15%
-- Contractor/Supervisor: 8-12%
-- Management Planner: 4-6%
-- Project Designer: 4-6%
```

### Geographic Distribution
```sql
-- Counties with most license holders
SELECT 
    cn.primary_name as county,
    COUNT(DISTINCT r.source_node_id) as license_holders
FROM relationships r
JOIN nodes cn ON r.target_node_id = cn.node_id
WHERE cn.node_type = 'County'
  AND r.relationship_type = 'Located_In'
  AND EXISTS (
    SELECT 1 FROM attributes a
    WHERE a.node_id = r.source_node_id
      AND a.attribute_type = 'asbestos_license_type'
  )
GROUP BY cn.primary_name
ORDER BY license_holders DESC
LIMIT 10;

-- Note: County data may not be available for all records
```

### License Expiration Analysis
```sql
-- Upcoming expirations by month
SELECT 
    TO_CHAR(attribute_value::date, 'YYYY-MM') as expire_month,
    COUNT(*) as licenses_expiring
FROM attributes
WHERE attribute_type = 'license_expire_date'
  AND attribute_value::date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '12 months'
GROUP BY TO_CHAR(attribute_value::date, 'YYYY-MM')
ORDER BY expire_month;

-- Expired licenses (should be none in active dataset)
SELECT COUNT(*) as expired_count
FROM attributes
WHERE attribute_type = 'license_expire_date'
  AND attribute_value::date < CURRENT_DATE;
```

## Data Quality Checks

### Name Completeness
```sql
-- Check name attribute completeness
WITH license_holders AS (
    SELECT DISTINCT node_id
    FROM attributes
    WHERE attribute_type = 'asbestos_license_type'
)
SELECT 
    'Total License Holders' as metric,
    COUNT(*) as count
FROM license_holders
UNION ALL
SELECT 
    'With First Name',
    COUNT(DISTINCT a.node_id)
FROM attributes a
JOIN license_holders l ON a.node_id = l.node_id
WHERE a.attribute_type = 'computed_first_name'
UNION ALL
SELECT 
    'With Last Name',
    COUNT(DISTINCT a.node_id)
FROM attributes a
JOIN license_holders l ON a.node_id = l.node_id
WHERE a.attribute_type = 'computed_surname';

-- Should all be equal (~2,600)
```

### Duplicate Detection
```sql
-- Check for potential duplicates
SELECT 
    n.primary_name,
    COUNT(*) as occurrences,
    STRING_AGG(DISTINCT a.attribute_value, ', ') as license_types
FROM nodes n
JOIN attributes a ON n.node_id = a.node_id
WHERE n.node_type = 'Person'
  AND a.attribute_type = 'asbestos_license_type'
GROUP BY n.primary_name
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;

-- Note: Same person may legitimately have multiple license types
```

### Registration Number Uniqueness
```sql
-- Verify registration numbers are unique
SELECT 
    attribute_value as registration_number,
    COUNT(*) as count
FROM attributes
WHERE attribute_type = 'asbestos_registration_number'
GROUP BY attribute_value
HAVING COUNT(*) > 1;

-- Should return no rows (all unique)
```

## Performance Metrics

### Processing Rate
```sql
-- Calculate average processing rate
WITH loader_stats AS (
    SELECT 
        source_id,
        records_processed,
        import_started_at,
        import_completed_at,
        EXTRACT(EPOCH FROM (import_completed_at - import_started_at)) as seconds_elapsed
    FROM sources
    WHERE source_type = 'iowa_asbestos_database'
      AND status = 'completed'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT 
    records_processed,
    ROUND(seconds_elapsed/60.0, 1) as minutes_elapsed,
    ROUND(records_processed / (seconds_elapsed/60.0), 0) as records_per_minute,
    ROUND(records_processed / (seconds_elapsed), 1) as records_per_second
FROM loader_stats;

-- Expected: 250-500 records/minute
```

## Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| Processing Time | >10 min | >20 min |
| Success Rate | <98% | <95% |
| Records Processed | <2,500 | <2,000 |
| Processing Rate | <100/min | <50/min |
| Failed Records | >50 | >100 |

## Daily Report Query
```sql
-- Daily summary for Iowa Asbestos Licenses
WITH today_stats AS (
    SELECT 
        COUNT(*) as runs_today,
        SUM(records_imported) as total_imported,
        SUM(records_failed) as total_failed,
        AVG(EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/60) as avg_minutes
    FROM sources
    WHERE source_type = 'iowa_asbestos_database'
      AND import_started_at > CURRENT_DATE
),
license_stats AS (
    SELECT 
        COUNT(DISTINCT node_id) as total_license_holders,
        COUNT(DISTINCT attribute_value) as license_types
    FROM attributes
    WHERE attribute_type = 'asbestos_license_type'
)
SELECT 
    'Runs Today' as metric,
    runs_today::text as value
FROM today_stats
UNION ALL
SELECT 
    'Records Imported',
    COALESCE(total_imported, 0)::text
FROM today_stats
UNION ALL
SELECT 
    'Average Duration',
    COALESCE(ROUND(avg_minutes, 1)::text || ' min', 'N/A')
FROM today_stats
UNION ALL
SELECT 
    'Total License Holders',
    total_license_holders::text
FROM license_stats
UNION ALL
SELECT 
    'License Types',
    license_types::text
FROM license_stats;
```

## Monthly Trend Analysis
```sql
-- Monthly loader performance
SELECT 
    DATE_TRUNC('month', import_started_at) as month,
    COUNT(*) as runs,
    AVG(records_imported) as avg_records,
    AVG(EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/60) as avg_minutes,
    MIN(records_imported) as min_records,
    MAX(records_imported) as max_records
FROM sources
WHERE source_type = 'iowa_asbestos_database'
  AND status = 'completed'
GROUP BY DATE_TRUNC('month', import_started_at)
ORDER BY month DESC
LIMIT 12;
```

## Quick Status Commands

### Check Latest Run
```bash
./run_job.sh status
```

### Generate Report
```bash
./run_job.sh report
cat reports/report_*.md | head -50
```

### View Statistics
```bash
# Quick stats from latest log
grep "Statistics:" logs/asbestos_loader_*.log | tail -1 -A 10
```

### Check DLQ
```bash
python3 src/loaders/dlq_handler.py --action stats | grep asbestos
```

## Success Criteria

A successful load shows:
1. ✅ Status: completed
2. ✅ ~2,600 records imported
3. ✅ <50 failed records
4. ✅ 5 distinct license types
5. ✅ Processing time <10 minutes
6. ✅ Name attributes computed for all persons
7. ✅ Valid date ranges for licenses

## Troubleshooting Indicators

Watch for these warning signs:
- ⚠️ Processing rate <100/minute
- ⚠️ >100 failed records
- ⚠️ Missing license types
- ⚠️ Duplicate registration numbers
- ⚠️ Many records without counties
- ⚠️ Expired licenses in "active" dataset

## Integration Points

### Monitoring Systems
- CloudWatch: Custom metric for asbestos_licenses_imported
- Prometheus: iowa_asbestos_loader_records_total counter
- Datadog: Tagged with source:iowa_asbestos

### Notifications
- Email: On completion (success/failure)
- Slack: Summary statistics
- PagerDuty: Only if critical failure

### Downstream Systems
- Conflict checking: New persons available immediately
- Name alias matching: Applied to all new persons
- License expiration alerts: Based on expire_date attribute