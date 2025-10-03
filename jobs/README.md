# Jobs Directory - Rerunnable Data Loaders

## Overview

This directory contains rerunnable data loader jobs that follow the enterprise loader specification. Each job is self-contained in its own subdirectory with configuration, scripts, logs, and documentation.

## Directory Structure

```
jobs/
├── README.md                           # This file
├── iowa_business_loader/               # Iowa Business Entities loader
│   ├── config/                        # Job configuration
│   │   └── iowa_business_loader.yaml  # Loader settings
│   ├── scripts/                       # Execution scripts
│   │   └── run_iowa_loader.sh        # Main loader script
│   ├── logs/                          # Job logs
│   ├── reports/                       # Generated reports
│   ├── run_job.sh                    # Claude Code runner
│   ├── CLAUDE_CODE_INSTRUCTIONS.md   # Instructions for Claude Code
│   ├── ERROR_HANDLING.md             # Error recovery procedures
│   └── STATUS_REPORTING.md           # Monitoring and reporting
└── [future_loader]/                   # Additional loaders follow same pattern
```

## Running Jobs

### Method 1: Direct Execution
```bash
cd jobs/iowa_business_loader
./scripts/run_iowa_loader.sh
```

### Method 2: Job Runner (Recommended)
```bash
cd jobs/iowa_business_loader
./run_job.sh start
```

### Method 3: Through Claude Code
Ask Claude Code to run the job with proper monitoring:
```
Please run the Iowa Business loader job at:
jobs/iowa_business_loader/run_job.sh start

Monitor the progress and report the final statistics.
```

## Job Management

### Check Status
```bash
./run_job.sh status
```

### Resume Failed Job
```bash
./run_job.sh resume
# Then run the loader again - it will auto-resume
```

### Generate Report
```bash
./run_job.sh report
```

## Common Operations

### View Logs
```bash
# Latest log
ls -t jobs/iowa_business_loader/logs/*.log | head -1 | xargs tail -f

# All logs
ls -la jobs/iowa_business_loader/logs/
```

### Check Database Status
```sql
-- Active loaders
SELECT source_type, status, records_processed, updated_at
FROM sources
WHERE status = 'processing';

-- Recent completions
SELECT source_type, source_version, status, 
       records_imported, records_failed,
       import_completed_at
FROM sources
WHERE import_completed_at > NOW() - INTERVAL '24 hours'
ORDER BY import_completed_at DESC;
```

### Process Failed Records
```bash
# Check DLQ statistics
python3 src/loaders/dlq_handler.py --action stats

# Reprocess failed records
python3 src/loaders/dlq_handler.py --action reprocess --limit 100
```

## Creating New Jobs

To create a new loader job:

1. **Create job directory structure**
```bash
mkdir -p jobs/my_new_loader/{config,scripts,logs,reports}
```

2. **Copy template files**
```bash
cp jobs/iowa_business_loader/run_job.sh jobs/my_new_loader/
cp jobs/iowa_business_loader/*.md jobs/my_new_loader/
```

3. **Create loader implementation**
- Extend `BaseDataLoader` class
- Implement required methods
- Follow patterns in `src/loaders/iowa_business_loader.py`

4. **Create configuration**
- Define source settings
- Set processing parameters
- Configure validation rules

5. **Update documentation**
- Customize ERROR_HANDLING.md
- Update STATUS_REPORTING.md
- Modify CLAUDE_CODE_INSTRUCTIONS.md

## Best Practices

### 1. Idempotency
- Always use file hashing to prevent duplicate processing
- Implement proper versioning for periodic updates
- Ensure all operations can be safely retried

### 2. Error Handling
- Use the DLQ for all failed records
- Implement exponential backoff for retries
- Provide clear error messages and recovery steps

### 3. Monitoring
- Save checkpoints regularly (every 1000-5000 records)
- Log progress at consistent intervals
- Track key metrics (success rate, processing speed)

### 4. Documentation
- Keep ERROR_HANDLING.md current with known issues
- Document all manual interventions
- Update CLAUDE_CODE_INSTRUCTIONS.md with lessons learned

## Scheduling

### Cron Examples
```bash
# Daily at 2 AM
0 2 * * * /path/to/jobs/my_loader/scripts/run_loader.sh

# Weekly on Sunday at 3 AM
0 3 * * 0 /path/to/jobs/my_loader/scripts/run_loader.sh

# Quarterly (Jan, Apr, Jul, Oct) on first Monday
0 2 1-7 1,4,7,10 1 /path/to/jobs/iowa_business_loader/scripts/run_iowa_loader.sh
```

### Systemd Timer (Alternative)
```ini
# /etc/systemd/system/iowa-loader.timer
[Unit]
Description=Iowa Business Loader Timer

[Timer]
OnCalendar=Mon *-01,04,07,10-01..07 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

## Performance Guidelines

### Batch Sizes
- Small dataset (<10K): 100-500 records
- Medium dataset (10K-100K): 500-1000 records
- Large dataset (>100K): 1000-5000 records

### Checkpoint Intervals
- Every 1000 records for small datasets
- Every 5000 records for medium datasets
- Every 10000 records for large datasets

### Resource Requirements
- Memory: 1-2GB per loader
- CPU: 1-2 cores
- Disk: 10GB for logs and temp files
- Network: Stable connection to database

## Troubleshooting

### Common Issues

1. **Loader Won't Start**
   - Check lock file: `.locks/loader.lock`
   - Verify database connectivity
   - Check file permissions

2. **Slow Performance**
   - Reduce batch size
   - Check database indexes
   - Monitor system resources

3. **High Failure Rate**
   - Review validation rules
   - Check data quality
   - Examine DLQ for patterns

4. **Stalled Process**
   - Check database locks
   - Review recent changes
   - Kill and restart if needed

## Support

For issues or questions:
1. Check job-specific ERROR_HANDLING.md
2. Review logs in jobs/*/logs/
3. Check DLQ for failed records
4. Consult docs/RERUNNABLE_LOADER_SPEC.md

## Future Enhancements

- [ ] Web UI for job monitoring
- [ ] Automated alerting system
- [ ] Parallel processing support
- [ ] Real-time progress dashboard
- [ ] Automatic retry scheduling
- [ ] Performance analytics
- [ ] Data quality scoring
- [ ] Integration with workflow orchestrators