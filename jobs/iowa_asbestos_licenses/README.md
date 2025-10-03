# Iowa Asbestos Licenses Loader

## Overview

This job loads Active Iowa Asbestos License holder data from data.iowa.gov into the graph database. It creates Person nodes for approximately 2,600 licensed asbestos professionals with their license information as attributes.

**Data Source:** https://data.iowa.gov/Workforce/Active-Iowa-Asbestos-Licenses/c9cg-ivvu  
**Update Frequency:** Monthly  
**Expected Records:** ~2,600  
**Processing Time:** 5-10 minutes  

## Quick Start

### 1. Download Latest Data
```bash
./scripts/download_data.sh --format csv
```

### 2. Run Loader
```bash
# Test run (100 records)
./scripts/run_asbestos_loader.sh --test

# Full run
./scripts/run_asbestos_loader.sh

# Download and run
./scripts/run_asbestos_loader.sh --download-first
```

### 3. Via Claude Code
```bash
# Complete workflow
./run_job.sh download   # Download data
./run_job.sh start      # Run loader
./run_job.sh status     # Check status
./run_job.sh report     # Generate report
```

## Data Structure

### Input Fields
- **FolderRSN**: Unique identifier
- **Registration Number**: License registration ID
- **License Type**: Worker, Inspector, Contractor/Supervisor, Management Planner, Project Designer
- **First Name**: License holder's first name
- **Last Name**: License holder's last name
- **County**: Iowa county (optional)
- **Issue Date**: License issue date
- **Expire Date**: License expiration date

### Created Entities

#### Person Nodes
```
Node Type: Person
Name: FIRST LAST
Attributes:
  - asbestos_license_type: "Worker"
  - asbestos_registration_number: "23-10167"
  - license_status: "Active"
  - professional_license: "Iowa Asbestos License"
  - license_issue_date: "2023-05-18"
  - license_expire_date: "2023-11-12"
  - iowa_folder_rsn: "103683"
  - computed_first_name: "LUIS"
  - computed_surname: "MEMBRENO ARANA"
```

#### Relationships
1. **Person → State (Iowa)**
   - Type: LICENSED_IN
   - Represents licensing authority
   - Includes license metadata

2. **Person → County** (when available)
   - Type: LOCATED_IN
   - Business county location

## License Type Distribution

Expected distribution:
- **Worker**: 60-70% (~1,600 people)
- **Inspector**: 10-15% (~350 people)
- **Contractor/Supervisor**: 8-12% (~250 people)
- **Management Planner**: 4-6% (~130 people)
- **Project Designer**: 4-6% (~130 people)

## Configuration

Key settings in `config/iowa_asbestos_loader.yaml`:

```yaml
processing:
  batch_size: 500        # Smaller dataset
  checkpoint_interval: 1000

validation:
  required_fields:
    - full_name
    - license_type

provenance:
  confidence_score: 0.95  # Official government source
```

## Monitoring

### Check Progress
```sql
SELECT 
    records_processed || '/' || records_in_file as progress,
    ROUND(100.0 * records_processed / records_in_file, 2) || '%' as percent
FROM sources
WHERE source_type = 'iowa_asbestos_database'
  AND status = 'processing';
```

### Verify Results
```sql
-- Count by license type
SELECT attribute_value, COUNT(*)
FROM attributes
WHERE attribute_type = 'asbestos_license_type'
GROUP BY attribute_value
ORDER BY COUNT(*) DESC;
```

## Scheduling

Run monthly on the first Monday:
```cron
0 3 1-7 * 1 /path/to/jobs/iowa_asbestos_licenses/scripts/run_asbestos_loader.sh
```

## Troubleshooting

### Common Issues

1. **Download fails**
   - Check internet connection
   - Try API endpoint directly
   - Manual download from website

2. **Missing names**
   - Records without first/last name are skipped
   - Check skipped count in statistics

3. **Date format errors**
   - Loader handles multiple formats automatically
   - ISO8601, YYYY-MM-DD, MM/DD/YYYY

### Quick Recovery

Since dataset is small:
```bash
# Just run again - takes only 5 minutes
./scripts/run_asbestos_loader.sh
```

## Files

```
iowa_asbestos_licenses/
├── config/
│   └── iowa_asbestos_loader.yaml    # Configuration
├── scripts/
│   ├── download_data.sh             # Download from data.iowa.gov
│   └── run_asbestos_loader.sh       # Main loader script
├── data/                            # Downloaded CSV files
├── logs/                            # Execution logs
├── reports/                         # Generated reports
├── run_job.sh                       # Claude Code runner
├── ERROR_HANDLING.md                # Error recovery guide
├── STATUS_REPORTING.md              # Monitoring guide
└── README.md                        # This file
```

## Success Criteria

✅ ~2,600 Person nodes created  
✅ All persons have computed name attributes  
✅ 5 distinct license types  
✅ Valid issue/expire dates  
✅ >98% success rate  
✅ <10 minutes processing time  

## Next Steps

After successful load:
1. Check for name aliases (Mike/Michael, Bob/Robert)
2. Verify counties are linked correctly
3. Review expiring licenses
4. Check DLQ for any failures

## Support

- **Logs**: `logs/asbestos_loader_*.log`
- **DLQ Check**: `python3 src/loaders/dlq_handler.py --action stats`
- **Documentation**: `ERROR_HANDLING.md`, `STATUS_REPORTING.md`