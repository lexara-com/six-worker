# Iowa Business Entities Import Script

## Overview

This script demonstrates a real-world implementation of the Propose API workflow by importing the complete Active Iowa Business Entities dataset from Iowa.gov.

## Data Source

- **Source**: https://data.iowa.gov/Regulation/Active-Iowa-Business-Entities/ez5t-3qay/about_data
- **Downloaded**: October 1, 2025 at 04:17 AM
- **Records**: ~300,000 active business entities
- **Provenance**: Official Iowa government database with high confidence (0.92)

## What It Does

The script imports each Iowa business entity by:

1. **Creating Company Entities**: Each business becomes a Company node with attributes like:
   - Iowa corporation number
   - Entity type (DOMESTIC BANKS, INSURANCE COMPANIES, etc.)
   - Incorporation date
   - Headquarters location

2. **Creating Registered Agent Entities**: When present, registered agents become Person or Company nodes with:
   - Address information
   - Location details
   - Agent role designation

3. **Establishing Relationships**:
   - `Incorporated_In`: Company → State of Iowa
   - `Registered_Agent`: Agent → Company

4. **Automatic Features**:
   - Entity resolution (detects duplicates)
   - Conflict detection
   - Complete provenance tracking
   - Audit trail creation

## Usage

### Basic Import (Testing)
```bash
# Import first 100 records for testing
python3 scripts/import_iowa_businesses.py --limit 100

# Import with debug logging
python3 scripts/import_iowa_businesses.py --limit 50 --log-level DEBUG

# Start from a specific row (useful for resuming)
python3 scripts/import_iowa_businesses.py --start-from 1000 --limit 500
```

### Full Production Import
```bash
# Import all ~300k records (takes several hours)
python3 scripts/import_iowa_businesses.py

# Import in chunks for better monitoring
python3 scripts/import_iowa_businesses.py --limit 10000 --batch-size 100
```

### Environment Setup
```bash
# Required environment variables
export DB_HOST="your-aurora-endpoint"
export DB_NAME="graph_db"
export DB_USER="graph_admin"
export DB_PASS="your-password"
export DB_PORT="5432"
```

## Performance Characteristics

Based on testing:
- **Processing Rate**: ~89 records/minute
- **Full Dataset Time**: ~55 hours for 300k records
- **Database Growth**: 
  - ~300k company entities
  - ~150k agent entities (estimate)
  - ~450k relationships
  - ~900k provenance records

## Real-World Features Demonstrated

### 1. Entity Resolution
```
Input: "ACME CORP" vs existing "ACME CORPORATION"
Result: Fuzzy matching detects potential duplicate
Action: Human review flagged
```

### 2. Conflict Detection
```
Input: Agent "John Smith" represents competing companies
Result: Conflict detected and flagged
Action: Legal team alerted for ethics review
```

### 3. Data Quality
```
Source: Iowa.gov (confidence: 0.92)
Validation: Official corporation numbers cross-referenced
Audit: Complete provenance trail maintained
```

### 4. Schema Compliance
The script demonstrates working within existing database constraints:
- Node types limited to: Person, Company, Place, Thing, Event
- All relationships properly categorized
- ULID generation for all identifiers

## Output and Logs

The script generates:
- **Console Output**: Real-time progress and statistics
- **Log Files**: Detailed processing logs with timestamps
- **Statistics Report**: Final summary of import results

Sample output:
```
============================================================
IOWA BUSINESS IMPORT STATISTICS  
============================================================
Total rows processed: 50
Records imported: 50
Failed: 0
Skipped: 0
Companies created/matched: 50
Agents created/matched: 29
Relationships created: 79
Conflicts detected: 1
Processing rate: 89.4 records/minute
Elapsed time: 0:00:33.559357
============================================================
```

## Integration with Conflict Checking

Once imported, this data becomes immediately useful for law firm operations:

### Client Intake
```python
# Check if potential client exists in Iowa business database
result = client.propose_fact(
    source_entity=('Company', 'Potential Client Corp'),
    target_entity=('LawFirm', 'Our Firm'),
    relationship='Potential_Client',
    source_info=('Intake Form', 'client_intake')
)

# Automatic match with Iowa business data
if result.actions[0]['match_reason'] == 'exact_name_match':
    print(f"Found in Iowa database: Corp #{entity_attributes['iowa_corp_number']}")
```

### Conflict Detection
```python
# Opposing counsel check
result = client.propose_fact(
    source_entity=('Person', 'Attorney Name'),
    target_entity=('Company', 'Opposing Party'),
    relationship='Opposing_Counsel',
    source_info=('Court Filing', 'legal_records')
)

# Automatic conflict detection if attorney already represents client
if result.conflicts:
    print("CONFLICT: Attorney already represents a client in this matter")
```

## Data Quality Monitoring

After import, monitor data quality with:

```sql
-- Check Iowa business data quality
SELECT 
    COUNT(*) as total_companies,
    COUNT(*) FILTER (WHERE a.attribute_type = 'iowa_corp_number') as with_corp_numbers,
    AVG(p.confidence_score) as avg_confidence
FROM nodes n
LEFT JOIN attributes a ON n.node_id = a.node_id
LEFT JOIN provenance p ON n.node_id = p.asset_id
WHERE n.node_type = 'Company'
  AND EXISTS (
      SELECT 1 FROM provenance p2 
      WHERE p2.asset_id = n.node_id 
      AND p2.source_type = 'iowa_gov_database'
  );
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify database credentials and network access
2. **Node Type Errors**: Ensure schema allows required entity types
3. **Memory Issues**: Use smaller batch sizes for large imports
4. **Timeout Errors**: Increase timeout values for slow networks

### Resume Interrupted Import
```bash
# Check last processed record in logs
tail -100 iowa_import_20251001_*.log | grep "Processing row"

# Resume from that point
python3 scripts/import_iowa_businesses.py --start-from 15000
```

## Future Enhancements

Potential improvements for production use:
1. **Parallel Processing**: Multi-threaded import for faster processing
2. **Delta Updates**: Periodic refresh of changed records
3. **Data Validation**: Enhanced business rule validation
4. **Progress Persistence**: Save/resume state for long-running imports
5. **Error Recovery**: Automatic retry of failed records

---

This script serves as a complete example of using the Propose API for real-world data ingestion, demonstrating all key features while maintaining data quality and audit trails suitable for legal environments.