# Propose API Python Integration Examples

This directory contains Python integration examples for the Propose API intelligent fact ingestion system.

## Files

- **`propose_api_client.py`**: Core Python client library for interacting with the Propose API
- **`usage_examples.py`**: Comprehensive usage examples covering common law firm scenarios
- **`requirements.txt`**: Python dependencies required for the client
- **`README.md`**: This documentation file

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables
```bash
export DB_HOST="your-database-host"
export DB_NAME="graph_db"
export DB_USER="graph_admin" 
export DB_PASS="your-password"
export DB_PORT="5432"
```

### 3. Basic Usage
```python
from propose_api_client import ProposeAPIClient

# Setup client
client = ProposeAPIClient({
    'host': 'your-host',
    'database': 'graph_db',
    'user': 'graph_admin', 
    'password': 'your-password',
    'port': 5432
})

# Propose a fact
result = client.propose_fact(
    source_entity=('Person', 'John Smith'),
    target_entity=('Company', 'ACME Corporation'),
    relationship='Employment',
    source_info=('HR Database', 'hr_system')
)

print(f"Status: {result.status}")
print(f"Confidence: {result.overall_confidence}")
```

## Examples Covered

The `usage_examples.py` file demonstrates:

1. **Client Intake Processing**: How to process new client intake forms and detect conflicts
2. **Attorney Verification**: Verifying attorney employment from official sources
3. **Document Processing**: Extracting relationships from legal documents
4. **Conflict Checking**: Systematic pre-case conflict checking workflow
5. **Bulk Data Import**: Processing large datasets from external systems

## API Client Features

The `ProposeAPIClient` class provides:

- **Intelligent Fact Ingestion**: Entity resolution, conflict detection, provenance tracking
- **Batch Processing**: Handle multiple facts efficiently
- **Error Handling**: Comprehensive error management with detailed responses
- **Provenance Queries**: Access audit trails and data sources
- **Conflict Detection**: Check for relationship conflicts between entities

## Response Format

All API calls return a `ProposeResponse` object with:

```python
response = ProposeResponse(
    success=bool,                    # True if successful
    status='success|conflicts|error', # Operation status
    overall_confidence=float,         # 0.0 to 1.0
    actions=[...],                   # Actions taken
    conflicts=[...],                 # Conflicts detected
    provenance_ids=[...],           # Provenance records created
    error_message=str               # Error details (if any)
)
```

## Integration Patterns

### Single Fact Ingestion
```python
result = client.propose_fact(
    source_entity=('Person', 'Attorney Name'),
    target_entity=('Company', 'Law Firm'),
    relationship='Employment',
    source_info=('Source Name', 'source_type'),
    source_attributes={'title': 'Partner'},
    relationship_strength=0.95
)
```

### Batch Processing
```python
facts = [
    {
        'source_entity': ('Person', 'Person1'),
        'target_entity': ('Company', 'Company1'),
        'relationship': 'Employment',
        'source_info': ('Source', 'type')
    },
    # ... more facts
]

results = client.batch_propose_facts(facts)
```

### Conflict Checking
```python
# Propose potential opposing relationship
result = client.propose_fact(
    source_entity=('Person', 'Attorney'),
    target_entity=('Company', 'Opposing Party'),
    relationship='Opposing_Counsel',
    source_info=('Court Filing', 'legal_records')
)

if result.conflicts:
    print("Conflicts detected - manual review required")
```

## Error Handling

The client handles various error conditions:

- Database connection failures
- SQL execution errors  
- Response parsing errors
- Validation errors

All errors are returned as `ProposeResponse` objects with appropriate error messages.

## Running the Examples

```bash
# Run all examples
python usage_examples.py

# Run just the client demo
python propose_api_client.py
```

## Database Requirements

The client requires:
- PostgreSQL database with Propose API functions deployed
- Network access to the database
- Valid credentials with appropriate permissions

See the main project documentation for database setup instructions.

## Support

For issues or questions:
1. Check the main Propose API documentation (`docs/PROPOSE_API.md`)
2. Review the comprehensive test cases (`scripts/propose_api_demo.sql`)
3. Examine the provenance system documentation (`scripts/provenance_demo.sql`)