# Propose API - Intelligent Fact Ingestion System

## Overview

The Propose API is an intelligent fact ingestion system designed for law firm conflict checking databases. Unlike traditional CRUD operations, it accepts proposed facts (consisting of two entities and their relationship) and intelligently processes them through entity resolution, conflict detection, and automatic provenance tracking.

## Key Features

- **Entity Resolution**: Uses exact matching, alias matching, and fuzzy matching to identify existing entities
- **Conflict Detection**: Identifies conflicting relationships (e.g., Legal_Counsel vs Opposing_Counsel)
- **Provenance Tracking**: Automatically creates audit trails for all data with source attribution
- **Change History**: Tracks all modifications with full audit logs
- **Confidence Scoring**: Provides confidence metrics for all operations

## Main Function Interface

```sql
SELECT propose_fact(
    -- Required: Source entity
    p_source_node_type VARCHAR(50),
    p_source_node_name VARCHAR(255),
    
    -- Required: Target entity  
    p_target_node_type VARCHAR(50),
    p_target_node_name VARCHAR(255),
    
    -- Required: Relationship
    p_relationship_type VARCHAR(50),
    
    -- Required: Provenance
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50),
    
    -- Optional: Entity attributes
    p_source_attributes JSONB DEFAULT '[]'::JSONB,
    p_target_attributes JSONB DEFAULT '[]'::JSONB,
    
    -- Optional: Relationship details
    p_relationship_strength DECIMAL(3,2) DEFAULT 1.0,
    p_relationship_valid_from DATE DEFAULT NULL,
    p_relationship_valid_to DATE DEFAULT NULL,
    p_relationship_metadata JSONB DEFAULT NULL,
    
    -- Optional: Provenance details
    p_provenance_confidence DECIMAL(3,2) DEFAULT 0.9,
    p_provenance_metadata JSONB DEFAULT NULL
);
```

## Response Format

The API returns a `propose_api_response` type with the following structure:

```sql
{
    status: 'success' | 'conflicts' | 'error',
    overall_confidence: DECIMAL(3,2),  -- 0.0 to 1.0
    actions: [
        {
            action: string,           -- What action was taken
            entity_type: string,      -- 'source' | 'target' 
            entity_id: string,        -- ULID of the entity
            confidence: decimal,      -- Confidence in this action
            match_reason: string      -- How the match was made
        }
    ],
    conflicts: [
        {
            type: string,             -- Type of conflict detected
            conflicts: array          -- Details of conflicting relationships
        }
    ],
    provenance_ids: [string]         -- ULIDs of provenance records created
}
```

## Entity Resolution Process

### 1. Exact Name Matching
- Compares normalized names for perfect matches
- Confidence: 1.0
- Match reason: 'exact_name_match'

### 2. Alias Matching  
- Searches through `nameAlias` attributes
- Confidence: 0.9
- Match reason: 'alias_match'

### 3. Fuzzy Matching
- Uses PostgreSQL trigram similarity
- Confidence: 0.4 - 0.8 based on similarity score
- Match reason: 'fuzzy_match'

### 4. Entity Creation
- Creates new entity if no good matches found
- Confidence: 1.0
- Match reason: 'new_entity'

## Relationship Evaluation

### Duplicate Detection
- Identifies existing relationships between same entities
- Action: 'duplicate' if same strength, 'updated' if higher strength

### Conflict Detection
- Detects opposing relationships (Legal_Counsel vs Opposing_Counsel)
- Creates relationship with reduced confidence (0.7x original)
- Records conflicts in response

### New Relationships
- Creates new relationship if none exists
- Full confidence in relationship strength

## Usage Examples

### Basic Fact Ingestion
```sql
SELECT propose_fact(
    'Person', 'John Smith',
    'Company', 'ACME Corporation', 
    'Employment',
    'HR Database Export',
    'hr_system'
);
```

### With Entity Attributes
```sql
SELECT propose_fact(
    'Person', 'Maria Rodriguez',
    'Company', 'Global Holdings Inc',
    'Employment', 
    'LinkedIn Profile',
    'linkedin',
    '[{"type":"title","value":"Chief Marketing Officer"}, {"type":"nameAlias","value":"Maria R."}]'::JSONB,
    '[{"type":"industry","value":"Financial Services"}]'::JSONB
);
```

### With Relationship Metadata
```sql
SELECT propose_fact(
    'Person', 'Jennifer White',
    'Company', 'TechCorp Industries',
    'Legal_Counsel',
    'Signed Retainer Agreement', 
    'contracts',
    '[]'::JSONB, '[]'::JSONB,
    0.95,  -- High confidence relationship
    '2024-01-15'::DATE,  -- Valid from
    '2025-01-15'::DATE,  -- Valid to
    '{"hourly_rate": 450, "practice_area": "Corporate Law"}'::JSONB
);
```

## Response Status Codes

### Success
- **Status**: 'success'
- **Description**: All operations completed successfully
- **Confidence**: > 0.0

### Conflicts  
- **Status**: 'conflicts'
- **Description**: Conflicting relationships detected but data still processed
- **Conflicts Array**: Details of detected conflicts

### Error
- **Status**: 'error' 
- **Description**: System error occurred
- **Actions**: Contains error message
- **Confidence**: 0.0

## Integration Considerations

### Batch Processing
- Each call processes one fact (2 entities + 1 relationship)
- For bulk imports, call multiple times with different facts
- Consider transaction boundaries for related facts

### Confidence Thresholds
- Entity matching threshold: 0.8 (configurable)
- Ambiguous matching: 0.5 - 0.8 
- Relationship conflict penalty: 0.7x original confidence

### Performance
- Entity resolution queries are optimized with indexes
- Fuzzy matching is expensive - only used when exact/alias matching fails
- Consider caching frequently matched entities

### Data Quality
- Higher source type reliability improves overall confidence
- Multiple sources for same entity increases confidence
- Regular review of low-confidence data recommended

## Provenance and Audit Trail

Every fact ingested through the Propose API automatically creates:

1. **Provenance Records**: Source attribution for all entities and relationships
2. **Change History**: Audit trail of all modifications
3. **Confidence Tracking**: Quality metrics for all data

Query provenance:
```sql
SELECT p.*, st.description as source_description
FROM provenance p
JOIN source_types st ON p.source_type = st.source_type
WHERE p.asset_id = 'YOUR_ENTITY_ULID';
```

## Error Handling

The API uses PostgreSQL's exception handling:
- Validation errors return structured error responses
- System errors are caught and returned as error status
- All operations are wrapped in transactions for consistency

## Database Dependencies

Required database components:
- Core schema: nodes, relationships, attributes tables
- Provenance system: provenance, source_types, change_history tables  
- ULID generation: generate_ulid() function
- Name normalization: normalize_name() function
- Extensions: pg_trgm for fuzzy matching

---

For implementation examples and test cases, see:
- `scripts/propose_api_demo.sql` - Comprehensive test scenarios
- `scripts/provenance_demo.sql` - Provenance system examples