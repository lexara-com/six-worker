# Six Worker - Aurora PostgreSQL Graph Database

A Cloudflare Worker-based system for law firm conflict checking using Aurora PostgreSQL as a graph database.

## Project Structure

```
six_worker/
├── PRD.md                          # Product Requirements Document
├── db/
│   ├── migrations/
│   │   ├── V1__initial_schema.sql       # Core schema with nodes, relationships, attributes
│   │   └── V2__indexes_and_performance.sql  # Optimized indexes and views
│   ├── test-data/
│   │   └── insert_test_data.sql         # Comprehensive test scenarios
│   └── test-scripts/
│       ├── conflict_check_tests.sql     # Conflict detection test cases
│       └── performance_tests.sql        # Performance benchmarking
├── src/
│   └── worker.py                   # Cloudflare Worker (queue processing)
├── wrangler.toml                   # Cloudflare configuration
└── requirements.txt                # Python dependencies
```

## Database Schema Overview

### Core Tables
- **`nodes`**: Entities (Person, Company, Place, Thing, Event)
- **`relationships`**: Directed edges with types and strength
- **`attributes`**: Metadata, aliases, and supplemental data
- **`conflict_matrix`**: Pre-computed conflicts for performance
- **`conflict_checks`**: Audit trail for all conflict queries

### Key Features
- **Automatic normalization** via triggers for consistent searching
- **Alias support** through attributes table (handles "J. Smith" = "John Smith")
- **Fuzzy matching** using PostgreSQL trigram indexes
- **2-3 degree relationship traversal** for complex conflict detection
- **Pre-computed conflict matrices** for sub-200ms response times

## Test Data Scenarios

The test data includes comprehensive conflict scenarios:

1. **Direct Representation Conflict**: Same attorney representing competing companies
2. **Family Business Conflict**: Family member suing company where relative works
3. **Historical Relationships**: Former employees who moved to competitors
4. **Subsidiary Relationships**: Complex corporate ownership structures
5. **Alias-based Detection**: Conflicts found through alternate names

## Performance Optimizations

- **Composite indexes** for fast graph traversal
- **Partial indexes** for active records only
- **GIN indexes** for fuzzy text matching
- **Materialized views** for entity summaries
- **Connection pooling** via Hyperdrive

## Getting Started

### 1. Database Setup

```bash
# Run migrations (requires PostgreSQL connection)
psql -h your-aurora-endpoint -U username -d dbname -f db/migrations/V1__initial_schema.sql
psql -h your-aurora-endpoint -U username -d dbname -f db/migrations/V2__indexes_and_performance.sql

# Insert test data
psql -h your-aurora-endpoint -U username -d dbname -f db/test-data/insert_test_data.sql
```

### 2. Run Tests

```bash
# Conflict detection tests
psql -h your-aurora-endpoint -U username -d dbname -f db/test-scripts/conflict_check_tests.sql

# Performance benchmarks
psql -h your-aurora-endpoint -U username -d dbname -f db/test-scripts/performance_tests.sql
```

### 3. Cloudflare Worker Setup

```bash
# Deploy worker (requires wrangler CLI)
wrangler deploy
```

## Key Functions

### `comprehensive_conflict_check(entity_names, matter_description)`
Main conflict detection function that:
- Resolves all entity aliases
- Checks pre-computed conflict matrix
- Performs dynamic relationship traversal
- Returns structured conflict results
- Maintains audit trail

### `find_conflict_paths(entity_name, max_degrees)`
Recursive function to find all relationship paths up to 3 degrees of separation.

### `get_all_entity_names(entity_name)`
Resolves entity aliases for comprehensive name matching.

## API Endpoints (Planned)

- `POST /api/v1/conflicts/check` - Run conflict analysis
- `POST /api/v1/entities/bulk` - Bulk entity upload
- `POST /api/v1/relationships/bulk` - Bulk relationship upload
- `GET /api/v1/uploads/{id}/status` - Track upload progress

## AWS Configuration

Current setup uses AWS profile "lexara_super_agent" with:
- Account: 492149691043
- Full AdministratorAccess permissions
- Aurora PostgreSQL cluster deployment ready

## Performance Targets

- **Conflict checks**: <200ms response time
- **Entity resolution**: <50ms for name/alias lookup
- **Relationship traversal**: <100ms for 3-degree paths
- **Bulk uploads**: 1000+ entities/second processing

## Next Steps

1. Deploy Aurora PostgreSQL cluster via CloudFormation
2. Set up Hyperdrive connection pooling
3. Implement Cloudflare Worker queue processing
4. Add comprehensive API layer
5. Set up monitoring and alerting