# Product Requirements Document (PRD)
# Aurora PostgreSQL Graph Database for Law Firm Conflict Checking

## 1. Executive Summary

### 1.1 Product Vision
Build a general-purpose graph database on AWS Aurora PostgreSQL to track relationships between people, companies, places, things, and events. The primary use case is law firm conflict-of-interest checking, with extensibility for future business domains.

### 1.2 Key Objectives
- Process individual entity/relationship records via Cloudflare Worker queue system
- Enable real-time conflict checking for law firm client intake (2-3 degree traversal)
- Support law firm data uploads through dedicated APIs
- Deliver low-latency global access via Cloudflare Workers and Hyperdrive
- Support regulatory compliance through comprehensive audit logging
- Pre-compute conflict matrices for performance optimization

### 1.3 Success Metrics
- Sub-200ms conflict check response times globally
- 99.9% uptime for database operations
- Zero false negatives in conflict detection
- Support for 100K+ entities with complex relationship networks

## 2. Product Overview

### 2.1 Core Functionality
The system provides a graph database that:
- Stores entities (nodes) and their relationships (edges)
- Handles entity aliases and metadata through attributes
- Performs conflict checks by traversing relationship networks
- Maintains audit trails for regulatory compliance

### 2.2 Architecture Components
- **Database**: AWS Aurora PostgreSQL cluster with pre-computed conflict matrices
- **Queue System**: Cloudflare Queues for individual record processing
- **API Layer**: Cloudflare Workers with Hyperdrive connection pooling
- **Upload APIs**: RESTful endpoints for law firm data ingestion
- **Infrastructure**: CloudFormation templates with Secrets Manager
- **Schema Management**: Flyway migrations (recommended for PostgreSQL compatibility)
- **Connection Pooling**: Hyperdrive with optimized settings for graph queries

## 3. Functional Requirements

### 3.1 Entity Management
- **FR-001**: System shall support five entity types: Person, Company, Place, Thing, Event
- **FR-002**: Each entity shall have a unique identifier (UUID) and primary name
- **FR-003**: System shall support unlimited aliases per entity via attributes table
- **FR-004**: Entity names shall be stored in raw format for display, normalized for searches

### 3.2 Relationship Management
- **FR-005**: System shall support directed relationships between any two entities
- **FR-006**: Relationships shall have configurable types: Employment, Ownership, Location, Participation, Organizer, Conflict
- **FR-007**: System shall maintain relationship history with timestamps

### 3.3 Conflict Checking
- **FR-008**: System shall identify potential conflicts by traversing 2-3 degrees of relationships
- **FR-009**: Conflict checks shall include all entity aliases in search criteria
- **FR-010**: System shall normalize names (case, whitespace) to prevent false negatives
- **FR-011**: System shall pre-compute conflict matrices for performance optimization
- **FR-012**: Conflict check results shall be logged with full audit trail
- **FR-013**: System shall detect indirect conflicts (e.g., client company → employee → opponent)

### 3.4 Queue Processing
- **FR-014**: Worker shall process individual records from Cloudflare Queues
- **FR-015**: Queue processing shall handle entity creation, updates, and relationship management
- **FR-016**: Worker shall support future external data ingestion integrations
- **FR-017**: Failed queue messages shall be retried with exponential backoff

### 3.5 Upload API Requirements
- **FR-018**: System shall provide RESTful upload APIs for law firm data ingestion
- **FR-019**: Upload APIs shall support bulk entity and relationship creation
- **FR-020**: Upload APIs shall validate data integrity before processing
- **FR-021**: Upload APIs shall provide progress tracking for large uploads

### 3.6 General API Requirements
- **FR-022**: System shall provide RESTful API with standard HTTP response codes
- **FR-023**: API shall support authentication via managed API keys
- **FR-024**: System shall return structured conflict check results with entity details

## 4. Non-Functional Requirements

### 4.1 Performance
- **NFR-001**: Conflict checks shall complete within 200ms for 95% of requests
- **NFR-002**: System shall support 1000+ concurrent users
- **NFR-003**: Database queries shall leverage optimized indexing strategy

### 4.2 Availability
- **NFR-004**: System shall maintain 99.9% uptime
- **NFR-005**: Aurora cluster shall use Multi-AZ deployment for failover
- **NFR-006**: Cloudflare Workers shall provide global edge access

### 4.3 Security
- **NFR-007**: All database credentials shall be stored in AWS Secrets Manager
- **NFR-008**: API keys shall be managed and revocable through Secrets Manager
- **NFR-009**: Database connections shall be encrypted in transit and at rest

### 4.4 Scalability
- **NFR-010**: System shall handle 100K+ entities with complex relationship networks
- **NFR-011**: Schema shall support extensible entity and relationship types
- **NFR-012**: Database shall use partitioning strategies as data volume grows
- **NFR-013**: Hyperdrive shall maintain <50ms connection establishment time
- **NFR-014**: Schema migrations shall complete with zero downtime using blue/green deployments

## 5. Technical Specifications

### 5.1 Database Schema
```sql
-- Core entity storage
CREATE TABLE nodes (
  node_id UUID PRIMARY KEY,
  node_type VARCHAR(50) NOT NULL,
  primary_name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Relationship edges
CREATE TABLE relationships (
  relationship_id UUID PRIMARY KEY,
  source_node_id UUID REFERENCES nodes(node_id) ON DELETE CASCADE,
  target_node_id UUID REFERENCES nodes(node_id) ON DELETE CASCADE,
  relationship_type VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Entity metadata and aliases
CREATE TABLE attributes (
  attribute_id UUID PRIMARY KEY,
  node_id UUID REFERENCES nodes(node_id) ON DELETE CASCADE,
  attribute_type VARCHAR(50) NOT NULL,
  attribute_value VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 5.2 Indexing Strategy
- Primary keys on all tables
- Composite index: `attributes(node_id, attribute_type, attribute_value)`
- Partial index: `attributes(attribute_value) WHERE attribute_type = 'nameAlias'`
- Composite index: `relationships(source_node_id, target_node_id, relationship_type)`
- Unique index: `nodes(primary_name)` if business rules require

### 5.3 Schema Migration Strategy (Flyway Recommended)

**Why Flyway over Liquibase:**
- Better PostgreSQL native support and performance
- Simpler SQL-based migrations (easier to review)
- Excellent Aurora PostgreSQL compatibility
- Strong rollback capabilities with commercial version

**Migration Approach:**
```
# Directory Structure
db/migration/
├── V1__initial_schema.sql
├── V2__add_indexes.sql
├── V3__add_conflict_matrices.sql
└── R__rebuild_conflict_view.sql  # Repeatable

# Environment Promotion
dev → staging → production
- Automated via CI/CD pipeline
- Validation testing on staging replica
- Zero-downtime using Aurora blue/green deployments
```

**Rollback Strategy:**
- Flyway Pro: Automatic rollback scripts
- Community: Manual rollback procedures documented per migration
- Database snapshots before major schema changes
- Feature flags for application-level rollbacks

### 5.4 Hyperdrive Configuration Best Practices

```toml
# wrangler.toml Hyperdrive settings
[[hyperdrive]]
binding = "GRAPH_DB"
id = "your-hyperdrive-id"
max_age = 60  # Cache for 60 seconds (conflict checks)

[[hyperdrive]]
binding = "GRAPH_DB_READONLY" 
id = "your-readonly-hyperdrive-id"
max_age = 300  # Cache for 5 minutes (entity lookups)
```

**Connection Pool Settings:**
- Primary DB: 20-50 connections (writes + conflict checks)
- Read Replica: 10-20 connections (cached lookups)
- Connection timeout: 30 seconds
- Query timeout: 10 seconds for conflict checks

**Cache Strategy:**
- Entity lookups: 5-minute TTL (relatively static)
- Conflict checks: 1-minute TTL (needs freshness)
- Relationship queries: 2-minute TTL (moderate change frequency)
- Pre-computed matrices: 10-minute TTL (updated via queue)

**Regional Distribution:**
- Primary: us-east-1 (Aurora cluster location)
- Read replicas: us-west-2, eu-west-1 (if global users)
- Hyperdrive automatically routes to nearest region

### 5.5 API Response Codes
- `200 OK`: Successful request
- `400 Bad Request`: Validation error
- `401 Unauthorized`: Authentication failure
- `409 Conflict`: Duplicate entity
- `422 Unprocessable Entity`: Data validation error
- `500 Internal Server Error`: System error

## 6. Law Firm Use Case Details

### 6.1 Entity Mapping for Legal Context

| Entity Type | Legal Categories | Examples |
|-------------|------------------|----------|
| Person | Attorney, Client, Staff, Opponent | John Smith (Client), Jane Doe (Opposing Counsel) |
| Company | Law Firm, Corporation, Vendor | ABC Corp (Client), XYZ LLC (Counterparty) |
| Place | Court, Office, Property | Superior Court, Client Office |
| Thing | Document, Asset, Vehicle | Contract, Intellectual Property |
| Event | Case, Meeting, Filing | Initial Consultation, Deposition |

### 6.2 Conflict Check Workflow
1. New matter intake captures all parties and counterparties
2. System creates entities and relationships for all participants
3. Conflict check queries traverse relationship graph for existing connections
4. Results highlight potential conflicts with existing clients/matters
5. Audit log records all conflict checks for regulatory compliance

## 7. API Specifications

### 7.1 Law Firm Upload APIs

#### Bulk Entity Upload
```
POST /api/v1/entities/bulk
Content-Type: application/json
Authorization: Bearer {api_key}

{
  "entities": [
    {
      "node_type": "Person",
      "primary_name": "John Smith",
      "attributes": [
        {"type": "nameAlias", "value": "J. Smith"},
        {"type": "email", "value": "john.smith@example.com"}
      ]
    },
    {
      "node_type": "Company", 
      "primary_name": "ACME Corporation",
      "attributes": [
        {"type": "nameAlias", "value": "ACME Corp"}
      ]
    }
  ]
}
```

#### Bulk Relationship Upload
```
POST /api/v1/relationships/bulk
Content-Type: application/json
Authorization: Bearer {api_key}

{
  "relationships": [
    {
      "source_entity_name": "John Smith",
      "target_entity_name": "ACME Corporation",
      "relationship_type": "Employment"
    }
  ]
}
```

#### Upload Progress Tracking
```
GET /api/v1/uploads/{upload_id}/status
Authorization: Bearer {api_key}

Response:
{
  "upload_id": "uuid",
  "status": "processing|completed|failed",
  "progress": {
    "entities_processed": 150,
    "entities_total": 200,
    "relationships_processed": 75,
    "relationships_total": 100
  },
  "errors": []
}
```

### 7.2 Conflict Check API
```
POST /api/v1/conflicts/check
Content-Type: application/json
Authorization: Bearer {api_key}

{
  "entities": ["John Smith", "ACME Corporation"],
  "traverse_depth": 3,
  "matter_id": "optional-matter-uuid"
}

Response:
{
  "conflicts_found": true,
  "conflicts": [
    {
      "entity": "John Smith",
      "conflict_path": [
        {"entity": "John Smith", "relationship": "Employment"},
        {"entity": "ACME Corporation", "relationship": "Conflict"}
      ],
      "severity": "high|medium|low"
    }
  ]
}
```

## 8. Queue Processing Architecture

### 8.1 Queue Message Types
```typescript
// Entity Processing Message
interface EntityMessage {
  type: 'entity_create' | 'entity_update' | 'entity_delete';
  entity_id?: string;
  node_type: string;
  primary_name: string;
  attributes?: Array<{type: string, value: string}>;
}

// Relationship Processing Message  
interface RelationshipMessage {
  type: 'relationship_create' | 'relationship_delete';
  source_entity_id: string;
  target_entity_id: string;
  relationship_type: string;
}

// Conflict Matrix Update Message
interface ConflictUpdateMessage {
  type: 'conflict_matrix_update';
  entity_ids: string[];
  operation: 'recalculate' | 'invalidate';
}
```

### 8.2 Worker Processing Logic
- Process messages individually (not in batches)
- Validate entity/relationship data before database operations
- Update pre-computed conflict matrices when relationships change
- Retry failed messages with exponential backoff (max 3 attempts)
- Log all processing activities for audit trail

## 9. Implementation Phases

### 9.1 Phase 1: Core Infrastructure (MVP)
- CloudFormation deployment of Aurora cluster
- Basic schema implementation with core tables
- Cloudflare Worker setup with Hyperdrive integration
- Basic CRUD operations for entities and relationships

### 9.2 Phase 2: Conflict Checking Engine
- Advanced query engine for relationship traversal
- Alias normalization and matching logic
- Conflict detection algorithms
- Audit logging implementation

### 9.3 Phase 3: Production Readiness
- Performance optimization and indexing
- Comprehensive error handling
- Security hardening
- Monitoring and alerting

### 9.4 Phase 4: Advanced Features
- Full-text search capabilities
- Vector indexing with pgvector for semantic search
- Advanced reporting and analytics
- Additional entity/relationship types

## 10. Risk Assessment

### 10.1 Technical Risks
- **High**: Complex relationship queries may impact performance at scale
- **Medium**: Schema migrations in production environment
- **Low**: Cloudflare Workers cold start latency

### 10.2 Business Risks
- **High**: False negatives in conflict detection causing legal compliance issues
- **Medium**: Data migration from existing law firm systems
- **Low**: Integration complexity with existing law firm workflows

## 11. Success Criteria

### 11.1 Launch Criteria
- All functional requirements implemented and tested
- Performance benchmarks met (200ms response time)
- Security audit completed
- Regulatory compliance validation

### 11.2 Post-Launch Metrics
- Conflict check accuracy (zero false negatives target)
- System uptime (99.9% target)
- User adoption rate
- Query performance under load

## 12. Future Considerations

### 12.1 Extensibility
- Support for additional practice areas beyond conflict checking
- Integration with legal case management systems
- International law firm requirements
- Regulatory compliance for different jurisdictions

### 12.2 Advanced Features
- Machine learning for intelligent conflict prediction
- Natural language processing for document entity extraction
- Real-time collaboration features
- Mobile access optimization