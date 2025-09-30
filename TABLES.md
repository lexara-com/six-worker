# Database Table Structures

## Core Graph Database Tables

### 📊 `nodes` - Core Entities
Stores all entities in the graph (Person, Company, Place, Thing, Event)

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | UUID | Primary key (auto-generated) |
| `node_type` | VARCHAR(50) | Entity type: Person, Company, Place, Thing, Event |
| `primary_name` | VARCHAR(255) | Display name (raw format) |
| `normalized_name` | VARCHAR(255) | Lowercase, trimmed for searching (auto-generated) |
| `status` | VARCHAR(20) | active, inactive, deleted (default: active) |
| `created_at` | TIMESTAMP | Record creation time |
| `updated_at` | TIMESTAMP | Last update time (auto-updated) |
| `created_by` | VARCHAR(100) | User/system that created record |

**Example Data:**
```sql
node_id: 22222222-2222-2222-2222-222222222221
node_type: Person
primary_name: John Smith
normalized_name: john smith
status: active
```

---

### 🔗 `relationships` - Graph Edges
Directed relationships between any two nodes

| Column | Type | Description |
|--------|------|-------------|
| `relationship_id` | UUID | Primary key (auto-generated) |
| `source_node_id` | UUID | FK to nodes (relationship origin) |
| `target_node_id` | UUID | FK to nodes (relationship target) |
| `relationship_type` | VARCHAR(50) | Employment, Ownership, Legal_Counsel, etc. |
| `strength` | DECIMAL(3,2) | Relationship strength (0.0-1.0, default: 1.0) |
| `status` | VARCHAR(20) | active, inactive, deleted |
| `valid_from` | DATE | When relationship started |
| `valid_to` | DATE | When relationship ended (NULL = ongoing) |
| `created_at` | TIMESTAMP | Record creation |
| `updated_at` | TIMESTAMP | Last update |
| `created_by` | VARCHAR(100) | Creator |
| `metadata` | JSONB | Additional structured data |

**Example Data:**
```sql
source_node_id: 22222222-2222-2222-2222-222222222221 (John Smith)
target_node_id: 33333333-3333-3333-3333-333333333331 (ACME Corp)
relationship_type: Legal_Counsel
strength: 1.0
```

---

### 🏷️ `attributes` - Entity Metadata & Aliases
Stores aliases, metadata, and supplemental information

| Column | Type | Description |
|--------|------|-------------|
| `attribute_id` | UUID | Primary key (auto-generated) |
| `node_id` | UUID | FK to nodes |
| `attribute_type` | VARCHAR(50) | nameAlias, email, phone, title, etc. |
| `attribute_value` | VARCHAR(500) | The actual value |
| `normalized_value` | VARCHAR(500) | Normalized for searching (auto-generated) |
| `confidence` | DECIMAL(3,2) | Data confidence (0.0-1.0, default: 1.0) |
| `source` | VARCHAR(100) | Where this data came from |
| `status` | VARCHAR(20) | active, inactive, deleted |
| `created_at` | TIMESTAMP | Record creation |
| `updated_at` | TIMESTAMP | Last update |
| `created_by` | VARCHAR(100) | Creator |

**Example Data:**
```sql
node_id: 22222222-2222-2222-2222-222222222221 (John Smith)
attribute_type: nameAlias
attribute_value: J. Smith
normalized_value: j. smith
```

---

## Conflict Detection Tables

### ⚡ `conflict_matrix` - Pre-computed Conflicts
High-performance conflict detection through pre-computation

| Column | Type | Description |
|--------|------|-------------|
| `matrix_id` | UUID | Primary key |
| `entity_a_id` | UUID | FK to nodes (first entity) |
| `entity_b_id` | UUID | FK to nodes (second entity) |
| `conflict_type` | VARCHAR(50) | Type of conflict detected |
| `conflict_path` | JSONB | Array of relationship steps |
| `conflict_strength` | DECIMAL(3,2) | Severity of conflict (0.0-1.0) |
| `degrees_of_separation` | INTEGER | Path length (1-3) |
| `computed_at` | TIMESTAMP | When computed |
| `expires_at` | TIMESTAMP | Cache expiration (NULL = no expiry) |

**Example Data:**
```sql
entity_a_id: ACME Corp
entity_b_id: TechCorp Industries  
conflict_type: Legal_Counsel_Conflict
conflict_path: [{"entity":"ACME Corporation","relationship":"Legal_Counsel"}, 
               {"entity":"Mary Johnson","relationship":"Legal_Counsel"}, 
               {"entity":"TechCorp Industries"}]
degrees_of_separation: 2
```

---

### 📋 `conflict_checks` - Audit Trail
Complete audit log of all conflict analysis requests

| Column | Type | Description |
|--------|------|-------------|
| `check_id` | UUID | Primary key |
| `matter_id` | VARCHAR(100) | External matter/case reference |
| `checked_entities` | JSONB | Array of entity names checked |
| `conflicts_found` | JSONB | Array of conflicts detected |
| `check_parameters` | JSONB | Search parameters used |
| `execution_time_ms` | INTEGER | Query performance metric |
| `checked_at` | TIMESTAMP | When check was performed |
| `checked_by` | VARCHAR(100) | User who ran check |
| `api_key_id` | VARCHAR(100) | API key used |

---

## Reference Data Tables

### 🔧 `relationship_types` - Valid Relationship Types
Defines allowed relationship types and their properties

| Column | Type | Description |
|--------|------|-------------|
| `type_name` | VARCHAR(50) | Primary key (Employment, Ownership, etc.) |
| `description` | TEXT | Human-readable description |
| `category` | VARCHAR(50) | Professional, Legal, Personal, etc. |
| `is_bidirectional` | BOOLEAN | Whether relationship goes both ways |
| `created_at` | TIMESTAMP | When type was defined |

**Predefined Types:**
- `Employment` - Person works for Company
- `Ownership` - Entity owns another entity  
- `Legal_Counsel` - Attorney represents Entity
- `Conflict` - Adversarial relationship
- `Family` - Family relationship
- `Partnership` - Business partnership

---

### 🏷️ `attribute_types` - Valid Attribute Types
Defines allowed attribute types and validation rules

| Column | Type | Description |
|--------|------|-------------|
| `type_name` | VARCHAR(50) | Primary key (nameAlias, email, etc.) |
| `description` | TEXT | Human-readable description |
| `data_type` | VARCHAR(20) | text, number, date, boolean, json |
| `is_searchable` | BOOLEAN | Whether to index for searches |
| `created_at` | TIMESTAMP | When type was defined |

**Predefined Types:**
- `nameAlias` - Alternative names and nicknames
- `email` - Email address
- `phone` - Phone number
- `title` - Professional title or role
- `category` - Entity classification

---

## Performance Views

### 📊 `mv_entity_summary` - Materialized View
Pre-computed entity statistics for performance

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | UUID | Entity ID |
| `node_type` | VARCHAR(50) | Entity type |
| `primary_name` | VARCHAR(255) | Entity name |
| `normalized_name` | VARCHAR(255) | Normalized name |
| `created_at` | TIMESTAMP | Creation time |
| `outbound_relationships` | BIGINT | Number of relationships where this is source |
| `inbound_relationships` | BIGINT | Number of relationships where this is target |
| `attribute_count` | BIGINT | Total attributes |
| `alias_count` | BIGINT | Number of name aliases |

---

### 🔍 `v_entity_with_aliases` - View
Easy lookup of entities with all their aliases

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | UUID | Entity ID |
| `node_type` | VARCHAR(50) | Entity type |
| `primary_name` | VARCHAR(255) | Primary name |
| `normalized_name` | VARCHAR(255) | Normalized name |
| `aliases` | VARCHAR[] | Array of all aliases |
| `created_at` | TIMESTAMP | Creation time |

---

## Key Functions

### 🔧 Core Helper Functions

#### `normalize_name(input_name TEXT) → TEXT`
Normalizes names for consistent searching (lowercase, trim, compress whitespace)

#### `comprehensive_conflict_check(entity_names VARCHAR[], matter_description VARCHAR) → TABLE`
Main conflict detection function that:
- Resolves all aliases for input entities
- Checks pre-computed conflict matrix
- Performs dynamic relationship traversal
- Returns structured conflict results
- Logs audit trail

#### `find_conflict_paths(entity_name VARCHAR, max_degrees INTEGER) → TABLE`
Recursive function to find all relationship paths up to specified degrees of separation

#### `get_all_entity_names(entity_name VARCHAR) → TABLE`
Resolves all possible names (primary + aliases) for an entity

---

## Performance Optimizations

### 🚀 Key Indexes
- **Name lookups**: `idx_nodes_normalized_name`, `idx_attributes_name_aliases`
- **Graph traversal**: `idx_relationships_source_type`, `idx_relationships_target_type`
- **Fuzzy matching**: GIN indexes with trigram support
- **Conflict detection**: `idx_conflict_matrix_entity_a`, `idx_conflict_matrix_entity_b`

### ⚡ Performance Features
- **Automatic triggers** for name normalization
- **Partial indexes** for active records only
- **Materialized views** for common aggregations
- **Connection pooling** via Hyperdrive
- **Pre-computed conflict matrices** for sub-200ms response times

## Sample Data Relationships

The test data creates this relationship network:

```
Law Firm: Smith & Associates
├── John Smith (Partner) → Legal_Counsel → ACME Corp
├── Mary Johnson (Partner) → Legal_Counsel → ACME Corp + TechCorp (CONFLICT!)
└── David Wilson (Associate)

ACME Corporation
├── Robert Brown (CEO)
├── Lisa Anderson (CFO)  
└── Michael Taylor (VP Engineering)

Family Conflict Chain:
Amanda Brown → Family → Robert Brown → Employment → ACME Corp → Legal_Counsel → John Smith

Historical Conflict:
Kevin Miller → Employment (expired) → ACME Corp
              → Employment (current) → TechCorp
```

This structure enables testing of:
- ✅ Direct representation conflicts
- ✅ 2-3 degree relationship conflicts  
- ✅ Family/business connection conflicts
- ✅ Historical relationship conflicts
- ✅ Alias-based conflict detection