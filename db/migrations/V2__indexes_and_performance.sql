-- =============================================
-- Performance Indexes and Optimization
-- Version: 2.0
-- Purpose: Indexes for high-performance graph traversal and conflict checking
-- =============================================

-- =============================================
-- Primary Search Indexes
-- =============================================

-- Nodes table indexes
CREATE INDEX CONCURRENTLY idx_nodes_type_status ON nodes(node_type, status);
CREATE INDEX CONCURRENTLY idx_nodes_normalized_name ON nodes(normalized_name);
CREATE INDEX CONCURRENTLY idx_nodes_primary_name_gin ON nodes USING gin(primary_name gin_trgm_ops);
CREATE INDEX CONCURRENTLY idx_nodes_created_at ON nodes(created_at);
CREATE INDEX CONCURRENTLY idx_nodes_updated_at ON nodes(updated_at);

-- Unique constraint on primary_name per type (optional business rule)
-- CREATE UNIQUE INDEX CONCURRENTLY idx_nodes_unique_name_type ON nodes(normalized_name, node_type) WHERE status = 'active';

-- =============================================
-- Relationship Traversal Indexes
-- =============================================

-- Core relationship indexes for graph traversal
CREATE INDEX CONCURRENTLY idx_relationships_source_type ON relationships(source_node_id, relationship_type, status);
CREATE INDEX CONCURRENTLY idx_relationships_target_type ON relationships(target_node_id, relationship_type, status);
CREATE INDEX CONCURRENTLY idx_relationships_source_target ON relationships(source_node_id, target_node_id, status);

-- Reverse traversal index
CREATE INDEX CONCURRENTLY idx_relationships_target_source ON relationships(target_node_id, source_node_id, status);

-- Index for relationship type filtering
CREATE INDEX CONCURRENTLY idx_relationships_type_status ON relationships(relationship_type, status);

-- Index for time-based relationship queries
CREATE INDEX CONCURRENTLY idx_relationships_valid_period ON relationships(valid_from, valid_to) WHERE status = 'active';

-- Composite index for complex traversal queries
CREATE INDEX CONCURRENTLY idx_relationships_full_traversal ON relationships(source_node_id, relationship_type, target_node_id, status, strength);

-- =============================================
-- Attribute Search Indexes
-- =============================================

-- Primary attribute lookup indexes
CREATE INDEX CONCURRENTLY idx_attributes_node_type ON attributes(node_id, attribute_type, status);
CREATE INDEX CONCURRENTLY idx_attributes_type_value ON attributes(attribute_type, normalized_value, status);

-- Specialized index for name aliases (most common lookup)
CREATE INDEX CONCURRENTLY idx_attributes_name_aliases ON attributes(normalized_value, status) WHERE attribute_type = 'nameAlias';

-- GIN index for fuzzy matching on attribute values
CREATE INDEX CONCURRENTLY idx_attributes_value_gin ON attributes USING gin(normalized_value gin_trgm_ops);

-- Index for confidence-based queries
CREATE INDEX CONCURRENTLY idx_attributes_confidence ON attributes(attribute_type, confidence, status) WHERE confidence >= 0.8;

-- =============================================
-- Conflict Matrix Indexes
-- =============================================

-- Primary conflict lookup indexes
CREATE INDEX CONCURRENTLY idx_conflict_matrix_entity_a ON conflict_matrix(entity_a_id, conflict_type);
CREATE INDEX CONCURRENTLY idx_conflict_matrix_entity_b ON conflict_matrix(entity_b_id, conflict_type);

-- Bidirectional conflict lookup
CREATE INDEX CONCURRENTLY idx_conflict_matrix_bidirectional ON conflict_matrix(LEAST(entity_a_id, entity_b_id), GREATEST(entity_a_id, entity_b_id), conflict_type);

-- Index for cache management
CREATE INDEX CONCURRENTLY idx_conflict_matrix_expires ON conflict_matrix(expires_at) WHERE expires_at IS NOT NULL;

-- Index for conflict strength filtering
CREATE INDEX CONCURRENTLY idx_conflict_matrix_strength ON conflict_matrix(conflict_strength DESC, degrees_of_separation ASC);

-- =============================================
-- Audit and Logging Indexes
-- =============================================

-- Conflict checks audit indexes
CREATE INDEX CONCURRENTLY idx_conflict_checks_matter ON conflict_checks(matter_id, checked_at);
CREATE INDEX CONCURRENTLY idx_conflict_checks_timestamp ON conflict_checks(checked_at DESC);
CREATE INDEX CONCURRENTLY idx_conflict_checks_api_key ON conflict_checks(api_key_id, checked_at);

-- GIN index for searching within checked entities JSON
CREATE INDEX CONCURRENTLY idx_conflict_checks_entities_gin ON conflict_checks USING gin(checked_entities);

-- =============================================
-- Partial Indexes for Performance
-- =============================================

-- Only index active records for most queries
CREATE INDEX CONCURRENTLY idx_nodes_active_type ON nodes(node_type, primary_name) WHERE status = 'active';
CREATE INDEX CONCURRENTLY idx_relationships_active ON relationships(source_node_id, target_node_id, relationship_type) WHERE status = 'active';
CREATE INDEX CONCURRENTLY idx_attributes_active ON attributes(node_id, attribute_type, normalized_value) WHERE status = 'active';

-- Index only recent conflict checks (last 2 years)
CREATE INDEX CONCURRENTLY idx_conflict_checks_recent ON conflict_checks(checked_at, matter_id) 
WHERE checked_at > CURRENT_DATE - INTERVAL '2 years';

-- =============================================
-- Materialized Views for Performance
-- =============================================

-- Materialized view for entity summary with attribute counts
CREATE MATERIALIZED VIEW mv_entity_summary AS
SELECT 
    n.node_id,
    n.node_type,
    n.primary_name,
    n.normalized_name,
    n.created_at,
    COUNT(DISTINCT r1.relationship_id) as outbound_relationships,
    COUNT(DISTINCT r2.relationship_id) as inbound_relationships,
    COUNT(DISTINCT a.attribute_id) as attribute_count,
    COUNT(DISTINCT CASE WHEN a.attribute_type = 'nameAlias' THEN a.attribute_id END) as alias_count
FROM nodes n
    LEFT JOIN relationships r1 ON n.node_id = r1.source_node_id AND r1.status = 'active'
    LEFT JOIN relationships r2 ON n.node_id = r2.target_node_id AND r2.status = 'active'  
    LEFT JOIN attributes a ON n.node_id = a.node_id AND a.status = 'active'
WHERE n.status = 'active'
GROUP BY n.node_id, n.node_type, n.primary_name, n.normalized_name, n.created_at;

-- Index on the materialized view
CREATE INDEX idx_mv_entity_summary_type ON mv_entity_summary(node_type, normalized_name);
CREATE INDEX idx_mv_entity_summary_relationships ON mv_entity_summary(outbound_relationships + inbound_relationships DESC);

-- =============================================
-- Database Statistics and Maintenance
-- =============================================

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_entity_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entity_summary;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- Query Performance Analysis Views
-- =============================================

-- View for relationship path analysis
CREATE VIEW v_relationship_paths AS
SELECT 
    r.source_node_id,
    s.primary_name as source_name,
    r.relationship_type,
    r.target_node_id,
    t.primary_name as target_name,
    r.strength,
    r.valid_from,
    r.valid_to
FROM relationships r
    JOIN nodes s ON r.source_node_id = s.node_id
    JOIN nodes t ON r.target_node_id = t.node_id  
WHERE r.status = 'active' 
    AND s.status = 'active' 
    AND t.status = 'active';

-- View for entity with all aliases
CREATE VIEW v_entity_with_aliases AS
SELECT 
    n.node_id,
    n.node_type,
    n.primary_name,
    n.normalized_name,
    COALESCE(
        ARRAY_AGG(DISTINCT a.attribute_value ORDER BY a.attribute_value) 
        FILTER (WHERE a.attribute_type = 'nameAlias' AND a.status = 'active'), 
        ARRAY[]::VARCHAR[]
    ) as aliases,
    n.created_at
FROM nodes n
    LEFT JOIN attributes a ON n.node_id = a.node_id 
WHERE n.status = 'active'
GROUP BY n.node_id, n.node_type, n.primary_name, n.normalized_name, n.created_at;

-- =============================================
-- Index Usage Monitoring
-- =============================================

-- Function to check index usage statistics
CREATE OR REPLACE FUNCTION index_usage_stats()
RETURNS TABLE(
    schemaname text,
    tablename text,
    indexname text,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schemaname::text,
        s.tablename::text, 
        s.indexname::text,
        s.idx_scan,
        s.idx_tup_read,
        s.idx_tup_fetch
    FROM pg_stat_user_indexes s
    WHERE s.schemaname = 'public'
    ORDER BY s.idx_scan DESC;
END;
$$ LANGUAGE plpgsql;