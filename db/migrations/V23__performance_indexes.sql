-- =============================================
-- V23: Add Critical Performance Indexes
-- =============================================
-- These indexes are critical for propose_fact performance
-- as data volumes grow. Without these, entity resolution
-- requires full table scans.
-- =============================================

-- 1. Entity Resolution Indexes on nodes table
-- =============================================

-- Critical for exact name matching in entity resolution
-- Query pattern: SELECT * FROM nodes WHERE node_type = ? AND primary_name = ?
CREATE INDEX IF NOT EXISTS idx_nodes_type_primary_name 
ON nodes(node_type, primary_name);

-- Critical for case-insensitive matching in entity resolution
-- Query pattern: SELECT * FROM nodes WHERE node_type = ? AND UPPER(primary_name) = UPPER(?)
CREATE INDEX IF NOT EXISTS idx_nodes_type_upper_name 
ON nodes(node_type, UPPER(primary_name));

-- Improve the existing normalized_name index to include node_type
DROP INDEX IF EXISTS idx_nodes_normalized_name;
CREATE INDEX idx_nodes_type_normalized_name 
ON nodes(node_type, normalized_name);

-- 2. Relationship Duplicate Detection Index
-- =============================================

-- Critical for checking if relationship already exists
-- Query pattern: SELECT * FROM relationships WHERE source_id = ? AND target_id = ? AND relationship_type = ?
CREATE INDEX IF NOT EXISTS idx_relationships_triple 
ON relationships(source_id, target_id, relationship_type);

-- Also helpful for reverse lookups
CREATE INDEX IF NOT EXISTS idx_relationships_target_source_type
ON relationships(target_id, source_id, relationship_type);

-- 3. Attribute Lookup Indexes
-- =============================================

-- For exact attribute value matching
-- Query pattern: SELECT * FROM attributes WHERE attribute_type = ? AND attribute_value = ?
CREATE INDEX IF NOT EXISTS idx_attributes_type_value_exact 
ON attributes(attribute_type, attribute_value);

-- For finding entities by specific attribute
-- Query pattern: SELECT node_id FROM attributes WHERE attribute_type = ? AND attribute_value = ?
CREATE INDEX IF NOT EXISTS idx_attributes_lookup 
ON attributes(attribute_type, attribute_value, node_id);

-- 4. Provenance Performance Indexes
-- =============================================

-- For finding recent provenance by entity
CREATE INDEX IF NOT EXISTS idx_provenance_asset_recent
ON provenance(asset_type, asset_id, data_obtained_at DESC);

-- 5. Source Processing Index
-- =============================================

-- For checking if source file already processed
CREATE INDEX IF NOT EXISTS idx_sources_type_hash
ON sources(source_type, file_hash);

-- 6. Update Table Statistics
-- =============================================

-- Ensure query planner has accurate statistics
ANALYZE nodes;
ANALYZE relationships;
ANALYZE attributes;
ANALYZE provenance;

-- 7. Report on new indexes
-- =============================================

SELECT 
    'Index created' as status,
    indexname,
    tablename,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) as size
FROM pg_indexes
WHERE schemaname = 'public'
AND indexname IN (
    'idx_nodes_type_primary_name',
    'idx_nodes_type_upper_name',
    'idx_nodes_type_normalized_name',
    'idx_relationships_triple',
    'idx_relationships_target_source_type',
    'idx_attributes_type_value_exact',
    'idx_attributes_lookup',
    'idx_provenance_asset_recent',
    'idx_sources_type_hash'
)
ORDER BY tablename, indexname;

-- Performance note
COMMENT ON INDEX idx_nodes_type_primary_name IS 'Critical for entity resolution performance - exact name matching';
COMMENT ON INDEX idx_nodes_type_upper_name IS 'Critical for entity resolution performance - case-insensitive matching';
COMMENT ON INDEX idx_relationships_triple IS 'Critical for duplicate relationship detection';
COMMENT ON INDEX idx_attributes_type_value_exact IS 'Critical for attribute-based entity matching';