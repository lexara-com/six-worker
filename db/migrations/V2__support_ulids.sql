-- =============================================
-- Aurora PostgreSQL Schema Update
-- Version: 2.0
-- Purpose: Update schema to support ULIDs instead of UUIDs
-- =============================================

-- Step 1: Drop foreign key constraints
ALTER TABLE relationships DROP CONSTRAINT IF EXISTS fk_relationships_source;
ALTER TABLE relationships DROP CONSTRAINT IF EXISTS fk_relationships_target;
ALTER TABLE attributes DROP CONSTRAINT IF EXISTS fk_attributes_node;
ALTER TABLE conflict_matrix DROP CONSTRAINT IF EXISTS fk_conflict_entity_a;
ALTER TABLE conflict_matrix DROP CONSTRAINT IF EXISTS fk_conflict_entity_b;

-- Step 2: Change all ID columns from UUID to VARCHAR(26) for ULID support
ALTER TABLE nodes ALTER COLUMN node_id SET DATA TYPE VARCHAR(26);
ALTER TABLE relationships ALTER COLUMN relationship_id SET DATA TYPE VARCHAR(26);
ALTER TABLE relationships ALTER COLUMN source_node_id SET DATA TYPE VARCHAR(26);
ALTER TABLE relationships ALTER COLUMN target_node_id SET DATA TYPE VARCHAR(26);
ALTER TABLE attributes ALTER COLUMN attribute_id SET DATA TYPE VARCHAR(26);
ALTER TABLE attributes ALTER COLUMN node_id SET DATA TYPE VARCHAR(26);
ALTER TABLE conflict_matrix ALTER COLUMN matrix_id SET DATA TYPE VARCHAR(26);
ALTER TABLE conflict_matrix ALTER COLUMN entity_a_id SET DATA TYPE VARCHAR(26);
ALTER TABLE conflict_matrix ALTER COLUMN entity_b_id SET DATA TYPE VARCHAR(26);

-- Step 3: Recreate foreign key constraints
ALTER TABLE relationships ADD CONSTRAINT fk_relationships_source 
    FOREIGN KEY (source_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE;
ALTER TABLE relationships ADD CONSTRAINT fk_relationships_target 
    FOREIGN KEY (target_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE;
ALTER TABLE attributes ADD CONSTRAINT fk_attributes_node 
    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE;
ALTER TABLE conflict_matrix ADD CONSTRAINT fk_conflict_entity_a 
    FOREIGN KEY (entity_a_id) REFERENCES nodes(node_id) ON DELETE CASCADE;
ALTER TABLE conflict_matrix ADD CONSTRAINT fk_conflict_entity_b 
    FOREIGN KEY (entity_b_id) REFERENCES nodes(node_id) ON DELETE CASCADE;

-- Create improved indexes for ULID performance
DROP INDEX IF EXISTS idx_nodes_normalized_name;
DROP INDEX IF EXISTS idx_relationships_source;
DROP INDEX IF EXISTS idx_attributes_node_type;

-- Recreate indexes optimized for VARCHAR(26) ULIDs
CREATE INDEX idx_nodes_normalized_name ON nodes USING btree (normalized_name);
CREATE INDEX idx_nodes_type_name ON nodes USING btree (node_type, normalized_name);
CREATE INDEX idx_relationships_source ON relationships USING btree (source_node_id, relationship_type);
CREATE INDEX idx_relationships_target ON relationships USING btree (target_node_id, relationship_type);
CREATE INDEX idx_relationships_type ON relationships USING btree (relationship_type, strength DESC);
CREATE INDEX idx_attributes_node_type ON attributes USING btree (node_id, attribute_type);
CREATE INDEX idx_attributes_value_search ON attributes USING gin (normalized_value gin_trgm_ops);

-- Update the default value generation functions
-- Note: We'll generate ULIDs in application code rather than database triggers

-- Add comment to document the ULID format
COMMENT ON COLUMN nodes.node_id IS 'ULID: 26 character Universally Unique Lexicographically Sortable Identifier';
COMMENT ON COLUMN relationships.relationship_id IS 'ULID: 26 character identifier';
COMMENT ON COLUMN attributes.attribute_id IS 'ULID: 26 character identifier';