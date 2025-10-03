-- =============================================
-- Properly Consolidate Duplicate City Nodes
-- =============================================
-- This script consolidates all duplicate city nodes by:
-- 1. Moving all relationships to point to the reference entity
-- 2. Deleting all duplicate nodes
-- 3. Ensuring the reference entity node exists
-- =============================================

BEGIN;

-- Step 1: Ensure reference entity nodes exist in the nodes table
INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, status, created_by)
SELECT 
    re.reference_id,
    re.node_type,
    re.primary_name,
    re.normalized_name,
    'reference',
    'active',
    'consolidation'
FROM reference_entities re
WHERE re.node_type = 'City'
  AND NOT EXISTS (
    SELECT 1 FROM nodes n 
    WHERE n.node_id = re.reference_id
  );

-- Show how many reference nodes were created
SELECT 'Reference nodes created: ' || COUNT(*) as status
FROM nodes 
WHERE created_by = 'consolidation';

-- Step 2: Create consolidation mapping
CREATE TEMP TABLE city_consolidation_map AS
SELECT 
    n.node_id as old_node_id,
    n.normalized_name,
    re.reference_id as new_node_id,
    n.entity_class as old_class,
    COUNT(r.relationship_id) as relationship_count
FROM nodes n
JOIN reference_entities re ON re.normalized_name = n.normalized_name AND re.node_type = 'City'
LEFT JOIN relationships r ON (r.source_node_id = n.node_id OR r.target_node_id = n.node_id)
WHERE n.node_type = 'City'
  AND n.node_id != re.reference_id  -- Only duplicates, not the reference itself
GROUP BY n.node_id, n.normalized_name, re.reference_id, n.entity_class;

-- Show consolidation plan
SELECT 
    'Cities to consolidate: ' || COUNT(DISTINCT normalized_name) as summary,
    'Total duplicate nodes: ' || COUNT(*) as total_nodes,
    'Total relationships to migrate: ' || SUM(relationship_count) as total_relationships
FROM city_consolidation_map;

-- Step 3: Migrate relationships from duplicates to reference entities
-- Update source_node_id
UPDATE relationships r
SET source_node_id = c.new_node_id
FROM city_consolidation_map c
WHERE r.source_node_id = c.old_node_id;

-- Update target_node_id  
UPDATE relationships r
SET target_node_id = c.new_node_id
FROM city_consolidation_map c
WHERE r.target_node_id = c.old_node_id;

-- Step 4: Migrate provenance records
UPDATE provenance p
SET asset_id = c.new_node_id
FROM city_consolidation_map c
WHERE p.asset_type = 'node' 
  AND p.asset_id = c.old_node_id;

-- Step 5: Migrate unique attributes (if any)
INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, normalized_value, confidence, source, status, created_by)
SELECT 
    generate_ulid(),
    c.new_node_id,
    a.attribute_type,
    a.attribute_value,
    a.normalized_value,
    a.confidence,
    COALESCE(a.source, 'consolidation'),
    a.status,
    'consolidation'
FROM attributes a
JOIN city_consolidation_map c ON a.node_id = c.old_node_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a2
    WHERE a2.node_id = c.new_node_id
      AND a2.attribute_type = a.attribute_type
      AND COALESCE(a2.normalized_value, a2.attribute_value) = COALESCE(a.normalized_value, a.attribute_value)
);

-- Step 6: Delete duplicate nodes
DELETE FROM nodes n
USING city_consolidation_map c
WHERE n.node_id = c.old_node_id;

-- Step 7: Remove any duplicate relationships that may have been created
WITH duplicate_relationships AS (
    SELECT 
        relationship_id,
        ROW_NUMBER() OVER (
            PARTITION BY source_node_id, target_node_id, relationship_type
            ORDER BY created_at
        ) as rn
    FROM relationships
    WHERE status = 'active'
)
DELETE FROM relationships
WHERE relationship_id IN (
    SELECT relationship_id 
    FROM duplicate_relationships 
    WHERE rn > 1
);

-- Step 8: Verify consolidation results
WITH verification AS (
    SELECT 
        n.normalized_name,
        COUNT(DISTINCT n.node_id) as node_count,
        COUNT(DISTINCT r.relationship_id) as relationship_count,
        BOOL_OR(n.entity_class = 'reference') as has_reference
    FROM nodes n
    LEFT JOIN relationships r ON (r.source_node_id = n.node_id OR r.target_node_id = n.node_id)
    WHERE n.node_type = 'City'
    GROUP BY n.normalized_name
)
SELECT 
    'Post-consolidation summary:' as status,
    COUNT(*) as unique_cities,
    SUM(CASE WHEN node_count = 1 AND has_reference THEN 1 ELSE 0 END) as properly_consolidated,
    SUM(CASE WHEN node_count > 1 THEN 1 ELSE 0 END) as still_duplicated,
    SUM(relationship_count) as total_relationships
FROM verification;

-- Show specific results for problem cities
SELECT 
    'Ames nodes remaining: ' || COUNT(*) as ames_status,
    'All reference: ' || BOOL_AND(entity_class = 'reference') as all_reference
FROM nodes 
WHERE node_type = 'City' AND normalized_name = 'ames';

SELECT 
    'Dubuque nodes remaining: ' || COUNT(*) as dubuque_status,
    'All reference: ' || BOOL_AND(entity_class = 'reference') as all_reference
FROM nodes 
WHERE node_type = 'City' AND normalized_name = 'dubuque';

COMMIT;