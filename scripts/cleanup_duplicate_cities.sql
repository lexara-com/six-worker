-- =============================================
-- Cleanup Duplicate City Nodes
-- =============================================
-- This script consolidates duplicate city nodes by:
-- 1. Keeping the reference entity nodes
-- 2. Migrating relationships from duplicates to the reference nodes
-- 3. Deleting the duplicate fact-based nodes
-- =============================================

BEGIN;

-- Step 1: Create a mapping table of duplicates to their reference entities
CREATE TEMP TABLE city_consolidation AS
WITH duplicate_cities AS (
    SELECT 
        n.node_id as duplicate_id,
        n.normalized_name,
        n.primary_name,
        n.entity_class,
        re.reference_id as target_id,
        ROW_NUMBER() OVER (PARTITION BY n.normalized_name ORDER BY 
            CASE n.entity_class 
                WHEN 'reference' THEN 0  -- Keep reference nodes
                ELSE 1                    -- Mark fact_based for deletion
            END,
            n.created_at
        ) as rn
    FROM nodes n
    JOIN reference_entities re ON re.normalized_name = n.normalized_name AND re.node_type = 'City'
    WHERE n.node_type = 'City'
)
SELECT 
    duplicate_id,
    target_id,
    normalized_name,
    entity_class
FROM duplicate_cities
WHERE duplicate_id != target_id;  -- Only include nodes that need consolidation

-- Show what will be consolidated
SELECT 
    'Cities to consolidate: ' || COUNT(DISTINCT normalized_name) as summary,
    'Duplicate nodes to process: ' || COUNT(*) as total_duplicates
FROM city_consolidation;

-- Step 2: Update relationships to point to reference entities
UPDATE relationships r
SET source_node_id = c.target_id
FROM city_consolidation c
WHERE r.source_node_id = c.duplicate_id;

UPDATE relationships r
SET target_node_id = c.target_id
FROM city_consolidation c
WHERE r.target_node_id = c.duplicate_id;

-- Step 3: Update provenance records to point to reference entities
UPDATE provenance p
SET asset_id = c.target_id
FROM city_consolidation c
WHERE p.asset_type = 'node' AND p.asset_id = c.duplicate_id;

-- Step 4: Migrate attributes to reference entities (if any unique ones exist)
-- Only migrate attributes that don't already exist on the target
INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, confidence, source, status, created_by)
SELECT DISTINCT
    c.target_id,
    a.attribute_type,
    a.attribute_value,
    a.normalized_value,
    a.confidence,
    a.source,
    a.status,
    'consolidation'
FROM attributes a
JOIN city_consolidation c ON a.node_id = c.duplicate_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a2
    WHERE a2.node_id = c.target_id
      AND a2.attribute_type = a.attribute_type
      AND a2.normalized_value = a.normalized_value
);

-- Step 5: Delete attributes from duplicate nodes
DELETE FROM attributes a
USING city_consolidation c
WHERE a.node_id = c.duplicate_id;

-- Step 6: Delete the duplicate nodes
DELETE FROM nodes n
USING city_consolidation c
WHERE n.node_id = c.duplicate_id;

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

-- Show final results
SELECT 
    'Consolidation complete!' as status,
    (SELECT COUNT(*) FROM city_consolidation) as duplicates_removed,
    (SELECT COUNT(*) FROM nodes WHERE node_type = 'City') as remaining_cities,
    (SELECT COUNT(*) FROM nodes WHERE node_type = 'City' AND entity_class = 'reference') as reference_cities;

COMMIT;