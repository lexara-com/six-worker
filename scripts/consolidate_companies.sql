-- =============================================
-- Consolidate Duplicate Company Nodes
-- =============================================
-- These duplicates occur when the same company has multiple
-- business registrations (different corp numbers)
-- We'll consolidate to the first instance
-- =============================================

BEGIN;

-- Step 1: Create consolidation mapping for companies
CREATE TEMP TABLE company_consolidation_map AS
WITH company_groups AS (
    SELECT 
        node_id,
        normalized_name,
        primary_name,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY normalized_name ORDER BY created_at ASC) as rn,
        FIRST_VALUE(node_id) OVER (PARTITION BY normalized_name ORDER BY created_at ASC) as keeper_id
    FROM nodes
    WHERE node_type = 'Company'
      AND status = 'active'
)
SELECT 
    cg.node_id as old_node_id,
    cg.normalized_name,
    cg.keeper_id as new_node_id,
    cg.rn,
    COUNT(r.relationship_id) as relationship_count
FROM company_groups cg
LEFT JOIN relationships r ON (r.source_node_id = cg.node_id OR r.target_node_id = cg.node_id)
WHERE cg.rn > 1  -- Only duplicates, not the keeper
GROUP BY cg.node_id, cg.normalized_name, cg.keeper_id, cg.rn;

-- Show consolidation plan
SELECT 
    'Companies to consolidate: ' || COUNT(DISTINCT normalized_name) as summary,
    'Total duplicate nodes: ' || COUNT(*) as total_nodes,
    'Total relationships to migrate: ' || SUM(relationship_count) as total_relationships
FROM company_consolidation_map;

-- Show top duplicated companies
SELECT 
    'Top duplicated companies:' as header,
    normalized_name,
    COUNT(*) as duplicate_count,
    SUM(relationship_count) as total_relationships
FROM company_consolidation_map
GROUP BY normalized_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Step 2: Migrate relationships from duplicates to keeper companies
-- Update source_node_id
UPDATE relationships r
SET source_node_id = c.new_node_id
FROM company_consolidation_map c
WHERE r.source_node_id = c.old_node_id;

-- Update target_node_id  
UPDATE relationships r
SET target_node_id = c.new_node_id
FROM company_consolidation_map c
WHERE r.target_node_id = c.old_node_id;

-- Step 3: Migrate provenance records
UPDATE provenance p
SET asset_id = c.new_node_id
FROM company_consolidation_map c
WHERE p.asset_type = 'node' 
  AND p.asset_id = c.old_node_id;

-- Step 4: Migrate unique attributes 
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
JOIN company_consolidation_map c ON a.node_id = c.old_node_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a2
    WHERE a2.node_id = c.new_node_id
      AND a2.attribute_type = a.attribute_type
      AND COALESCE(a2.normalized_value, a2.attribute_value) = COALESCE(a.normalized_value, a.attribute_value)
);

-- Step 5: Delete duplicate nodes
DELETE FROM nodes n
USING company_consolidation_map c
WHERE n.node_id = c.old_node_id;

-- Step 6: Remove any duplicate relationships that may have been created
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

-- Step 7: Also consolidate duplicate cities that remain
CREATE TEMP TABLE city_consolidation_map AS
WITH city_groups AS (
    SELECT 
        node_id,
        normalized_name,
        entity_class,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY normalized_name ORDER BY 
            CASE entity_class WHEN 'reference' THEN 0 ELSE 1 END, created_at ASC) as rn,
        FIRST_VALUE(node_id) OVER (PARTITION BY normalized_name ORDER BY 
            CASE entity_class WHEN 'reference' THEN 0 ELSE 1 END, created_at ASC) as keeper_id
    FROM nodes
    WHERE node_type = 'City'
      AND status = 'active'
)
SELECT 
    cg.node_id as old_node_id,
    cg.normalized_name,
    cg.keeper_id as new_node_id,
    cg.rn
FROM city_groups cg
WHERE cg.rn > 1;

-- Migrate city relationships
UPDATE relationships r
SET source_node_id = c.new_node_id
FROM city_consolidation_map c
WHERE r.source_node_id = c.old_node_id;

UPDATE relationships r
SET target_node_id = c.new_node_id
FROM city_consolidation_map c
WHERE r.target_node_id = c.old_node_id;

-- Delete duplicate city nodes
DELETE FROM nodes n
USING city_consolidation_map c
WHERE n.node_id = c.old_node_id;

-- Step 8: Verify consolidation results
WITH verification AS (
    SELECT 
        node_type,
        COUNT(DISTINCT normalized_name) as unique_names,
        COUNT(*) as total_nodes,
        COUNT(*) - COUNT(DISTINCT normalized_name) as duplicates
    FROM nodes
    WHERE node_type IN ('Company', 'City')
    GROUP BY node_type
)
SELECT 
    'Post-consolidation summary by type:' as status,
    node_type,
    unique_names,
    total_nodes,
    duplicates
FROM verification;

-- Final check for any remaining duplicates
SELECT 
    'Remaining duplicates:' as status,
    COUNT(*) as duplicate_count
FROM (
    SELECT normalized_name
    FROM nodes
    GROUP BY normalized_name
    HAVING COUNT(*) > 1
) d;

COMMIT;

-- Final statistics
SELECT 
    'Final statistics:' as metric,
    node_type,
    entity_class,
    COUNT(*) as count
FROM nodes
GROUP BY node_type, entity_class
ORDER BY count DESC;