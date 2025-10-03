-- =============================================
-- Consolidate Duplicate Address Nodes
-- =============================================
-- This script consolidates all duplicate address nodes by:
-- 1. Keeping the first instance of each address
-- 2. Migrating all relationships to point to the first instance
-- 3. Deleting the duplicate nodes
-- =============================================

BEGIN;

-- Step 1: Create consolidation mapping
CREATE TEMP TABLE address_consolidation_map AS
WITH address_groups AS (
    SELECT 
        node_id,
        normalized_name,
        primary_name,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY normalized_name ORDER BY created_at ASC) as rn,
        FIRST_VALUE(node_id) OVER (PARTITION BY normalized_name ORDER BY created_at ASC) as keeper_id
    FROM nodes
    WHERE node_type = 'Address'
      AND status = 'active'
)
SELECT 
    ag.node_id as old_node_id,
    ag.normalized_name,
    ag.keeper_id as new_node_id,
    ag.rn,
    COUNT(r.relationship_id) as relationship_count
FROM address_groups ag
LEFT JOIN relationships r ON (r.source_node_id = ag.node_id OR r.target_node_id = ag.node_id)
WHERE ag.rn > 1  -- Only duplicates, not the keeper
GROUP BY ag.node_id, ag.normalized_name, ag.keeper_id, ag.rn;

-- Show consolidation plan
SELECT 
    'Addresses to consolidate: ' || COUNT(DISTINCT normalized_name) as summary,
    'Total duplicate nodes: ' || COUNT(*) as total_nodes,
    'Total relationships to migrate: ' || SUM(relationship_count) as total_relationships
FROM address_consolidation_map;

-- Show top duplicated addresses
SELECT 
    'Top duplicated addresses:' as header,
    normalized_name,
    COUNT(*) as duplicate_count,
    SUM(relationship_count) as total_relationships
FROM address_consolidation_map
GROUP BY normalized_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Step 2: Migrate relationships from duplicates to keeper addresses
-- Update source_node_id
UPDATE relationships r
SET source_node_id = c.new_node_id
FROM address_consolidation_map c
WHERE r.source_node_id = c.old_node_id;

-- Update target_node_id  
UPDATE relationships r
SET target_node_id = c.new_node_id
FROM address_consolidation_map c
WHERE r.target_node_id = c.old_node_id;

-- Step 3: Migrate provenance records
UPDATE provenance p
SET asset_id = c.new_node_id
FROM address_consolidation_map c
WHERE p.asset_type = 'node' 
  AND p.asset_id = c.old_node_id;

-- Step 4: Migrate unique attributes (especially coordinates)
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
JOIN address_consolidation_map c ON a.node_id = c.old_node_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a2
    WHERE a2.node_id = c.new_node_id
      AND a2.attribute_type = a.attribute_type
      AND COALESCE(a2.normalized_value, a2.attribute_value) = COALESCE(a.normalized_value, a.attribute_value)
);

-- Step 5: Delete duplicate nodes
DELETE FROM nodes n
USING address_consolidation_map c
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

-- Step 7: Verify consolidation results
WITH verification AS (
    SELECT 
        n.normalized_name,
        COUNT(DISTINCT n.node_id) as node_count,
        COUNT(DISTINCT r.relationship_id) as relationship_count
    FROM nodes n
    LEFT JOIN relationships r ON (r.source_node_id = n.node_id OR r.target_node_id = n.node_id)
    WHERE n.node_type = 'Address'
    GROUP BY n.normalized_name
)
SELECT 
    'Post-consolidation summary:' as status,
    COUNT(*) as unique_addresses,
    SUM(CASE WHEN node_count = 1 THEN 1 ELSE 0 END) as properly_consolidated,
    SUM(CASE WHEN node_count > 1 THEN 1 ELSE 0 END) as still_duplicated,
    SUM(relationship_count) as total_relationships
FROM verification;

-- Show specific example that was problematic
SELECT 
    'Address "1636 n.w. 114th street, clive, ia, 50325" nodes remaining: ' || COUNT(*) as status
FROM nodes 
WHERE node_type = 'Address' 
  AND normalized_name = normalize_name('1636 n.w. 114th street, clive, ia, 50325');

COMMIT;

-- Final verification
SELECT 
    'Final address count: ' || COUNT(*) as addresses,
    'Distinct normalized names: ' || COUNT(DISTINCT normalized_name) as unique_addresses
FROM nodes
WHERE node_type = 'Address';