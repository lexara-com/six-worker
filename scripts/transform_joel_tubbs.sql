-- =============================================
-- Transform JOEL W TUBBS to proper data model
-- =============================================
-- This demonstrates the correct structure with:
-- - Address node with geographic relationships
-- - ZIP code as separate node
-- - Computed name attributes for the Person
-- =============================================

BEGIN;

-- Process JOEL W TUBBS specifically
SELECT migrate_person_address_attributes('01K6GMKE4Z0GHW7637XAB9KYVR');

-- Show the transformed data
SELECT '==================== JOEL W TUBBS - TRANSFORMED DATA ====================' as section;

-- Person information with computed name attributes
SELECT 'Person Node:' as category;
SELECT 
    node_id,
    node_type,
    primary_name,
    normalized_name
FROM nodes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR';

SELECT 'Person Attributes (including computed names):' as category;
SELECT 
    attribute_type,
    attribute_value
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY 
    CASE attribute_type
        WHEN 'computed_first_name' THEN 1
        WHEN 'computed_additional_name' THEN 2
        WHEN 'computed_surname' THEN 3
        ELSE 4
    END;

-- Show relationships from Person
SELECT 'Person Relationships:' as category;
SELECT 
    r.relationship_type,
    n.node_type as target_type,
    n.primary_name as target_name
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY r.relationship_type;

-- Find the address node
WITH person_address AS (
    SELECT r.target_node_id as address_id
    FROM relationships r
    WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND r.relationship_type = 'Located_At'
    LIMIT 1
)
SELECT 'Address Node:' as category;

WITH person_address AS (
    SELECT r.target_node_id as address_id
    FROM relationships r
    WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND r.relationship_type = 'Located_At'
    LIMIT 1
)
SELECT 
    n.node_id,
    n.node_type,
    n.primary_name
FROM nodes n
JOIN person_address pa ON n.node_id = pa.address_id;

-- Show address attributes (USPS normalized if available)
SELECT 'Address Attributes:' as category;
WITH person_address AS (
    SELECT r.target_node_id as address_id
    FROM relationships r
    WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND r.relationship_type = 'Located_At'
    LIMIT 1
)
SELECT 
    a.attribute_type,
    a.attribute_value
FROM attributes a
JOIN person_address pa ON a.node_id = pa.address_id;

-- Show address relationships to geographic entities
SELECT 'Address Geographic Relationships:' as category;
WITH person_address AS (
    SELECT r.target_node_id as address_id
    FROM relationships r
    WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND r.relationship_type = 'Located_At'
    LIMIT 1
)
SELECT 
    'Address' as source,
    r.relationship_type,
    n.node_type as target_type,
    n.primary_name as target_name
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
JOIN person_address pa ON r.source_node_id = pa.address_id
ORDER BY 
    CASE n.node_type
        WHEN 'ZipCode' THEN 1
        WHEN 'City' THEN 2
        WHEN 'State' THEN 3
        ELSE 4
    END;

-- Show full geographic hierarchy
SELECT '==================== GEOGRAPHIC HIERARCHY ====================' as section;
WITH RECURSIVE geo_hierarchy AS (
    -- Start with the person
    SELECT 
        1 as level,
        'Person' as source_type,
        n1.primary_name as source_name,
        r.relationship_type,
        n2.node_type as target_type,
        n2.primary_name as target_name,
        n2.node_id as current_id
    FROM relationships r
    JOIN nodes n1 ON n1.node_id = r.source_node_id
    JOIN nodes n2 ON n2.node_id = r.target_node_id
    WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND r.relationship_type = 'Located_At'
    
    UNION ALL
    
    -- Recurse through geographic relationships
    SELECT 
        gh.level + 1,
        gh.target_type,
        gh.target_name,
        r.relationship_type,
        n.node_type,
        n.primary_name,
        n.node_id
    FROM geo_hierarchy gh
    JOIN relationships r ON r.source_node_id = gh.current_id
    JOIN nodes n ON n.node_id = r.target_node_id
    WHERE r.relationship_type = 'Located_In'
      AND gh.level < 5
)
SELECT 
    REPEAT('  ', level - 1) || source_type || ': ' || source_name as entity,
    '→ ' || relationship_type || ' → ' || target_type || ': ' || target_name as relationship
FROM geo_hierarchy
ORDER BY level;

COMMIT;