-- Verify JOEL W TUBBS transformation
SELECT 'JOEL W TUBBS Attributes:' as section;
SELECT attribute_type, attribute_value
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY attribute_type;

SELECT 'JOEL W TUBBS Relationships:' as section;
SELECT 
    r.relationship_type,
    n.node_type as target_type,
    n.primary_name as target_name
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY r.relationship_type;

-- Check the address relationships
SELECT 'Address Geographic Relationships:' as section;
WITH joel_address AS (
    SELECT target_node_id as address_id
    FROM relationships
    WHERE source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND relationship_type = 'Located_At'
)
SELECT 
    n1.primary_name as address,
    r.relationship_type,
    n2.node_type as target_type,
    n2.primary_name as target_name
FROM joel_address ja
JOIN nodes n1 ON n1.node_id = ja.address_id
JOIN relationships r ON r.source_node_id = ja.address_id
JOIN nodes n2 ON n2.node_id = r.target_node_id
WHERE r.relationship_type IN ('Located_In', 'Location_Of')
ORDER BY r.relationship_type, n2.node_type;