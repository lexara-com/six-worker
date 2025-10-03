-- Investigate why registered agents are being created as Company nodes instead of Person nodes

-- Check the specific node
SELECT 
    'Node details:' as info,
    node_id,
    node_type,
    primary_name,
    normalized_name,
    entity_class,
    created_by
FROM nodes 
WHERE node_id = '01K6GN0A81AR204MWXJ6P1VC6K';

-- Check relationships for this node
SELECT 'Relationships:' as info;
SELECT 
    r.relationship_type,
    CASE 
        WHEN r.source_node_id = '01K6GN0A81AR204MWXJ6P1VC6K' THEN 'SOURCE'
        ELSE 'TARGET'
    END as node_role,
    n.primary_name as connected_to,
    n.node_type as connected_type
FROM relationships r
JOIN nodes n ON (
    CASE 
        WHEN r.source_node_id = '01K6GN0A81AR204MWXJ6P1VC6K' THEN r.target_node_id 
        ELSE r.source_node_id 
    END = n.node_id
)
WHERE r.source_node_id = '01K6GN0A81AR204MWXJ6P1VC6K'
   OR r.target_node_id = '01K6GN0A81AR204MWXJ6P1VC6K';

-- Check attributes
SELECT 'Attributes:' as info;
SELECT 
    attribute_key,
    attribute_value
FROM attributes
WHERE node_id = '01K6GN0A81AR204MWXJ6P1VC6K';

-- Find other similar cases - names that look like people but are Company nodes
SELECT 'Other potential person names as Company nodes:' as info;
SELECT 
    node_id,
    primary_name,
    node_type
FROM nodes
WHERE node_type = 'Company'
  AND (
    -- Common patterns for person names
    primary_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+$' OR -- First Last
    primary_name ~ '^[A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+$' OR -- First M. Last
    primary_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+$' -- First Middle Last
  )
  AND primary_name NOT LIKE '%LLC%'
  AND primary_name NOT LIKE '%INC%'
  AND primary_name NOT LIKE '%CORP%'
  AND primary_name NOT LIKE '%COMPANY%'
  AND primary_name NOT LIKE '%GROUP%'
  AND primary_name NOT LIKE '%ENTERPRISES%'
LIMIT 20;

-- Check if there are any Person nodes that are registered agents
SELECT 'Person nodes as registered agents:' as info;
SELECT 
    n.node_id,
    n.primary_name,
    COUNT(*) as agent_for_count
FROM nodes n
JOIN relationships r ON n.node_id = r.source_node_id
WHERE n.node_type = 'Person'
  AND r.relationship_type = 'registered_agent_for'
GROUP BY n.node_id, n.primary_name
LIMIT 10;

-- Look at the propose_geographic_fact function to see how it determines node_type
SELECT 'How node types are determined:' as info;
SELECT '\nThe propose_geographic_fact function appears to create all non-geographic entities as Company nodes by default.
This is likely the issue - when processing Registered Agent columns, the function doesn''t distinguish
between company names and person names.' as analysis;