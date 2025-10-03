-- =============================================
-- Examine Node: 01K6GMKE4Z0GHW7637XAB9KYVR
-- =============================================
-- This script provides a comprehensive view of a specific node,
-- its relationships, and attributes to analyze data modeling decisions
-- =============================================

-- Section 1: Basic node information
SELECT '==================== NODE INFORMATION ====================' as section;
SELECT 
    node_id,
    node_type,
    primary_name,
    normalized_name,
    entity_class,
    status,
    created_at,
    updated_at,
    created_by
FROM nodes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR';

-- Section 2: Attributes for this node
SELECT '==================== NODE ATTRIBUTES ====================' as section;
SELECT 
    attribute_id,
    attribute_type,
    attribute_value,
    normalized_value,
    confidence,
    source,
    status,
    created_at
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY attribute_type, created_at;

-- Count attributes by type
SELECT '==================== ATTRIBUTE SUMMARY ====================' as section;
SELECT 
    attribute_type,
    COUNT(*) as count,
    STRING_AGG(DISTINCT attribute_value, ' | ' ORDER BY attribute_value) as sample_values
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
GROUP BY attribute_type
ORDER BY count DESC;

-- Section 3: Relationships where this node is the source
SELECT '==================== OUTGOING RELATIONSHIPS ====================' as section;
SELECT 
    r.relationship_id,
    r.relationship_type,
    r.target_node_id,
    n.node_type as target_type,
    n.primary_name as target_name,
    r.strength,
    r.created_at
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
  AND r.status = 'active'
ORDER BY r.relationship_type, r.created_at;

-- Section 4: Relationships where this node is the target
SELECT '==================== INCOMING RELATIONSHIPS ====================' as section;
SELECT 
    r.relationship_id,
    r.relationship_type,
    r.source_node_id,
    n.node_type as source_type,
    n.primary_name as source_name,
    r.strength,
    r.created_at
FROM relationships r
JOIN nodes n ON n.node_id = r.source_node_id
WHERE r.target_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
  AND r.status = 'active'
ORDER BY r.relationship_type, r.created_at;

-- Section 5: Relationship summary
SELECT '==================== RELATIONSHIP SUMMARY ====================' as section;
WITH relationship_counts AS (
    SELECT 
        'Outgoing' as direction,
        relationship_type,
        COUNT(*) as count
    FROM relationships
    WHERE source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND status = 'active'
    GROUP BY relationship_type
    
    UNION ALL
    
    SELECT 
        'Incoming' as direction,
        relationship_type,
        COUNT(*) as count
    FROM relationships
    WHERE target_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
      AND status = 'active'
    GROUP BY relationship_type
)
SELECT 
    direction,
    relationship_type,
    count
FROM relationship_counts
ORDER BY direction, count DESC, relationship_type;

-- Section 6: Analysis of attributes that might be better as relationships
SELECT '==================== ATTRIBUTE ANALYSIS ====================' as section;
SELECT 
    attribute_type,
    attribute_value,
    CASE 
        WHEN attribute_type IN ('corp_number', 'registration_number', 'ein', 'ssn', 'license_number') 
            THEN 'Keep as attribute - Identifier'
        WHEN attribute_type IN ('date_incorporated', 'date_of_birth', 'registration_date', 'expiry_date')
            THEN 'Keep as attribute - Date value'
        WHEN attribute_type IN ('status', 'entity_type', 'business_type')
            THEN 'Keep as attribute - Status/Type'
        WHEN attribute_type IN ('coordinates', 'latitude', 'longitude')
            THEN 'Keep as attribute - Geographic data'
        WHEN attribute_type LIKE '%address%' OR attribute_type LIKE '%location%'
            THEN '⚠️  CONSIDER RELATIONSHIP - Could link to Address node'
        WHEN attribute_type LIKE '%owner%' OR attribute_type LIKE '%president%' OR attribute_type LIKE '%ceo%' OR attribute_type LIKE '%director%'
            THEN '⚠️  CONSIDER RELATIONSHIP - Could link to Person node'
        WHEN attribute_type LIKE '%parent%' OR attribute_type LIKE '%subsidiary%'
            THEN '⚠️  CONSIDER RELATIONSHIP - Could link to Company node'
        WHEN attribute_type IN ('industry', 'sector')
            THEN '⚠️  CONSIDER RELATIONSHIP - Could link to Industry reference node'
        WHEN attribute_type LIKE '%email%' OR attribute_type LIKE '%phone%' OR attribute_type LIKE '%website%'
            THEN 'Keep as attribute - Contact info (unless creating ContactInfo nodes)'
        ELSE 'Review needed'
    END as recommendation,
    CASE 
        WHEN attribute_type LIKE '%address%' OR attribute_type LIKE '%location%'
            THEN 'CREATE: Address node with Located_At relationship'
        WHEN attribute_type LIKE '%owner%' OR attribute_type LIKE '%president%' OR attribute_type LIKE '%ceo%' OR attribute_type LIKE '%director%'
            THEN 'CREATE: Person node with Officer_Of/Director_Of relationship'
        WHEN attribute_type LIKE '%parent%' OR attribute_type LIKE '%subsidiary%'
            THEN 'CREATE: Company node with Parent_Of/Subsidiary_Of relationship'
        WHEN attribute_type IN ('industry', 'sector')
            THEN 'CREATE: Industry reference node with Operates_In relationship'
        ELSE NULL
    END as suggested_action
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY 
    CASE 
        WHEN attribute_type LIKE '%address%' OR attribute_type LIKE '%location%' THEN 1
        WHEN attribute_type LIKE '%owner%' OR attribute_type LIKE '%president%' THEN 2
        WHEN attribute_type LIKE '%parent%' OR attribute_type LIKE '%subsidiary%' THEN 3
        ELSE 4
    END,
    attribute_type;

-- Section 7: Check for potential duplicate relationships
SELECT '==================== DUPLICATE RELATIONSHIP CHECK ====================' as section;
WITH potential_duplicates AS (
    SELECT 
        source_node_id,
        target_node_id,
        relationship_type,
        COUNT(*) as count
    FROM relationships
    WHERE (source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR' OR target_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR')
      AND status = 'active'
    GROUP BY source_node_id, target_node_id, relationship_type
    HAVING COUNT(*) > 1
)
SELECT 
    CASE 
        WHEN source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR' THEN 'Outgoing'
        ELSE 'Incoming'
    END as direction,
    relationship_type,
    count,
    source_node_id,
    target_node_id
FROM potential_duplicates;

-- Section 8: Connected node types summary
SELECT '==================== CONNECTED NODE TYPES ====================' as section;
WITH connected_nodes AS (
    SELECT DISTINCT target_node_id as connected_node_id
    FROM relationships
    WHERE source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR' AND status = 'active'
    
    UNION
    
    SELECT DISTINCT source_node_id as connected_node_id
    FROM relationships
    WHERE target_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR' AND status = 'active'
)
SELECT 
    n.node_type,
    COUNT(*) as count
FROM connected_nodes cn
JOIN nodes n ON n.node_id = cn.connected_node_id
GROUP BY n.node_type
ORDER BY count DESC;

-- Section 9: Provenance information
SELECT '==================== PROVENANCE ====================' as section;
SELECT 
    provenance_id,
    source_name,
    source_type,
    source_metadata,
    confidence,
    created_at
FROM provenance
WHERE asset_type = 'node' 
  AND asset_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY created_at;