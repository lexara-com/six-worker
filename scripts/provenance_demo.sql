-- =============================================
-- Provenance System Demonstration Queries
-- Shows how to track sources, changes, and data quality
-- =============================================

\echo '=== PROVENANCE SYSTEM DEMONSTRATION ==='
\echo ''

-- 1. Overview of all data with sources
\echo '1. Data Sources Overview:'
SELECT 
    asset_type,
    COUNT(*) as asset_count,
    COUNT(DISTINCT source_type) as source_types,
    AVG(confidence_score)::NUMERIC(3,2) as avg_confidence,
    MAX(data_obtained_at)::DATE as latest_update
FROM provenance 
WHERE status = 'active'
GROUP BY asset_type
ORDER BY asset_type;

\echo ''
\echo '2. Most Common Source Types:'
SELECT 
    source_type,
    COUNT(*) as usage_count,
    AVG(confidence_score)::NUMERIC(3,2) as avg_confidence,
    COUNT(DISTINCT asset_id) as unique_assets
FROM provenance 
GROUP BY source_type 
ORDER BY usage_count DESC
LIMIT 10;

\echo ''
\echo '3. Entities with Multiple Sources (High Confidence):'
SELECT 
    n.primary_name as entity_name,
    array_agg(DISTINCT p.source_type) as source_types,
    COUNT(DISTINCT p.provenance_id) as source_count,
    AVG(p.confidence_score)::NUMERIC(3,2) as avg_confidence
FROM nodes n
JOIN provenance p ON n.node_id = p.asset_id
WHERE p.asset_type = 'node' AND p.status = 'active'
GROUP BY n.node_id, n.primary_name
HAVING COUNT(DISTINCT p.provenance_id) > 1
ORDER BY avg_confidence DESC, source_count DESC
LIMIT 10;

\echo ''
\echo '4. Low Confidence Data Requiring Review:'
SELECT 
    p.asset_type,
    CASE 
        WHEN p.asset_type = 'node' THEN n.primary_name
        WHEN p.asset_type = 'attribute' THEN a.attribute_type || ': ' || a.attribute_value
        WHEN p.asset_type = 'relationship' THEN r.relationship_type
        ELSE 'Unknown'
    END as asset_description,
    p.source_name,
    p.confidence_score,
    p.reliability_rating
FROM provenance p
LEFT JOIN nodes n ON p.asset_type = 'node' AND p.asset_id = n.node_id
LEFT JOIN attributes a ON p.asset_type = 'attribute' AND p.asset_id = a.attribute_id
LEFT JOIN relationships r ON p.asset_type = 'relationship' AND p.asset_id = r.relationship_id
WHERE p.confidence_score < 0.7 
ORDER BY p.confidence_score ASC
LIMIT 10;

\echo ''
\echo '5. Recent Data Changes (Change History):'
SELECT 
    ch.table_name,
    ch.operation,
    ch.field_name,
    ch.changed_at::TIMESTAMP(0),
    ch.changed_by,
    p.source_type
FROM change_history ch
LEFT JOIN provenance p ON ch.provenance_id = p.provenance_id
WHERE ch.changed_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY ch.changed_at DESC
LIMIT 10;

-- Now let's test the change tracking by making some updates
\echo ''
\echo '6. Testing Change Tracking - Making Updates:'

-- Update a node
UPDATE nodes 
SET primary_name = 'Smith & Associates Legal Group LLP'
WHERE primary_name LIKE '%Smith & Associates%';

-- Update an attribute
UPDATE attributes 
SET attribute_value = 'Senior Managing Partner'
WHERE attribute_id = (
    SELECT attribute_id FROM attributes 
    WHERE attribute_type = 'title' AND attribute_value = 'Senior Partner' 
    LIMIT 1
);

-- Update a relationship strength
UPDATE relationships 
SET strength = 0.95
WHERE relationship_id = (
    SELECT relationship_id FROM relationships 
    WHERE relationship_type = 'Legal_Counsel' AND strength = 1.0 
    LIMIT 1
);

\echo 'Changes made. Checking change history...'
\echo ''

\echo '7. Latest Changes (Just Made):'
SELECT 
    ch.table_name,
    ch.operation,
    ch.field_name,
    ch.old_value,
    ch.new_value,
    ch.changed_at::TIMESTAMP(0)
FROM change_history ch
WHERE ch.changed_at > CURRENT_TIMESTAMP - INTERVAL '10 seconds'
ORDER BY ch.changed_at DESC;

\echo ''
\echo '8. Provenance for Specific High-Value Assets:'
-- Show provenance for law firms (high-value entities)
SELECT 
    n.primary_name as law_firm,
    p.source_name,
    p.source_type,
    p.confidence_score,
    p.reliability_rating,
    p.data_obtained_at::DATE
FROM nodes n
JOIN provenance p ON n.node_id = p.asset_id
WHERE n.node_type = 'Company' 
  AND n.primary_name LIKE '%Law%'
  AND p.asset_type = 'node'
ORDER BY n.primary_name, p.confidence_score DESC;

\echo ''
\echo '9. Data Quality Summary by Source Type:'
SELECT 
    st.source_type,
    st.description,
    st.default_reliability,
    COUNT(p.provenance_id) as records_count,
    AVG(p.confidence_score)::NUMERIC(3,2) as avg_confidence,
    MIN(p.confidence_score) as min_confidence,
    MAX(p.confidence_score) as max_confidence
FROM source_types st
LEFT JOIN provenance p ON st.source_type = p.source_type
GROUP BY st.source_type, st.description, st.default_reliability
HAVING COUNT(p.provenance_id) > 0
ORDER BY avg_confidence DESC;

\echo ''
\echo '10. Comprehensive Audit Trail for One Entity:'
-- Pick a specific entity and show all its provenance and changes
WITH sample_entity AS (
    SELECT node_id, primary_name 
    FROM nodes 
    WHERE node_type = 'Person' 
    LIMIT 1
)
SELECT 
    'PROVENANCE' as record_type,
    p.source_name as source_info,
    p.confidence_score::TEXT as score_or_value,
    p.created_at::TIMESTAMP(0) as timestamp_info
FROM sample_entity se
JOIN provenance p ON se.node_id = p.asset_id
WHERE p.asset_type = 'node'

UNION ALL

SELECT 
    'CHANGE' as record_type,
    ch.field_name || ': ' || COALESCE(ch.old_value, 'NULL') || ' → ' || COALESCE(ch.new_value, 'NULL') as source_info,
    ch.operation as score_or_value,
    ch.changed_at::TIMESTAMP(0) as timestamp_info
FROM sample_entity se
JOIN change_history ch ON se.node_id = ch.record_id
WHERE ch.table_name = 'nodes'

ORDER BY timestamp_info DESC;

\echo ''
\echo '=== PROVENANCE SYSTEM DEMONSTRATION COMPLETE ==='