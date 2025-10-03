-- Understanding the sources/provenance relationship

-- What source types are defined?
SELECT 'SOURCE_TYPES table contents:' as info;
SELECT 
    source_type,
    description,
    default_reliability,
    requires_license
FROM source_types
ORDER BY source_type;

-- How are source_types being used in provenance?
SELECT 'Source types used in provenance:' as info;
SELECT 
    p.source_type,
    st.description,
    COUNT(*) as usage_count
FROM provenance p
LEFT JOIN source_types st ON p.source_type = st.source_type
GROUP BY p.source_type, st.description
ORDER BY usage_count DESC
LIMIT 20;

-- Check the relationship via source_type_id
SELECT 'Provenance source_type_id usage:' as info;
SELECT 
    COUNT(*) as total_provenance,
    COUNT(source_type_id) as with_source_type_id,
    COUNT(*) - COUNT(source_type_id) as without_source_type_id
FROM provenance;

-- Sample provenance records with their source_type details
SELECT 'Sample provenance with source_type details:' as info;
SELECT 
    p.asset_type,
    p.asset_id,
    p.source_name,
    p.source_type,
    st.description as source_description,
    p.confidence_score,
    p.reliability_rating
FROM provenance p
LEFT JOIN source_types st ON p.source_type = st.source_type
LIMIT 10;

-- Check unique source_names and source_types
SELECT 'Unique source combinations:' as info;
SELECT 
    source_name,
    source_type,
    COUNT(*) as count
FROM provenance
GROUP BY source_name, source_type
ORDER BY count DESC
LIMIT 10;