-- =============================================
-- V17: Fix Registered Agent Person/Company Node Types
-- =============================================
-- This migration identifies registered agents that are likely persons
-- but were incorrectly created as Company nodes, and converts them to Person nodes
-- =============================================

-- First, identify likely person names that are registered agents
CREATE TEMP TABLE person_agents_to_fix AS
SELECT DISTINCT
    n.node_id,
    n.primary_name,
    n.normalized_name,
    COUNT(DISTINCT r.target_node_id) as companies_represented
FROM nodes n
JOIN relationships r ON n.node_id = r.source_node_id
WHERE n.node_type = 'Company'
  AND r.relationship_type = 'Registered_Agent'
  -- Pattern matching for person names
  AND (
    -- Simple name patterns (2-4 words, all caps)
    (n.primary_name ~ '^[A-Z][A-Z ,.-]+$'  -- All caps with spaces
     AND n.primary_name !~ '\d'  -- No numbers
     AND LENGTH(n.primary_name) - LENGTH(REPLACE(n.primary_name, ' ', '')) BETWEEN 1 AND 3  -- 2-4 words
    )
    OR
    -- Common person name patterns
    n.primary_name ~ '^[A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+$'  -- First M. Last
    OR n.primary_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+$'  -- First Middle Last
    OR n.primary_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+$'  -- First Last
  )
  -- Exclude obvious business names
  AND n.primary_name NOT LIKE '%LLC%'
  AND n.primary_name NOT LIKE '%L.L.C.%'
  AND n.primary_name NOT LIKE '%INC%'
  AND n.primary_name NOT LIKE '% INC.%'
  AND n.primary_name NOT LIKE '%CORP%'
  AND n.primary_name NOT LIKE '%CORPORATION%'
  AND n.primary_name NOT LIKE '%COMPANY%'
  AND n.primary_name NOT LIKE '%GROUP%'
  AND n.primary_name NOT LIKE '%ENTERPRISES%'
  AND n.primary_name NOT LIKE '%ASSOCIATES%'
  AND n.primary_name NOT LIKE '%PARTNERS%'
  AND n.primary_name NOT LIKE '%TRUST%'
  AND n.primary_name NOT LIKE '%BANK%'
  AND n.primary_name NOT LIKE '%CLUB%'
  AND n.primary_name NOT LIKE '%ASSOCIATION%'
  AND n.primary_name NOT LIKE '%FOUNDATION%'
  AND n.primary_name NOT LIKE '%CHURCH%'
  AND n.primary_name NOT LIKE '%SOCIETY%'
  AND n.primary_name NOT LIKE '%COUNCIL%'
  AND n.primary_name NOT LIKE '%COMMITTEE%'
  AND n.primary_name NOT LIKE '%& %'  -- No ampersands (partnerships)
  AND n.primary_name NOT LIKE '%/%'  -- No slashes (DBAs)
  AND n.primary_name NOT LIKE 'THE %'  -- Organizations often start with "THE"
GROUP BY n.node_id, n.primary_name, n.normalized_name;

-- Show what we're about to fix
SELECT 'Registered agents to convert from Company to Person:' as info;
SELECT 
    primary_name,
    companies_represented,
    node_id
FROM person_agents_to_fix
ORDER BY companies_represented DESC, primary_name
LIMIT 20;

-- Count total to fix
SELECT 'Total agents to fix:' as info, COUNT(*) as count FROM person_agents_to_fix;

-- Update the node types
UPDATE nodes
SET 
    node_type = 'Person',
    updated_at = CURRENT_TIMESTAMP
FROM person_agents_to_fix patf
WHERE nodes.node_id = patf.node_id;

-- For Person nodes, add name parsing attributes
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    n.node_id,
    'computed_first_name',
    CASE 
        WHEN n.primary_name ~ '^[A-Z][A-Z ]+$' THEN 
            -- All caps - split and take first word
            SPLIT_PART(n.primary_name, ' ', 1)
        ELSE
            -- Mixed case - take first word
            SPLIT_PART(n.primary_name, ' ', 1)
    END,
    'migration_v17'
FROM nodes n
JOIN person_agents_to_fix patf ON n.node_id = patf.node_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a 
    WHERE a.node_id = n.node_id 
    AND a.attribute_type = 'computed_first_name'
);

-- Add last name
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    n.node_id,
    'computed_surname',
    CASE 
        WHEN n.primary_name ~ '^[A-Z][A-Z ]+$' THEN 
            -- All caps - take last word
            REVERSE(SPLIT_PART(REVERSE(n.primary_name), ' ', 1))
        ELSE
            -- Mixed case - take last word
            REVERSE(SPLIT_PART(REVERSE(n.primary_name), ' ', 1))
    END,
    'migration_v17'
FROM nodes n
JOIN person_agents_to_fix patf ON n.node_id = patf.node_id
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a 
    WHERE a.node_id = n.node_id 
    AND a.attribute_type = 'computed_surname'
);

-- Add middle name/initial if present
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    n.node_id,
    'computed_additional_name',
    middle_name,
    'migration_v17'
FROM (
    SELECT 
        n.node_id,
        n.primary_name,
        NULLIF(TRIM(
            REPLACE(
                REPLACE(n.primary_name, SPLIT_PART(n.primary_name, ' ', 1), ''),
                REVERSE(SPLIT_PART(REVERSE(n.primary_name), ' ', 1)), ''
            )
        ), '') as middle_name
    FROM nodes n
    JOIN person_agents_to_fix patf ON n.node_id = patf.node_id
    WHERE LENGTH(n.primary_name) - LENGTH(REPLACE(n.primary_name, ' ', '')) >= 2
) n
WHERE middle_name IS NOT NULL
  AND middle_name != ''
  AND NOT EXISTS (
    SELECT 1 FROM attributes a 
    WHERE a.node_id = n.node_id 
    AND a.attribute_type = 'computed_additional_name'
);

-- Add role attribute for all converted agents
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    patf.node_id,
    'professional_role',
    'Registered Agent',
    'migration_v17'
FROM person_agents_to_fix patf
WHERE NOT EXISTS (
    SELECT 1 FROM attributes a 
    WHERE a.node_id = patf.node_id 
    AND a.attribute_type = 'professional_role'
    AND a.attribute_value = 'Registered Agent'
);

-- Update provenance to note the conversion (skip if trigger issues)
DO $$
BEGIN
    UPDATE provenance
    SET 
        notes = COALESCE(notes || E'\n', '') || 
                'Node type corrected from Company to Person (registered agent pattern match) - V17 migration',
        metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
            'node_type_correction', jsonb_build_object(
                'from', 'Company',
                'to', 'Person',
                'reason', 'registered_agent_name_pattern',
                'migration', 'V17',
                'corrected_at', CURRENT_TIMESTAMP
            )
        )
    FROM person_agents_to_fix patf
    WHERE provenance.asset_id = patf.node_id
      AND provenance.asset_type = 'node';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Provenance update skipped due to trigger issue: %', SQLERRM;
END;
$$;

-- Show results
SELECT 'Migration complete. Summary:' as info;
SELECT 
    'Converted to Person nodes' as action,
    COUNT(*) as count
FROM person_agents_to_fix
UNION ALL
SELECT 
    'Name attributes added' as action,
    COUNT(DISTINCT node_id) * 2 as count  -- first and last name for each
FROM person_agents_to_fix
UNION ALL
SELECT 
    'Provenance records updated' as action,
    COUNT(*) as count
FROM provenance p
JOIN person_agents_to_fix patf ON p.asset_id = patf.node_id
WHERE p.asset_type = 'node';

-- Verify specific cases
SELECT 'Verification - SCOTT CHRISTIAN NELSON:' as info;
SELECT 
    node_id,
    node_type,
    primary_name,
    updated_at
FROM nodes 
WHERE primary_name = 'SCOTT CHRISTIAN NELSON';

-- Show sample of converted nodes with their attributes
SELECT 'Sample converted nodes with attributes:' as info;
SELECT 
    n.primary_name,
    n.node_type,
    MAX(CASE WHEN a.attribute_type = 'computed_first_name' THEN a.attribute_value END) as first_name,
    MAX(CASE WHEN a.attribute_type = 'computed_additional_name' THEN a.attribute_value END) as middle_name,
    MAX(CASE WHEN a.attribute_type = 'computed_surname' THEN a.attribute_value END) as last_name,
    MAX(CASE WHEN a.attribute_type = 'professional_role' THEN a.attribute_value END) as role
FROM nodes n
JOIN person_agents_to_fix patf ON n.node_id = patf.node_id
LEFT JOIN attributes a ON n.node_id = a.node_id
WHERE n.primary_name IN ('SCOTT CHRISTIAN NELSON', 'SCOTT J. MORRIS', 'COLIN WETLAUFER')
GROUP BY n.node_id, n.primary_name, n.node_type;

-- Clean up temp table
DROP TABLE person_agents_to_fix;

-- Add comment about this migration
COMMENT ON COLUMN nodes.node_type IS 'Node type - V17 migration fixed registered agents incorrectly stored as Company when they should be Person';