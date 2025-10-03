-- =============================================
-- V18: Add Missing Computed Name Attributes for Person Nodes
-- =============================================
-- This migration adds computed name attributes (first, middle, last names)
-- to all Person nodes that don't have them yet
-- =============================================

-- First, let's see how many Person nodes are missing name attributes
SELECT 'Person nodes missing computed name attributes:' as info;
SELECT 
    COUNT(*) as total_missing,
    COUNT(*) FILTER (WHERE primary_name ~ '^[A-Z][A-Z ]+$') as all_caps_names,
    COUNT(*) FILTER (WHERE primary_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+$') as mixed_case_names
FROM nodes 
WHERE node_type = 'Person' 
  AND node_id NOT IN (
    SELECT DISTINCT(node_id) 
    FROM attributes 
    WHERE attribute_type = 'computed_surname'
);

-- Show a sample of Person nodes missing attributes
SELECT 'Sample Person nodes without name attributes:' as info;
SELECT 
    node_id,
    primary_name,
    created_at
FROM nodes 
WHERE node_type = 'Person' 
  AND node_id NOT IN (
    SELECT DISTINCT(node_id) 
    FROM attributes 
    WHERE attribute_type = 'computed_surname'
)
ORDER BY created_at DESC
LIMIT 10;

-- Create the parse_person_name function if it doesn't exist
CREATE OR REPLACE FUNCTION parse_person_name(p_full_name TEXT)
RETURNS TABLE(first_name TEXT, middle_name TEXT, last_name TEXT) AS $$
DECLARE
    v_name_parts TEXT[];
    v_num_parts INTEGER;
    v_clean_name TEXT;
BEGIN
    -- Clean the name - normalize spaces
    v_clean_name := REGEXP_REPLACE(TRIM(p_full_name), '\s+', ' ', 'g');
    
    -- Split the name
    v_name_parts := string_to_array(v_clean_name, ' ');
    v_num_parts := array_length(v_name_parts, 1);
    
    -- Handle NULL or empty
    IF v_num_parts IS NULL OR v_num_parts = 0 THEN
        RETURN QUERY SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT;
        RETURN;
    END IF;
    
    -- Parse based on number of parts
    IF v_num_parts = 1 THEN
        -- Single name - treat as last name
        RETURN QUERY SELECT NULL::TEXT, NULL::TEXT, v_name_parts[1];
    ELSIF v_num_parts = 2 THEN
        -- First Last
        RETURN QUERY SELECT v_name_parts[1], NULL::TEXT, v_name_parts[2];
    ELSIF v_num_parts = 3 THEN
        -- First Middle Last
        RETURN QUERY SELECT v_name_parts[1], v_name_parts[2], v_name_parts[3];
    ELSE
        -- 4+ parts: First [Middle parts...] Last
        -- Combine middle parts
        RETURN QUERY SELECT 
            v_name_parts[1],
            array_to_string(v_name_parts[2:v_num_parts-1], ' '),
            v_name_parts[v_num_parts];
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Parse all Person node names at once
WITH parsed_names AS (
    SELECT 
        n.node_id,
        n.primary_name,
        (parse_person_name(n.primary_name)).*
    FROM nodes n
    WHERE n.node_type = 'Person'
      AND n.node_id NOT IN (
        SELECT DISTINCT(node_id) 
        FROM attributes 
        WHERE attribute_type = 'computed_surname'
    )
)
-- Add computed_first_name for all Person nodes missing it
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    node_id,
    'computed_first_name',
    first_name,
    'migration_v18'
FROM parsed_names
WHERE first_name IS NOT NULL
  AND first_name != '';

-- Add computed_surname using the same CTE approach
WITH parsed_names AS (
    SELECT 
        n.node_id,
        n.primary_name,
        (parse_person_name(n.primary_name)).*
    FROM nodes n
    WHERE n.node_type = 'Person'
      AND n.node_id NOT IN (
        SELECT DISTINCT(node_id) 
        FROM attributes 
        WHERE attribute_type = 'computed_surname'
    )
)
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    node_id,
    'computed_surname',
    last_name,
    'migration_v18'
FROM parsed_names
WHERE last_name IS NOT NULL
  AND last_name != '';

-- Add computed_additional_name (middle name) for Person nodes with 3+ word names
WITH parsed_names AS (
    SELECT 
        n.node_id,
        n.primary_name,
        (parse_person_name(n.primary_name)).*
    FROM nodes n
    WHERE n.node_type = 'Person'
      AND n.node_id NOT IN (
        SELECT DISTINCT(node_id) 
        FROM attributes 
        WHERE attribute_type = 'computed_additional_name'
    )
)
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT 
    node_id,
    'computed_additional_name',
    middle_name,
    'migration_v18'
FROM parsed_names
WHERE middle_name IS NOT NULL
  AND middle_name != '';

-- Show results
SELECT 'Migration complete. Summary:' as info;

WITH stats AS (
    SELECT 
        COUNT(DISTINCT node_id) FILTER (WHERE attribute_type = 'computed_first_name') as first_names_added,
        COUNT(DISTINCT node_id) FILTER (WHERE attribute_type = 'computed_surname') as last_names_added,
        COUNT(DISTINCT node_id) FILTER (WHERE attribute_type = 'computed_additional_name') as middle_names_added
    FROM attributes 
    WHERE created_by = 'migration_v18'
)
SELECT 
    'First names added' as attribute,
    first_names_added as count
FROM stats
UNION ALL
SELECT 
    'Last names added',
    last_names_added
FROM stats
UNION ALL
SELECT 
    'Middle names added',
    middle_names_added
FROM stats;

-- Verify all Person nodes now have at least surname
SELECT 'Person nodes still missing computed_surname:' as info;
SELECT COUNT(*) as remaining_without_surname
FROM nodes 
WHERE node_type = 'Person' 
  AND node_id NOT IN (
    SELECT DISTINCT(node_id) 
    FROM attributes 
    WHERE attribute_type = 'computed_surname'
);

-- Show sample of fixed nodes
SELECT 'Sample of Person nodes with new attributes:' as info;
SELECT 
    n.primary_name,
    MAX(CASE WHEN a.attribute_type = 'computed_first_name' THEN a.attribute_value END) as first_name,
    MAX(CASE WHEN a.attribute_type = 'computed_additional_name' THEN a.attribute_value END) as middle_name,
    MAX(CASE WHEN a.attribute_type = 'computed_surname' THEN a.attribute_value END) as last_name
FROM nodes n
JOIN attributes a ON n.node_id = a.node_id
WHERE n.node_type = 'Person'
  AND a.created_by = 'migration_v18'
  AND a.attribute_type IN ('computed_first_name', 'computed_surname', 'computed_additional_name')
GROUP BY n.node_id, n.primary_name
LIMIT 20;

-- Add comment about this migration
COMMENT ON FUNCTION parse_person_name IS 'Parses a full name into first, middle, and last name components - added in V18 migration';