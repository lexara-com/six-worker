-- =============================================
-- V19: Create Trigger to Automatically Compute Person Name Attributes
-- =============================================
-- This creates a trigger that automatically adds computed name attributes
-- whenever a Person node is inserted or updated
-- =============================================

-- Create function to automatically compute and add name attributes
CREATE OR REPLACE FUNCTION auto_compute_person_name_attributes()
RETURNS TRIGGER AS $$
DECLARE
    v_parsed_name RECORD;
BEGIN
    -- Only process Person nodes
    IF NEW.node_type != 'Person' THEN
        RETURN NEW;
    END IF;
    
    -- Parse the name
    SELECT * INTO v_parsed_name 
    FROM parse_person_name(NEW.primary_name);
    
    -- Add computed_first_name if present
    IF v_parsed_name.first_name IS NOT NULL AND v_parsed_name.first_name != '' THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
        VALUES (NEW.node_id, 'computed_first_name', v_parsed_name.first_name, 'auto_trigger')
        ON CONFLICT DO NOTHING;
    END IF;
    
    -- Add computed_surname if present
    IF v_parsed_name.last_name IS NOT NULL AND v_parsed_name.last_name != '' THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
        VALUES (NEW.node_id, 'computed_surname', v_parsed_name.last_name, 'auto_trigger')
        ON CONFLICT DO NOTHING;
    END IF;
    
    -- Add computed_additional_name if present
    IF v_parsed_name.middle_name IS NOT NULL AND v_parsed_name.middle_name != '' THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
        VALUES (NEW.node_id, 'computed_additional_name', v_parsed_name.middle_name, 'auto_trigger')
        ON CONFLICT DO NOTHING;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on nodes table
DROP TRIGGER IF EXISTS auto_compute_person_names ON nodes;
CREATE TRIGGER auto_compute_person_names
    AFTER INSERT OR UPDATE OF primary_name, node_type ON nodes
    FOR EACH ROW
    WHEN (NEW.node_type = 'Person')
    EXECUTE FUNCTION auto_compute_person_name_attributes();

-- Add unique constraint to prevent duplicate attributes
-- This allows ON CONFLICT DO NOTHING to work
ALTER TABLE attributes 
    DROP CONSTRAINT IF EXISTS unique_node_attribute_type;
    
ALTER TABLE attributes
    ADD CONSTRAINT unique_node_attribute_type 
    UNIQUE (node_id, attribute_type);

-- Now add attributes for all existing Person nodes without them
SELECT 'Adding name attributes to existing Person nodes without them...' as info;

WITH person_nodes_missing_attrs AS (
    SELECT node_id, primary_name
    FROM nodes 
    WHERE node_type = 'Person' 
      AND node_id NOT IN (
        SELECT DISTINCT(node_id) 
        FROM attributes 
        WHERE attribute_type = 'computed_surname'
    )
),
parsed_names AS (
    SELECT 
        node_id,
        primary_name,
        (parse_person_name(primary_name)).*
    FROM person_nodes_missing_attrs
)
-- Add all three attributes in one go
INSERT INTO attributes (node_id, attribute_type, attribute_value, created_by)
SELECT node_id, 'computed_first_name', first_name, 'migration_v19'
FROM parsed_names
WHERE first_name IS NOT NULL AND first_name != ''
UNION ALL
SELECT node_id, 'computed_surname', last_name, 'migration_v19'
FROM parsed_names
WHERE last_name IS NOT NULL AND last_name != ''
UNION ALL
SELECT node_id, 'computed_additional_name', middle_name, 'migration_v19'
FROM parsed_names
WHERE middle_name IS NOT NULL AND middle_name != ''
ON CONFLICT (node_id, attribute_type) DO NOTHING;

-- Show results
SELECT 'Migration complete. Summary:' as info;

SELECT 
    'Person nodes processed' as metric,
    COUNT(DISTINCT node_id) as count
FROM attributes 
WHERE created_by = 'migration_v19'
UNION ALL
SELECT 
    'Total attributes added',
    COUNT(*)
FROM attributes 
WHERE created_by = 'migration_v19'
UNION ALL
SELECT 
    'Person nodes still without surname',
    COUNT(*)
FROM nodes 
WHERE node_type = 'Person' 
  AND node_id NOT IN (
    SELECT DISTINCT(node_id) 
    FROM attributes 
    WHERE attribute_type = 'computed_surname'
);

-- Test the trigger with a new Person node
SELECT 'Testing trigger with new Person node...' as info;

INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, created_by)
VALUES (
    generate_ulid(),
    'Person',
    'TEST TRIGGER PERSON',
    'test trigger person',
    'migration_v19_test'
);

-- Check if attributes were auto-created
SELECT 'Attributes created for TEST TRIGGER PERSON:' as info;
SELECT 
    attribute_type,
    attribute_value
FROM attributes
WHERE node_id IN (
    SELECT node_id FROM nodes 
    WHERE primary_name = 'TEST TRIGGER PERSON'
)
ORDER BY attribute_type;

-- Clean up test node
DELETE FROM attributes 
WHERE node_id IN (
    SELECT node_id FROM nodes 
    WHERE primary_name = 'TEST TRIGGER PERSON'
);

DELETE FROM nodes 
WHERE primary_name = 'TEST TRIGGER PERSON';

-- Add helpful comment
COMMENT ON TRIGGER auto_compute_person_names ON nodes IS 
'Automatically computes and adds name attributes (first, middle, last) whenever a Person node is created or renamed - Added in V19';

SELECT 'Trigger installed. All future Person nodes will automatically get name attributes.' as status;