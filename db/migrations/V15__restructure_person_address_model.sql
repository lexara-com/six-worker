-- =============================================
-- V15: Restructure Person and Address Data Model
-- =============================================
-- This migration:
-- 1. Creates proper Address nodes with geographic relationships
-- 2. Adds ZIP code nodes as separate entities
-- 3. Adds computed name attributes for Person nodes
-- 4. Transforms existing address/location attributes to relationships
-- =============================================

-- Create function to parse person names
CREATE OR REPLACE FUNCTION parse_person_name(p_full_name TEXT)
RETURNS TABLE(first_name TEXT, middle_name TEXT, last_name TEXT) AS $$
DECLARE
    v_name_parts TEXT[];
    v_first TEXT;
    v_middle TEXT;
    v_last TEXT;
BEGIN
    -- Clean and split the name
    v_name_parts := string_to_array(TRIM(p_full_name), ' ');
    
    IF array_length(v_name_parts, 1) >= 3 THEN
        -- Assume: First Middle Last
        v_first := v_name_parts[1];
        v_last := v_name_parts[array_length(v_name_parts, 1)];
        -- Everything in between is middle name(s)
        v_middle := array_to_string(v_name_parts[2:array_length(v_name_parts, 1)-1], ' ');
    ELSIF array_length(v_name_parts, 1) = 2 THEN
        -- Assume: First Last
        v_first := v_name_parts[1];
        v_last := v_name_parts[2];
        v_middle := NULL;
    ELSIF array_length(v_name_parts, 1) = 1 THEN
        -- Only one name part - treat as last name
        v_last := v_name_parts[1];
        v_first := NULL;
        v_middle := NULL;
    END IF;
    
    RETURN QUERY SELECT v_first, v_middle, v_last;
END;
$$ LANGUAGE plpgsql;

-- Create function to migrate person address attributes to proper nodes
CREATE OR REPLACE FUNCTION migrate_person_address_attributes(p_person_id VARCHAR(26))
RETURNS VOID AS $$
DECLARE
    v_address_attr TEXT;
    v_location_attr TEXT;
    v_full_address TEXT;
    v_address_id VARCHAR(26);
    v_city_name TEXT;
    v_state_code TEXT;
    v_zip_code TEXT;
    v_city_id VARCHAR(26);
    v_state_id VARCHAR(26);
    v_zip_id VARCHAR(26);
    v_person_name TEXT;
    v_first_name TEXT;
    v_middle_name TEXT;
    v_last_name TEXT;
BEGIN
    -- Get the person's name for parsing
    SELECT primary_name INTO v_person_name
    FROM nodes
    WHERE node_id = p_person_id;
    
    -- Parse the name and add computed name attributes
    SELECT first_name, middle_name, last_name 
    INTO v_first_name, v_middle_name, v_last_name
    FROM parse_person_name(v_person_name);
    
    -- Add computed name attributes to person
    IF v_first_name IS NOT NULL AND v_first_name != '' AND NOT EXISTS (
        SELECT 1 FROM attributes 
        WHERE node_id = p_person_id AND attribute_type = 'computed_first_name'
    ) THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, source, created_by)
        VALUES (p_person_id, 'computed_first_name', v_first_name, LOWER(v_first_name), 'name_parser', 'migration');
    END IF;
    
    IF v_middle_name IS NOT NULL AND v_middle_name != '' AND NOT EXISTS (
        SELECT 1 FROM attributes 
        WHERE node_id = p_person_id AND attribute_type = 'computed_additional_name'
    ) THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, source, created_by)
        VALUES (p_person_id, 'computed_additional_name', v_middle_name, LOWER(v_middle_name), 'name_parser', 'migration');
    END IF;
    
    IF v_last_name IS NOT NULL AND v_last_name != '' AND NOT EXISTS (
        SELECT 1 FROM attributes 
        WHERE node_id = p_person_id AND attribute_type = 'computed_surname'
    ) THEN
        INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, source, created_by)
        VALUES (p_person_id, 'computed_surname', v_last_name, LOWER(v_last_name), 'name_parser', 'migration');
    END IF;
    
    -- Get the current address attributes
    SELECT attribute_value INTO v_address_attr
    FROM attributes
    WHERE node_id = p_person_id
      AND attribute_type = 'address'
    LIMIT 1;
    
    SELECT attribute_value INTO v_location_attr
    FROM attributes
    WHERE node_id = p_person_id
      AND attribute_type = 'location'
    LIMIT 1;
    
    IF v_address_attr IS NOT NULL AND v_location_attr IS NOT NULL THEN
        -- Construct full address
        v_full_address := v_address_attr || ', ' || v_location_attr;
        
        -- Parse location components (assuming format: "CITY, STATE, ZIP")
        v_city_name := TRIM(SPLIT_PART(v_location_attr, ',', 1));
        v_state_code := TRIM(SPLIT_PART(v_location_attr, ',', 2));
        v_zip_code := TRIM(SPLIT_PART(v_location_attr, ',', 3));
        
        -- Find or create Address node
        SELECT node_id INTO v_address_id
        FROM nodes
        WHERE node_type = 'Address'
          AND normalized_name = normalize_name(v_full_address)
        LIMIT 1;
        
        IF v_address_id IS NULL THEN
            v_address_id := generate_ulid();
            INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
            VALUES (v_address_id, 'Address', v_full_address, normalize_name(v_full_address), 'fact_based', 'migration');
            
            -- Add USPS normalized address attribute (placeholder - would need actual USPS validation)
            INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, source, created_by)
            VALUES (v_address_id, 'usps_normalized_address', UPPER(v_full_address), normalize_name(v_full_address), 'migration', 'migration');
        END IF;
        
        -- Create Person -> Address relationship
        IF NOT EXISTS (
            SELECT 1 FROM relationships
            WHERE source_node_id = p_person_id
              AND target_node_id = v_address_id
              AND relationship_type = 'Located_At'
        ) THEN
            -- Create bidirectional relationship
            PERFORM create_bidirectional_relationship(
                p_person_id,
                v_address_id,
                'Located_At',
                'Location_Of',
                'migration',
                'system'
            );
        END IF;
        
        -- Find or create ZIP code node
        IF v_zip_code IS NOT NULL AND v_zip_code != '' THEN
            SELECT node_id INTO v_zip_id
            FROM nodes
            WHERE node_type = 'ZipCode'
              AND primary_name = v_zip_code
            LIMIT 1;
            
            IF v_zip_id IS NULL THEN
                v_zip_id := generate_ulid();
                INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
                VALUES (v_zip_id, 'ZipCode', v_zip_code, v_zip_code, 'reference', 'migration');
            END IF;
            
            -- Create Address -> ZipCode relationship
            IF NOT EXISTS (
                SELECT 1 FROM relationships
                WHERE source_node_id = v_address_id
                  AND target_node_id = v_zip_id
                  AND relationship_type = 'Located_In'
            ) THEN
                PERFORM create_bidirectional_relationship(
                    v_address_id,
                    v_zip_id,
                    'Located_In',
                    'Contains',
                    'migration',
                    'system'
                );
            END IF;
        END IF;
        
        -- Find City node (should already exist from imports)
        IF v_city_name IS NOT NULL THEN
            -- Look for city in reference entities first
            SELECT reference_id INTO v_city_id
            FROM reference_entities
            WHERE node_type = 'City'
              AND normalized_name = normalize_name(v_city_name)
            LIMIT 1;
            
            -- If not in reference, check nodes
            IF v_city_id IS NULL THEN
                SELECT node_id INTO v_city_id
                FROM nodes
                WHERE node_type = 'City'
                  AND normalized_name = normalize_name(v_city_name)
                LIMIT 1;
            END IF;
            
            -- Create Address -> City relationship
            IF v_city_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM relationships
                WHERE source_node_id = v_address_id
                  AND target_node_id = v_city_id
                  AND relationship_type = 'Located_In'
            ) THEN
                PERFORM create_bidirectional_relationship(
                    v_address_id,
                    v_city_id,
                    'Located_In',
                    'Contains',
                    'migration',
                    'system'
                );
            END IF;
            
            -- If ZIP exists and City exists, connect them
            IF v_zip_id IS NOT NULL AND v_city_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM relationships
                WHERE source_node_id = v_zip_id
                  AND target_node_id = v_city_id
                  AND relationship_type = 'Located_In'
            ) THEN
                PERFORM create_bidirectional_relationship(
                    v_zip_id,
                    v_city_id,
                    'Located_In',
                    'Contains',
                    'migration',
                    'system'
                );
            END IF;
        END IF;
        
        -- Find State node (should already exist)
        IF v_state_code IS NOT NULL THEN
            SELECT node_id INTO v_state_id
            FROM nodes
            WHERE node_type = 'State'
              AND (normalized_name = normalize_name(v_state_code) 
                   OR normalized_name = normalize_name(
                       CASE v_state_code
                           WHEN 'IA' THEN 'Iowa'
                           WHEN 'IL' THEN 'Illinois'
                           WHEN 'MN' THEN 'Minnesota'
                           WHEN 'WI' THEN 'Wisconsin'
                           WHEN 'MO' THEN 'Missouri'
                           WHEN 'NE' THEN 'Nebraska'
                           WHEN 'SD' THEN 'South Dakota'
                           ELSE v_state_code
                       END
                   ))
            LIMIT 1;
            
            -- Create Address -> State relationship
            IF v_state_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM relationships
                WHERE source_node_id = v_address_id
                  AND target_node_id = v_state_id
                  AND relationship_type = 'Located_In'
            ) THEN
                PERFORM create_bidirectional_relationship(
                    v_address_id,
                    v_state_id,
                    'Located_In',
                    'Contains',
                    'migration',
                    'system'
                );
            END IF;
        END IF;
        
        -- Remove the original address/location attributes (keeping role)
        DELETE FROM attributes
        WHERE node_id = p_person_id
          AND attribute_type IN ('address', 'location');
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Migrate all Person nodes with address attributes
DO $$
DECLARE
    v_person_record RECORD;
    v_migrated_count INT := 0;
BEGIN
    FOR v_person_record IN 
        SELECT DISTINCT node_id 
        FROM attributes 
        WHERE attribute_type IN ('address', 'location')
          AND node_id IN (SELECT node_id FROM nodes WHERE node_type = 'Person')
    LOOP
        PERFORM migrate_person_address_attributes(v_person_record.node_id);
        v_migrated_count := v_migrated_count + 1;
        
        -- Log progress every 100 records
        IF v_migrated_count % 100 = 0 THEN
            RAISE NOTICE 'Migrated % person records...', v_migrated_count;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Migration complete. Migrated % person records total.', v_migrated_count;
END $$;

-- Add node_type enum for ZipCode if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum 
        WHERE enumlabel = 'ZipCode' 
          AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'nodetype')
    ) THEN
        ALTER TYPE NodeType ADD VALUE 'ZipCode' AFTER 'Address';
    END IF;
END $$;

-- Show migration results
SELECT 'Migration Summary:' as status;

SELECT 
    'Node Type Counts:' as metric,
    node_type,
    COUNT(*) as count
FROM nodes
WHERE node_type IN ('Person', 'Address', 'ZipCode', 'City', 'State')
GROUP BY node_type
ORDER BY node_type;

SELECT 
    'Person Name Attributes:' as metric,
    attribute_type,
    COUNT(*) as count
FROM attributes
WHERE attribute_type IN ('computed_first_name', 'computed_additional_name', 'computed_surname')
GROUP BY attribute_type;

SELECT 
    'Address Relationships:' as metric,
    relationship_type,
    COUNT(*) as count
FROM relationships
WHERE relationship_type IN ('Located_At', 'Located_In', 'Location_Of', 'Contains')
  AND created_by = 'migration'
GROUP BY relationship_type;

-- Example: Show transformed data for JOEL W TUBBS
SELECT 'Example - JOEL W TUBBS transformed data:' as status;

SELECT 
    'Person Attributes:' as category,
    attribute_type,
    attribute_value
FROM attributes
WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY attribute_type;

SELECT 
    'Person Relationships:' as category,
    r.relationship_type,
    n.node_type as target_type,
    n.primary_name as target_name
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR'
ORDER BY r.relationship_type;