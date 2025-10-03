-- =============================================
-- Transform Person Attributes to Relationships
-- =============================================
-- This script demonstrates how to convert address/location
-- attributes into proper node relationships
-- =============================================

BEGIN;

-- Example for transforming JOEL W TUBBS node
DO $$
DECLARE
    v_person_id VARCHAR(26) := '01K6GMKE4Z0GHW7637XAB9KYVR';
    v_address_attr TEXT;
    v_location_attr TEXT;
    v_full_address TEXT;
    v_address_id VARCHAR(26);
    v_city_name TEXT;
    v_state_code TEXT;
    v_zip_code TEXT;
    v_city_id VARCHAR(26);
BEGIN
    -- Get the current attributes
    SELECT attribute_value INTO v_address_attr
    FROM attributes
    WHERE node_id = v_person_id
      AND attribute_type = 'address'
    LIMIT 1;
    
    SELECT attribute_value INTO v_location_attr
    FROM attributes
    WHERE node_id = v_person_id
      AND attribute_type = 'location'
    LIMIT 1;
    
    IF v_address_attr IS NOT NULL AND v_location_attr IS NOT NULL THEN
        -- Construct full address
        v_full_address := v_address_attr || ', ' || v_location_attr;
        
        -- Parse location components (assuming format: "CITY, STATE, ZIP")
        -- This is simplified - production code should handle various formats
        v_city_name := SPLIT_PART(v_location_attr, ',', 1);
        v_state_code := TRIM(SPLIT_PART(v_location_attr, ',', 2));
        v_zip_code := TRIM(SPLIT_PART(v_location_attr, ',', 3));
        
        -- Check if address node already exists
        SELECT node_id INTO v_address_id
        FROM nodes
        WHERE node_type = 'Address'
          AND normalized_name = normalize_name(v_full_address)
        LIMIT 1;
        
        -- Create address node if it doesn't exist
        IF v_address_id IS NULL THEN
            v_address_id := generate_ulid();
            INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
            VALUES (v_address_id, 'Address', v_full_address, normalize_name(v_full_address), 'fact_based', 'attribute_migration');
            
            -- Add zip code as attribute on address node
            IF v_zip_code IS NOT NULL THEN
                INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value, source)
                VALUES (v_address_id, 'zip_code', v_zip_code, v_zip_code, 'attribute_migration');
            END IF;
        END IF;
        
        -- Create Person -> Address relationship
        IF NOT EXISTS (
            SELECT 1 FROM relationships
            WHERE source_node_id = v_person_id
              AND target_node_id = v_address_id
              AND relationship_type = 'Located_At'
        ) THEN
            INSERT INTO relationships (
                relationship_id,
                source_node_id,
                target_node_id,
                relationship_type,
                created_by
            ) VALUES (
                generate_ulid(),
                v_person_id,
                v_address_id,
                'Located_At',
                'attribute_migration'
            );
            
            -- Create reverse relationship
            INSERT INTO relationships (
                relationship_id,
                source_node_id,
                target_node_id,
                relationship_type,
                created_by
            ) VALUES (
                generate_ulid(),
                v_address_id,
                v_person_id,
                'Location_Of',
                'attribute_migration'
            );
        END IF;
        
        -- Find or create city node
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
            
            -- Create Address -> City relationship if city exists
            IF v_city_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM relationships
                WHERE source_node_id = v_address_id
                  AND target_node_id = v_city_id
                  AND relationship_type = 'Located_In'
            ) THEN
                INSERT INTO relationships (
                    relationship_id,
                    source_node_id,
                    target_node_id,
                    relationship_type,
                    created_by
                ) VALUES (
                    generate_ulid(),
                    v_address_id,
                    v_city_id,
                    'Located_In',
                    'attribute_migration'
                );
                
                -- Create reverse relationship
                INSERT INTO relationships (
                    relationship_id,
                    source_node_id,
                    target_node_id,
                    relationship_type,
                    created_by
                ) VALUES (
                    generate_ulid(),
                    v_city_id,
                    v_address_id,
                    'Contains',
                    'attribute_migration'
                );
            END IF;
        END IF;
        
        -- Remove the original attributes (optional - might want to keep for audit)
        -- DELETE FROM attributes
        -- WHERE node_id = v_person_id
        --   AND attribute_type IN ('address', 'location');
        
        RAISE NOTICE 'Migrated address attributes for person % to relationships', v_person_id;
    END IF;
END $$;

-- Show the results
SELECT 'After Migration - Person Node:' as status;
SELECT * FROM nodes WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR';

SELECT 'After Migration - Person Relationships:' as status;
SELECT 
    r.relationship_type,
    r.target_node_id,
    n.node_type as target_type,
    n.primary_name as target_name
FROM relationships r
JOIN nodes n ON n.node_id = r.target_node_id
WHERE r.source_node_id = '01K6GMKE4Z0GHW7637XAB9KYVR';

SELECT 'After Migration - Person Attributes:' as status;
SELECT * FROM attributes WHERE node_id = '01K6GMKE4Z0GHW7637XAB9KYVR';

ROLLBACK; -- Change to COMMIT when ready to apply

-- =============================================
-- General recommendations for attribute migration:
-- =============================================
-- 1. Address/Location attributes → Address nodes with Located_At relationships
-- 2. Officer/Director name attributes → Person nodes with Officer_Of relationships  
-- 3. Parent company attributes → Company nodes with Parent_Of relationships
-- 4. Industry attributes → Industry reference nodes with Operates_In relationships
-- 5. Keep as attributes:
--    - Identifiers (EIN, corp_number, license numbers)
--    - Dates (incorporation date, expiry dates)
--    - Status fields
--    - Contact info (unless creating dedicated ContactInfo nodes)
-- =============================================