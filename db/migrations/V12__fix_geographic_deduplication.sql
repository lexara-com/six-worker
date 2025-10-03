-- =============================================
-- V12: Fix Geographic Entity Deduplication
-- =============================================
-- Problem: The propose_geographic_fact function was not checking reference_entities
--          first, causing massive duplication of city entities during import
--
-- Solution: Update the function to check reference_entities table for existing
--           geographic entities before creating new ones
-- =============================================

-- Drop the existing function to replace it
DROP FUNCTION IF EXISTS propose_geographic_fact;

-- Create improved version that checks reference entities first
CREATE OR REPLACE FUNCTION propose_geographic_fact(
    p_entity_name VARCHAR(255),
    p_entity_type VARCHAR(50),
    p_location_name VARCHAR(255),  
    p_location_type VARCHAR(50),
    p_address TEXT,
    p_coordinates JSONB,
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50)
) RETURNS JSONB AS $$
DECLARE
    v_entity_id VARCHAR(26);
    v_location_id VARCHAR(26);
    v_address_id VARCHAR(26);
    v_state_id VARCHAR(26);
    v_forward_rel_id VARCHAR(26);
    v_reverse_rel_id VARCHAR(26);
    v_result JSONB := '{}';
    v_relationships JSONB := '[]';
    v_normalized_location VARCHAR(255);
BEGIN
    -- Normalize the location name for matching
    v_normalized_location := normalize_name(p_location_name);
    
    -- CRITICAL FIX: Check reference_entities FIRST for the location
    IF p_location_type IN ('City', 'State', 'Country', 'County') THEN
        -- Look for existing reference entity
        SELECT re.reference_id INTO v_location_id
        FROM reference_entities re
        WHERE re.node_type = p_location_type
          AND re.normalized_name = v_normalized_location
        LIMIT 1;
        
        -- If found in reference entities, create the corresponding node if it doesn't exist
        IF v_location_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM nodes WHERE node_id = v_location_id
        ) THEN
            INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
            SELECT 
                reference_id,
                node_type,
                primary_name,
                normalized_name,
                'reference',
                'geographic_system'
            FROM reference_entities
            WHERE reference_id = v_location_id;
        END IF;
    END IF;
    
    -- If not found in reference entities, check existing nodes
    IF v_location_id IS NULL THEN
        SELECT node_id INTO v_location_id
        FROM nodes
        WHERE node_type = p_location_type
          AND normalized_name = v_normalized_location
          AND status = 'active'
        ORDER BY 
            CASE entity_class 
                WHEN 'reference' THEN 1
                WHEN 'fact_based' THEN 2
                ELSE 3
            END,
            created_at ASC
        LIMIT 1;
    END IF;
    
    -- Only create new location if it doesn't exist at all
    IF v_location_id IS NULL THEN
        v_location_id := generate_ulid();
        
        INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
        VALUES (v_location_id, p_location_type, INITCAP(p_location_name), v_normalized_location, 'fact_based', 'geographic_system');
        
        -- For new cities, try to establish state relationship
        IF p_location_type = 'City' THEN
            -- Try to find Iowa state reference entity
            SELECT node_id INTO v_state_id
            FROM nodes
            WHERE node_type = 'State'
              AND normalized_name = 'iowa'
              AND entity_class = 'reference'
            LIMIT 1;
            
            IF v_state_id IS NOT NULL THEN
                -- Create bidirectional relationship to state
                PERFORM create_bidirectional_relationship(
                    v_location_id,
                    v_state_id,
                    'Located_In',
                    'Contains',
                    p_source_name,
                    p_source_type
                );
            END IF;
        END IF;
    END IF;
    
    -- Create or find the main entity
    SELECT node_id INTO v_entity_id
    FROM nodes
    WHERE node_type = p_entity_type
      AND normalized_name = normalize_name(p_entity_name)
      AND status = 'active'
    LIMIT 1;
    
    IF v_entity_id IS NULL THEN
        v_entity_id := generate_ulid();
        INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, created_by)
        VALUES (v_entity_id, p_entity_type, p_entity_name, normalize_name(p_entity_name), 'geographic_system');
    END IF;
    
    -- Create address if provided
    IF p_address IS NOT NULL AND p_address != '' THEN
        v_address_id := generate_ulid();
        
        INSERT INTO nodes (node_id, node_type, primary_name, normalized_name, entity_class, created_by)
        VALUES (v_address_id, 'Address', p_address, normalize_name(p_address), 'fact_based', 'geographic_system');
        
        -- Add coordinates if provided
        IF p_coordinates IS NOT NULL THEN
            INSERT INTO attributes (node_id, attribute_type, attribute_value, source)
            VALUES (v_address_id, 'coordinates', p_coordinates::TEXT, 'geographic_system');
        END IF;
        
        -- Create entity -> address relationship
        SELECT (create_bidirectional_relationship(
            v_entity_id,
            v_address_id,
            'Located_At',
            'Location_Of',
            p_source_name,
            p_source_type
        ))->>'forward_id' INTO v_forward_rel_id;
        
        v_relationships := v_relationships || jsonb_build_object(
            'forward_id', v_forward_rel_id,
            'reverse_id', (SELECT relationship_id FROM relationships 
                          WHERE source_node_id = v_address_id 
                            AND target_node_id = v_entity_id 
                            AND relationship_type = 'Location_Of'
                          LIMIT 1),
            'forward_type', 'Located_At',
            'reverse_type', 'Location_Of'
        );
        
        -- Create address -> location relationship
        IF v_location_id IS NOT NULL THEN
            PERFORM create_bidirectional_relationship(
                v_address_id,
                v_location_id,
                'Located_In',
                'Contains',
                p_source_name,
                p_source_type
            );
        END IF;
    END IF;
    
    -- Create entity -> location relationship if no address
    IF v_address_id IS NULL AND v_location_id IS NOT NULL THEN
        SELECT (create_bidirectional_relationship(
            v_entity_id,
            v_location_id,
            'Located_In',
            'Contains',
            p_source_name,
            p_source_type
        ))->>'forward_id' INTO v_forward_rel_id;
        
        v_relationships := v_relationships || jsonb_build_object(
            'forward_id', v_forward_rel_id,
            'reverse_id', (SELECT relationship_id FROM relationships 
                          WHERE source_node_id = v_location_id 
                            AND target_node_id = v_entity_id 
                            AND relationship_type = 'Contains'
                          LIMIT 1),
            'forward_type', 'Located_In',
            'reverse_type', 'Contains'
        );
    END IF;
    
    -- Add provenance for all created entities
    IF p_source_name IS NOT NULL THEN
        INSERT INTO provenance (provenance_id, asset_type, asset_id, source_name, source_type)
        SELECT generate_ulid(), 'node', id, p_source_name, p_source_type
        FROM unnest(ARRAY[v_entity_id, v_location_id, v_address_id]::VARCHAR[]) AS id
        WHERE id IS NOT NULL;
    END IF;
    
    -- Build result
    v_result := jsonb_build_object(
        'entity_id', v_entity_id,
        'location_id', v_location_id,
        'address_id', v_address_id,
        'relationships', v_relationships
    );
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Test the fix with a known duplicate city
DO $$
DECLARE
    v_result JSONB;
    v_dubuque_count_before INTEGER;
    v_dubuque_count_after INTEGER;
BEGIN
    -- Count Dubuque nodes before
    SELECT COUNT(*) INTO v_dubuque_count_before
    FROM nodes 
    WHERE normalized_name = 'dubuque' AND node_type = 'City';
    
    -- Test the function - should use existing reference entity
    v_result := propose_geographic_fact(
        'Test Company in Dubuque',
        'Company',
        'Dubuque',
        'City',
        '123 Test St, Dubuque, IA',
        NULL,
        'Test Fix',
        'test'
    );
    
    -- Count Dubuque nodes after
    SELECT COUNT(*) INTO v_dubuque_count_after
    FROM nodes 
    WHERE normalized_name = 'dubuque' AND node_type = 'City';
    
    -- Verify no new Dubuque was created
    IF v_dubuque_count_after > v_dubuque_count_before THEN
        RAISE NOTICE 'WARNING: New Dubuque node was created! Fix may not be working.';
    ELSE
        RAISE NOTICE 'SUCCESS: No new Dubuque node created. Using existing entity.';
    END IF;
    
    RAISE NOTICE 'Test result: %', v_result;
END $$;