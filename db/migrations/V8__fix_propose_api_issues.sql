-- =============================================
-- Fix Propose API Function Issues
-- Version: 8.0
-- Purpose: Fix function signature conflicts and type issues
-- =============================================

-- Fix the get_or_create_reference_entity function parameter types
CREATE OR REPLACE FUNCTION get_or_create_reference_entity(
    p_node_type VARCHAR(50),
    p_primary_name VARCHAR(255),
    p_source_name VARCHAR(255) DEFAULT 'system',
    p_source_type VARCHAR(50) DEFAULT 'reference_entities'
) RETURNS VARCHAR(26) AS $$
DECLARE
    entity_id VARCHAR(26);
    normalized_name_val VARCHAR(255);
    ref_entity RECORD;
    alias_text TEXT;
BEGIN
    normalized_name_val := normalize_name(p_primary_name);
    
    -- First, check if this matches a pre-defined reference entity
    SELECT * INTO ref_entity 
    FROM reference_entities 
    WHERE node_type = p_node_type 
      AND normalized_name = normalized_name_val;
    
    IF FOUND THEN
        -- Check if we already created this reference entity in nodes table
        SELECT node_id INTO entity_id 
        FROM nodes 
        WHERE node_type = p_node_type 
          AND normalized_name = normalized_name_val
          AND entity_class = 'reference';
        
        IF NOT FOUND THEN
            -- Create the reference entity in nodes table
            entity_id := generate_ulid();
            
            INSERT INTO nodes (
                node_id, node_type, primary_name, entity_class, 
                created_by, status
            ) VALUES (
                entity_id, p_node_type, ref_entity.primary_name, 'reference',
                'reference_system', 'active'
            );
            
            -- Create single authoritative provenance record with explicit type casting
            PERFORM create_provenance_record(
                'node'::VARCHAR(20), 
                entity_id::VARCHAR(26), 
                ref_entity.authority_source::VARCHAR(255), 
                'government_authority'::VARCHAR(50),
                'reference_system'::VARCHAR(100), 
                ref_entity.authority_confidence::DECIMAL(3,2), 
                'high'::VARCHAR(20)
            );
            
            -- Add aliases as attributes if they exist
            IF ref_entity.aliases IS NOT NULL AND jsonb_array_length(ref_entity.aliases) > 0 THEN
                FOR alias_text IN SELECT jsonb_array_elements_text(ref_entity.aliases)
                LOOP
                    INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
                    VALUES (generate_ulid(), entity_id, 'nameAlias', alias_text, 'reference_system', 'active');
                END LOOP;
            END IF;
        END IF;
        
        RETURN entity_id;
    ELSE
        -- Not a reference entity, return NULL to indicate normal processing needed
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Clean up any duplicate create_provenance_record functions and create the correct one
DROP FUNCTION IF EXISTS create_provenance_record(VARCHAR(20), VARCHAR(26), VARCHAR(255), VARCHAR(50), VARCHAR(100), DECIMAL(3,2), VARCHAR(20), JSONB);
DROP FUNCTION IF EXISTS create_provenance_record(VARCHAR(20), VARCHAR(26), VARCHAR(255), VARCHAR(50), VARCHAR(100), DECIMAL(3,2), VARCHAR(20));

-- Create the corrected create_provenance_record function
CREATE OR REPLACE FUNCTION create_provenance_record(
    p_asset_type VARCHAR(20),
    p_asset_id VARCHAR(26),
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50),
    p_created_by VARCHAR(100) DEFAULT 'system',
    p_confidence_score DECIMAL(3,2) DEFAULT 1.0,
    p_reliability_rating VARCHAR(20) DEFAULT 'high',
    p_metadata JSONB DEFAULT NULL
) RETURNS VARCHAR(26) AS $$
DECLARE
    provenance_id VARCHAR(26);
    is_reference_entity BOOLEAN := FALSE;
BEGIN
    -- Check if this is a reference entity (only for node assets)
    IF p_asset_type = 'node' THEN
        SELECT (entity_class = 'reference') INTO is_reference_entity
        FROM nodes 
        WHERE node_id = p_asset_id;
        
        -- For reference entities, only create provenance if none exists
        IF is_reference_entity AND EXISTS (
            SELECT 1 FROM provenance 
            WHERE asset_id = p_asset_id AND asset_type = 'node'
        ) THEN
            -- Reference entity already has provenance, return existing ID
            SELECT p.provenance_id INTO provenance_id
            FROM provenance p
            WHERE p.asset_id = p_asset_id AND p.asset_type = 'node'
            LIMIT 1;
            
            RETURN provenance_id;
        END IF;
    END IF;
    
    -- Generate ULID for provenance record
    provenance_id := generate_ulid();
    
    -- Ensure source_type exists in source_types table
    INSERT INTO source_types (source_type, description, default_reliability, requires_license)
    VALUES (p_source_type, 'Auto-created: ' || p_source_type, 'medium', FALSE)
    ON CONFLICT (source_type) DO NOTHING;
    
    -- Insert provenance record
    INSERT INTO provenance (
        provenance_id, asset_type, asset_id, source_name, source_type,
        confidence_score, reliability_rating, data_obtained_at, created_by, status, metadata
    ) VALUES (
        provenance_id, p_asset_type, p_asset_id, p_source_name, p_source_type,
        p_confidence_score, p_reliability_rating, CURRENT_TIMESTAMP, p_created_by, 'active', p_metadata
    );
    
    RETURN provenance_id;
END;
$$ LANGUAGE plpgsql;

-- Test the fixed functions
\echo '=== Testing Fixed Functions ==='

\echo '1. Test reference entity resolution:'
SELECT * FROM resolve_entity('Place', 'State of Texas');

\echo '2. Test propose_fact with reference entity:'
SELECT * FROM propose_fact(
    'Company', 'Test Company Fixed',
    'Place', 'State of Texas', 
    'Incorporated_In',
    'Function Fix Test', 'manual_test'
);