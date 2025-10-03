-- =============================================
-- Clean Up Function Signatures
-- Version: 9.0
-- Purpose: Remove duplicate functions and create clean versions
-- =============================================

-- Drop all versions of create_provenance_record
DROP FUNCTION IF EXISTS create_provenance_record CASCADE;

-- Create the single, correct version of create_provenance_record
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

-- Now test the clean system
\echo '=== Testing Clean Function System ==='

\echo '1. Test propose_fact with reference entity (State of California):'
SELECT * FROM propose_fact(
    'Company', 'California Test Company',
    'Place', 'State of California', 
    'Incorporated_In',
    'Clean Function Test', 'manual_test'
);

\echo '2. Test multiple calls to same reference entity:'
SELECT * FROM propose_fact(
    'Company', 'Another California Company',
    'Place', 'State of California', 
    'Incorporated_In',
    'Second California Test', 'manual_test'
);

\echo '3. Check State of California provenance count:'
SELECT COUNT(*) as provenance_count
FROM provenance p
JOIN nodes n ON p.asset_id = n.node_id
WHERE n.normalized_name = 'state of california' 
  AND p.asset_type = 'node';