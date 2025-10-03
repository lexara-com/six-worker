-- =============================================
-- Update Propose API for Reference Entity Handling
-- Version: 7.0
-- Purpose: Modify entity resolution to handle reference entities efficiently
-- =============================================

-- Update the resolve_entity function to handle reference entities
CREATE OR REPLACE FUNCTION resolve_entity(
    p_node_type VARCHAR(50),
    p_primary_name VARCHAR(255),
    p_attributes JSONB DEFAULT '[]'::JSONB,
    p_confidence_threshold DECIMAL(3,2) DEFAULT 0.8
) RETURNS entity_resolution_result AS $$
DECLARE
    similar_entities JSONB;
    best_match JSONB;
    best_confidence DECIMAL(3,2) := 0.0;
    entity_id VARCHAR(26);
    result entity_resolution_result;
    attr JSONB;
BEGIN
    -- First, check if this is a reference entity we should create/match
    entity_id := get_or_create_reference_entity(p_node_type, p_primary_name);
    
    IF entity_id IS NOT NULL THEN
        -- This is a reference entity - return it without additional provenance
        result.action := 'matched';
        result.entity_id := entity_id;
        result.confidence := 1.0;
        result.match_reason := 'reference_entity';
        result.alternatives := '[]'::JSONB;
        RETURN result;
    END IF;
    
    -- Not a reference entity, proceed with normal entity resolution
    similar_entities := find_similar_entities(p_node_type, p_primary_name, p_attributes, p_confidence_threshold);
    
    -- Evaluate the best match
    IF jsonb_array_length(similar_entities) > 0 THEN
        SELECT jsonb_array_elements(similar_entities) INTO best_match
        ORDER BY (jsonb_array_elements(similar_entities)->>'confidence')::DECIMAL DESC
        LIMIT 1;
        
        best_confidence := (best_match->>'confidence')::DECIMAL;
    END IF;
    
    -- Decision logic for non-reference entities
    IF best_confidence >= p_confidence_threshold THEN
        -- High confidence match found
        result.action := 'matched';
        result.entity_id := best_match->>'entity_id';
        result.confidence := best_confidence;
        result.match_reason := best_match->>'match_reason';
        result.alternatives := similar_entities;
    ELSIF best_confidence >= 0.5 THEN
        -- Ambiguous - multiple possible matches
        result.action := 'ambiguous';
        result.entity_id := NULL;
        result.confidence := best_confidence;
        result.match_reason := 'multiple_candidates';
        result.alternatives := similar_entities;
    ELSE
        -- No good match - create new entity (fact-based by default)
        entity_id := generate_ulid();
        
        INSERT INTO nodes (node_id, node_type, primary_name, entity_class, created_by, status)
        VALUES (entity_id, p_node_type, p_primary_name, 'fact_based', 'propose_api', 'active');
        
        -- Add attributes
        FOR attr IN SELECT * FROM jsonb_array_elements(p_attributes)
        LOOP
            INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
            VALUES (generate_ulid(), entity_id, attr->>'type', attr->>'value', 'propose_api', 'active');
        END LOOP;
        
        result.action := 'created';
        result.entity_id := entity_id;
        result.confidence := 1.0;
        result.match_reason := 'new_entity';
        result.alternatives := '[]'::JSONB;
    END IF;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Update the create_provenance_record function to handle reference entities
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
            SELECT provenance_id INTO provenance_id
            FROM provenance 
            WHERE asset_id = p_asset_id AND asset_type = 'node'
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

-- Create a function to check if an entity should be promoted to reference status
CREATE OR REPLACE FUNCTION analyze_reference_entity_candidates() 
RETURNS TABLE(
    node_id VARCHAR(26),
    primary_name VARCHAR(255),
    node_type VARCHAR(50),
    provenance_count INTEGER,
    unique_sources INTEGER,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        n.node_id,
        n.primary_name,
        n.node_type,
        COUNT(p.provenance_id)::INTEGER as provenance_count,
        COUNT(DISTINCT p.source_type)::INTEGER as unique_sources,
        CASE 
            WHEN COUNT(p.provenance_id) > 100 THEN 'PROMOTE: High provenance volume'
            WHEN COUNT(p.provenance_id) > 50 AND COUNT(DISTINCT p.source_type) > 5 THEN 'CONSIDER: Multiple diverse sources'
            WHEN COUNT(p.provenance_id) > 20 AND n.node_type = 'Place' THEN 'CONSIDER: Geographic entity with substantial evidence'
            ELSE 'MONITOR: Continue current tracking'
        END as recommendation
    FROM nodes n
    LEFT JOIN provenance p ON n.node_id = p.asset_id AND p.asset_type = 'node'
    WHERE n.entity_class = 'fact_based'
    GROUP BY n.node_id, n.primary_name, n.node_type
    HAVING COUNT(p.provenance_id) > 10  -- Only show entities with significant provenance
    ORDER BY COUNT(p.provenance_id) DESC, COUNT(DISTINCT p.source_type) DESC;
END;
$$ LANGUAGE plpgsql;

-- Test the updated system
\echo '=== Reference Entity System Test ==='

\echo '1. Test reference entity resolution:'
SELECT * FROM resolve_entity('Place', 'State of Iowa');

\echo '2. Test normal entity resolution:'
SELECT * FROM resolve_entity('Company', 'Test Corporation XYZ');

\echo '3. Current State of Iowa provenance (should be minimal):'
SELECT COUNT(*) as provenance_count
FROM provenance p
JOIN nodes n ON p.asset_id = n.node_id
WHERE n.normalized_name = 'state of iowa' 
  AND p.asset_type = 'node';

\echo '4. Reference entity candidates:'
SELECT * FROM analyze_reference_entity_candidates() LIMIT 5;

\echo '5. Entity class distribution:'
SELECT 
    entity_class,
    COUNT(*) as entity_count,
    AVG(prov_count.cnt) as avg_provenance_per_entity
FROM nodes n
LEFT JOIN (
    SELECT asset_id, COUNT(*) as cnt 
    FROM provenance 
    WHERE asset_type = 'node' 
    GROUP BY asset_id
) prov_count ON n.node_id = prov_count.asset_id
GROUP BY entity_class
ORDER BY entity_class;