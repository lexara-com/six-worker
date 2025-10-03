-- =============================================
-- Six Worker "Propose" API for Intelligent Fact Ingestion
-- Version: 4.0 (Clean deployment)
-- =============================================

-- Clean up any existing types and functions
DROP TYPE IF EXISTS entity_resolution_result CASCADE;
DROP TYPE IF EXISTS relationship_evaluation_result CASCADE;
DROP TYPE IF EXISTS propose_api_response CASCADE;
DROP FUNCTION IF EXISTS find_similar_entities CASCADE;
DROP FUNCTION IF EXISTS resolve_entity CASCADE;
DROP FUNCTION IF EXISTS find_existing_relationships CASCADE;
DROP FUNCTION IF EXISTS evaluate_relationship CASCADE;
DROP FUNCTION IF EXISTS propose_fact CASCADE;

-- Custom type for entity resolution results
CREATE TYPE entity_resolution_result AS (
    action VARCHAR(20),           -- 'matched', 'created', 'ambiguous'
    entity_id VARCHAR(26),        -- ULID of matched/created entity
    confidence DECIMAL(3,2),      -- Confidence in the resolution (0.0-1.0)
    match_reason VARCHAR(50),     -- How the match was made
    alternatives JSONB            -- Alternative matches for ambiguous cases
);

-- Custom type for relationship evaluation results
CREATE TYPE relationship_evaluation_result AS (
    action VARCHAR(20),           -- 'created', 'updated', 'conflicted', 'duplicate'
    relationship_id VARCHAR(26),  -- ULID of relationship
    confidence DECIMAL(3,2),      -- Confidence in the relationship
    conflicts JSONB,              -- Array of conflicting relationships
    strength_delta DECIMAL(3,2)   -- Change in relationship strength
);

-- Custom type for the main propose API response
CREATE TYPE propose_api_response AS (
    status VARCHAR(20),           -- 'success', 'conflicts', 'error'
    overall_confidence DECIMAL(3,2), -- Overall confidence in the operation
    actions JSONB,                -- Array of actions taken
    conflicts JSONB,              -- Array of conflicts detected
    provenance_ids JSONB          -- Array of provenance record IDs created
);

-- Function to find similar entities using multiple matching strategies
CREATE OR REPLACE FUNCTION find_similar_entities(
    p_node_type VARCHAR(50),
    p_primary_name VARCHAR(255),
    p_attributes JSONB DEFAULT '[]'::JSONB,
    p_confidence_threshold DECIMAL(3,2) DEFAULT 0.7
) RETURNS JSONB AS $$
DECLARE
    matches JSONB := '[]'::JSONB;
    match_record RECORD;
    attr JSONB;
    similarity_score DECIMAL(3,2);
BEGIN
    -- Strategy 1: Exact primary name match
    FOR match_record IN
        SELECT n.node_id, n.primary_name, 1.0 as score, 'exact_name_match' as reason
        FROM nodes n
        WHERE n.node_type = p_node_type 
          AND n.normalized_name = normalize_name(p_primary_name)
          AND n.status = 'active'
    LOOP
        matches := matches || jsonb_build_object(
            'entity_id', match_record.node_id,
            'confidence', match_record.score,
            'match_reason', match_record.reason,
            'matched_name', match_record.primary_name
        );
    END LOOP;
    
    -- Strategy 2: Alias matches
    FOR attr IN SELECT * FROM jsonb_array_elements(p_attributes)
    LOOP
        IF attr->>'type' = 'nameAlias' THEN
            FOR match_record IN
                SELECT DISTINCT n.node_id, n.primary_name, 0.9 as score, 'alias_match' as reason
                FROM nodes n
                JOIN attributes a ON n.node_id = a.node_id
                WHERE n.node_type = p_node_type
                  AND a.attribute_type = 'nameAlias'
                  AND a.normalized_value = normalize_name(attr->>'value')
                  AND n.status = 'active'
                  AND a.status = 'active'
            LOOP
                -- Only add if not already matched
                IF NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(matches) AS m(match)
                    WHERE m.match->>'entity_id' = match_record.node_id
                ) THEN
                    matches := matches || jsonb_build_object(
                        'entity_id', match_record.node_id,
                        'confidence', match_record.score,
                        'match_reason', match_record.reason,
                        'matched_name', match_record.primary_name
                    );
                END IF;
            END LOOP;
        END IF;
    END LOOP;
    
    -- Strategy 3: Fuzzy name matching
    IF jsonb_array_length(matches) = 0 THEN
        FOR match_record IN
            SELECT n.node_id, n.primary_name,
                   CASE 
                       WHEN similarity(n.normalized_name, normalize_name(p_primary_name)) > 0.8 THEN 0.8
                       WHEN similarity(n.normalized_name, normalize_name(p_primary_name)) > 0.6 THEN 0.6
                       ELSE 0.4
                   END as score,
                   'fuzzy_match' as reason
            FROM nodes n
            WHERE n.node_type = p_node_type
              AND n.status = 'active'
              AND similarity(n.normalized_name, normalize_name(p_primary_name)) > 0.5
            ORDER BY score DESC
            LIMIT 3
        LOOP
            matches := matches || jsonb_build_object(
                'entity_id', match_record.node_id,
                'confidence', match_record.score,
                'match_reason', match_record.reason,
                'matched_name', match_record.primary_name
            );
        END LOOP;
    END IF;
    
    RETURN matches;
END;
$$ LANGUAGE plpgsql;

-- Function to resolve a single entity
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
    -- Find similar entities
    similar_entities := find_similar_entities(p_node_type, p_primary_name, p_attributes, p_confidence_threshold);
    
    -- Evaluate the best match
    IF jsonb_array_length(similar_entities) > 0 THEN
        SELECT jsonb_array_elements(similar_entities) INTO best_match
        ORDER BY (jsonb_array_elements(similar_entities)->>'confidence')::DECIMAL DESC
        LIMIT 1;
        
        best_confidence := (best_match->>'confidence')::DECIMAL;
    END IF;
    
    -- Decision logic
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
        -- No good match - create new entity
        entity_id := generate_ulid();
        
        INSERT INTO nodes (node_id, node_type, primary_name, created_by, status)
        VALUES (entity_id, p_node_type, p_primary_name, 'propose_api', 'active');
        
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

-- Function to find existing relationships between two entities
CREATE OR REPLACE FUNCTION find_existing_relationships(
    p_source_id VARCHAR(26),
    p_target_id VARCHAR(26),
    p_relationship_type VARCHAR(50)
) RETURNS JSONB AS $$
DECLARE
    relationships JSONB := '[]'::JSONB;
    rel_record RECORD;
BEGIN
    -- Find exact relationships
    FOR rel_record IN
        SELECT r.relationship_id, r.relationship_type, r.strength, r.status, 
               r.valid_from, r.valid_to, r.metadata
        FROM relationships r
        WHERE ((r.source_node_id = p_source_id AND r.target_node_id = p_target_id) OR
               (r.source_node_id = p_target_id AND r.target_node_id = p_source_id))
          AND r.relationship_type = p_relationship_type
          AND r.status = 'active'
    LOOP
        relationships := relationships || jsonb_build_object(
            'relationship_id', rel_record.relationship_id,
            'relationship_type', rel_record.relationship_type,
            'strength', rel_record.strength,
            'valid_from', rel_record.valid_from,
            'valid_to', rel_record.valid_to,
            'metadata', rel_record.metadata
        );
    END LOOP;
    
    -- Find conflicting relationships
    FOR rel_record IN
        SELECT r.relationship_id, r.relationship_type, r.strength, r.status
        FROM relationships r
        WHERE ((r.source_node_id = p_source_id AND r.target_node_id = p_target_id) OR
               (r.source_node_id = p_target_id AND r.target_node_id = p_source_id))
          AND r.relationship_type != p_relationship_type
          AND r.status = 'active'
          AND (
              (p_relationship_type = 'Legal_Counsel' AND r.relationship_type = 'Opposing_Counsel') OR
              (p_relationship_type = 'Opposing_Counsel' AND r.relationship_type = 'Legal_Counsel')
          )
    LOOP
        relationships := relationships || jsonb_build_object(
            'relationship_id', rel_record.relationship_id,
            'relationship_type', rel_record.relationship_type,
            'strength', rel_record.strength,
            'conflict_type', 'opposing_relationship'
        );
    END LOOP;
    
    RETURN relationships;
END;
$$ LANGUAGE plpgsql;

-- Function to evaluate a proposed relationship
CREATE OR REPLACE FUNCTION evaluate_relationship(
    p_source_id VARCHAR(26),
    p_target_id VARCHAR(26),
    p_relationship_type VARCHAR(50),
    p_strength DECIMAL(3,2),
    p_valid_from DATE DEFAULT NULL,
    p_valid_to DATE DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS relationship_evaluation_result AS $$
DECLARE
    existing_rels JSONB;
    exact_match JSONB;
    conflicts JSONB := '[]'::JSONB;
    relationship_id VARCHAR(26);
    result relationship_evaluation_result;
    rel JSONB;
BEGIN
    -- Find existing relationships
    existing_rels := find_existing_relationships(p_source_id, p_target_id, p_relationship_type);
    
    -- Check for exact match and conflicts
    FOR rel IN SELECT * FROM jsonb_array_elements(existing_rels)
    LOOP
        IF rel->>'relationship_type' = p_relationship_type THEN
            exact_match := rel;
        ELSIF rel->>'conflict_type' = 'opposing_relationship' THEN
            conflicts := conflicts || rel;
        END IF;
    END LOOP;
    
    -- Decision logic
    IF exact_match IS NOT NULL THEN
        -- Relationship already exists
        relationship_id := exact_match->>'relationship_id';
        
        IF (exact_match->>'strength')::DECIMAL < p_strength THEN
            -- Update with higher strength
            UPDATE relationships 
            SET strength = p_strength, updated_at = CURRENT_TIMESTAMP
            WHERE relationship_id = (exact_match->>'relationship_id')::VARCHAR;
            
            result.action := 'updated';
            result.strength_delta := p_strength - (exact_match->>'strength')::DECIMAL;
        ELSE
            -- Keep existing - just add provenance
            result.action := 'duplicate';
            result.strength_delta := 0.0;
        END IF;
        
        result.relationship_id := relationship_id;
        result.confidence := GREATEST(p_strength, (exact_match->>'strength')::DECIMAL);
    ELSIF jsonb_array_length(conflicts) > 0 THEN
        -- Conflicting relationship exists
        relationship_id := generate_ulid();
        
        INSERT INTO relationships (
            relationship_id, source_node_id, target_node_id, relationship_type,
            strength, valid_from, valid_to, metadata, created_by, status
        ) VALUES (
            relationship_id, p_source_id, p_target_id, p_relationship_type,
            p_strength * 0.7, -- Reduce confidence due to conflict
            COALESCE(p_valid_from, CURRENT_DATE), p_valid_to, p_metadata, 
            'propose_api', 'active'
        );
        
        result.action := 'conflicted';
        result.relationship_id := relationship_id;
        result.confidence := p_strength * 0.7;
        result.conflicts := conflicts;
        result.strength_delta := 0.0;
    ELSE
        -- New relationship - create it
        relationship_id := generate_ulid();
        
        INSERT INTO relationships (
            relationship_id, source_node_id, target_node_id, relationship_type,
            strength, valid_from, valid_to, metadata, created_by, status
        ) VALUES (
            relationship_id, p_source_id, p_target_id, p_relationship_type,
            p_strength, COALESCE(p_valid_from, CURRENT_DATE), p_valid_to, 
            p_metadata, 'propose_api', 'active'
        );
        
        result.action := 'created';
        result.relationship_id := relationship_id;
        result.confidence := p_strength;
        result.conflicts := '[]'::JSONB;
        result.strength_delta := 0.0;
    END IF;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Main Propose API Function
CREATE OR REPLACE FUNCTION propose_fact(
    -- Required parameters
    p_source_node_type VARCHAR(50),
    p_source_node_name VARCHAR(255),
    p_target_node_type VARCHAR(50),
    p_target_node_name VARCHAR(255), 
    p_relationship_type VARCHAR(50),
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50),
    
    -- Optional parameters with defaults
    p_source_attributes JSONB DEFAULT '[]'::JSONB,
    p_target_attributes JSONB DEFAULT '[]'::JSONB,
    p_relationship_strength DECIMAL(3,2) DEFAULT 1.0,
    p_relationship_valid_from DATE DEFAULT NULL,
    p_relationship_valid_to DATE DEFAULT NULL,
    p_relationship_metadata JSONB DEFAULT NULL,
    p_provenance_confidence DECIMAL(3,2) DEFAULT 0.9,
    p_provenance_metadata JSONB DEFAULT NULL
) RETURNS propose_api_response AS $$
DECLARE
    source_resolution entity_resolution_result;
    target_resolution entity_resolution_result;
    rel_evaluation relationship_evaluation_result;
    actions JSONB := '[]'::JSONB;
    conflicts JSONB := '[]'::JSONB;
    provenance_ids JSONB := '[]'::JSONB;
    overall_confidence DECIMAL(3,2);
    response propose_api_response;
    prov_id VARCHAR(26);
BEGIN
    -- Start transaction
    BEGIN
        -- Resolve source entity
        source_resolution := resolve_entity(
            p_source_node_type, p_source_node_name, p_source_attributes, 0.8
        );
        
        -- Resolve target entity  
        target_resolution := resolve_entity(
            p_target_node_type, p_target_node_name, p_target_attributes, 0.8
        );
        
        -- Handle ambiguous entities
        IF source_resolution.action = 'ambiguous' OR target_resolution.action = 'ambiguous' THEN
            response.status := 'conflicts';
            response.overall_confidence := 0.0;
            response.conflicts := jsonb_build_array(
                jsonb_build_object(
                    'type', 'ambiguous_entities',
                    'source_alternatives', source_resolution.alternatives,
                    'target_alternatives', target_resolution.alternatives
                )
            );
            RETURN response;
        END IF;
        
        -- Record entity resolution actions
        actions := actions || jsonb_build_object(
            'action', 'entity_' || source_resolution.action,
            'entity_type', 'source',
            'entity_id', source_resolution.entity_id,
            'confidence', source_resolution.confidence,
            'match_reason', source_resolution.match_reason
        );
        
        actions := actions || jsonb_build_object(
            'action', 'entity_' || target_resolution.action,
            'entity_type', 'target', 
            'entity_id', target_resolution.entity_id,
            'confidence', target_resolution.confidence,
            'match_reason', target_resolution.match_reason
        );
        
        -- Evaluate relationship
        rel_evaluation := evaluate_relationship(
            source_resolution.entity_id, target_resolution.entity_id,
            p_relationship_type, p_relationship_strength,
            p_relationship_valid_from, p_relationship_valid_to, p_relationship_metadata
        );
        
        -- Record relationship action
        actions := actions || jsonb_build_object(
            'action', 'relationship_' || rel_evaluation.action,
            'relationship_id', rel_evaluation.relationship_id,
            'confidence', rel_evaluation.confidence,
            'strength_delta', rel_evaluation.strength_delta
        );
        
        -- Add conflicts if any
        IF rel_evaluation.conflicts IS NOT NULL AND jsonb_array_length(rel_evaluation.conflicts) > 0 THEN
            conflicts := conflicts || jsonb_build_object(
                'type', 'relationship_conflicts',
                'conflicts', rel_evaluation.conflicts
            );
        END IF;
        
        -- Create provenance records
        -- For source entity
        prov_id := create_provenance_record(
            'node', source_resolution.entity_id, p_source_name, p_source_type, 
            'propose_api', p_provenance_confidence
        );
        provenance_ids := provenance_ids || to_jsonb(prov_id);
        
        -- For target entity
        prov_id := create_provenance_record(
            'node', target_resolution.entity_id, p_source_name, p_source_type,
            'propose_api', p_provenance_confidence
        );
        provenance_ids := provenance_ids || to_jsonb(prov_id);
        
        -- For relationship
        prov_id := create_provenance_record(
            'relationship', rel_evaluation.relationship_id, p_source_name, p_source_type,
            'propose_api', p_provenance_confidence
        );
        provenance_ids := provenance_ids || to_jsonb(prov_id);
        
        -- Calculate overall confidence
        overall_confidence := (
            source_resolution.confidence + 
            target_resolution.confidence + 
            rel_evaluation.confidence + 
            p_provenance_confidence
        ) / 4.0;
        
        -- Build response
        response.status := CASE 
            WHEN jsonb_array_length(conflicts) > 0 THEN 'conflicts'
            ELSE 'success'
        END;
        response.overall_confidence := overall_confidence;
        response.actions := actions;
        response.conflicts := conflicts;
        response.provenance_ids := provenance_ids;
        
        RETURN response;
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Handle errors
            response.status := 'error';
            response.overall_confidence := 0.0;
            response.actions := jsonb_build_array(
                jsonb_build_object(
                    'action', 'error',
                    'message', SQLERRM
                )
            );
            response.conflicts := '[]'::JSONB;
            response.provenance_ids := '[]'::JSONB;
            RETURN response;
    END;
END;
$$ LANGUAGE plpgsql;