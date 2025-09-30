-- =============================================
-- Conflict Checking Test Scenarios
-- Purpose: Test various conflict detection patterns
-- =============================================

-- =============================================
-- Test 1: Direct Conflict Detection
-- Should find: Law firm represents both ACME and TechCorp
-- =============================================

-- Test query: Check if we can represent a new client that conflicts with existing clients
WITH potential_client AS (
    SELECT '55555555-5555-5555-5555-555555555551'::UUID as client_id, 'TechCorp Industries' as client_name
),
existing_representations AS (
    SELECT DISTINCT
        r.target_node_id as client_id,
        n.primary_name as client_name,
        r.source_node_id as attorney_id,
        a.primary_name as attorney_name
    FROM relationships r
    JOIN nodes n ON r.target_node_id = n.node_id
    JOIN nodes a ON r.source_node_id = a.node_id
    WHERE r.relationship_type = 'Legal_Counsel'
    AND r.status = 'active'
)
SELECT 
    'CONFLICT DETECTED: Direct representation conflict' as conflict_type,
    pc.client_name as new_client,
    er.client_name as existing_client,
    er.attorney_name as conflicted_attorney,
    1 as degrees_of_separation
FROM potential_client pc
JOIN existing_representations er ON er.attorney_id IN (
    SELECT r2.source_node_id 
    FROM relationships r2 
    WHERE r2.target_node_id = pc.client_id 
    AND r2.relationship_type = 'Legal_Counsel'
    AND r2.status = 'active'
);

-- =============================================
-- Test 2: 2-Degree Relationship Conflict
-- Should find: Amanda Brown (family) → Robert Brown → ACME Corp
-- =============================================

-- Function to find relationship paths up to 3 degrees
CREATE OR REPLACE FUNCTION find_conflict_paths(
    entity_name VARCHAR(255),
    max_degrees INTEGER DEFAULT 3
)
RETURNS TABLE(
    path_description TEXT,
    degrees INTEGER,
    entity_chain TEXT[],
    relationship_chain TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE relationship_path AS (
        -- Base case: direct relationships
        SELECT 
            n1.node_id as start_id,
            n1.primary_name as start_name,
            r.target_node_id as current_id,
            n2.primary_name as current_name,
            r.relationship_type,
            1 as degree,
            ARRAY[n1.primary_name, n2.primary_name] as entity_path,
            ARRAY[r.relationship_type] as relationship_path
        FROM nodes n1
        JOIN relationships r ON n1.node_id = r.source_node_id
        JOIN nodes n2 ON r.target_node_id = n2.node_id
        WHERE n1.primary_name = entity_name
        AND r.status = 'active'
        AND n1.status = 'active'
        AND n2.status = 'active'
        
        UNION ALL
        
        -- Recursive case: extend paths
        SELECT 
            rp.start_id,
            rp.start_name,
            r.target_node_id as current_id,
            n.primary_name as current_name,
            r.relationship_type,
            rp.degree + 1,
            rp.entity_path || n.primary_name,
            rp.relationship_path || r.relationship_type
        FROM relationship_path rp
        JOIN relationships r ON rp.current_id = r.source_node_id
        JOIN nodes n ON r.target_node_id = n.node_id
        WHERE rp.degree < max_degrees
        AND r.status = 'active'
        AND n.status = 'active'
        AND NOT (n.primary_name = ANY(rp.entity_path)) -- Avoid cycles
    )
    SELECT 
        array_to_string(rp.entity_path, ' → ') || ' (' || array_to_string(rp.relationship_path, ' → ') || ')',
        rp.degree,
        rp.entity_path,
        rp.relationship_path
    FROM relationship_path rp
    WHERE rp.degree <= max_degrees
    ORDER BY rp.degree, rp.entity_path;
END;
$$ LANGUAGE plpgsql;

-- Test the path finding function
SELECT 'Testing 2-3 degree relationship paths:' as test_description;

SELECT * FROM find_conflict_paths('Amanda Brown', 3);

-- =============================================
-- Test 3: Alias-Based Conflict Detection
-- Should find conflicts even when names don't match exactly
-- =============================================

-- Function to find all possible names (including aliases) for an entity
CREATE OR REPLACE FUNCTION get_all_entity_names(entity_name VARCHAR(255))
RETURNS TABLE(name_variant VARCHAR(255)) AS $$
BEGIN
    RETURN QUERY
    -- Get primary name and all aliases
    SELECT DISTINCT n.primary_name as name_variant
    FROM nodes n
    WHERE normalize_name(n.primary_name) = normalize_name(entity_name)
    AND n.status = 'active'
    
    UNION
    
    SELECT DISTINCT a.attribute_value as name_variant
    FROM nodes n
    JOIN attributes a ON n.node_id = a.node_id
    WHERE normalize_name(n.primary_name) = normalize_name(entity_name)
    AND a.attribute_type = 'nameAlias'
    AND n.status = 'active'
    AND a.status = 'active'
    
    UNION
    
    -- Also check if the input might be an alias
    SELECT DISTINCT n.primary_name as name_variant
    FROM nodes n
    JOIN attributes a ON n.node_id = a.node_id
    WHERE normalize_name(a.attribute_value) = normalize_name(entity_name)
    AND a.attribute_type = 'nameAlias'
    AND n.status = 'active'
    AND a.status = 'active';
END;
$$ LANGUAGE plpgsql;

-- Test alias resolution
SELECT 'Testing alias resolution for "J. Smith":' as test_description;
SELECT * FROM get_all_entity_names('J. Smith');

SELECT 'Testing alias resolution for "ACME Corp":' as test_description;
SELECT * FROM get_all_entity_names('ACME Corp');

-- =============================================
-- Test 4: Comprehensive Conflict Check Function
-- This simulates what the API would call for a new matter
-- =============================================

CREATE OR REPLACE FUNCTION comprehensive_conflict_check(
    entity_names VARCHAR(255)[],
    matter_description VARCHAR(255) DEFAULT NULL
)
RETURNS TABLE(
    conflict_found BOOLEAN,
    conflict_type VARCHAR(100),
    conflict_description TEXT,
    conflicted_entity VARCHAR(255),
    existing_entity VARCHAR(255),
    relationship_path TEXT,
    conflict_strength DECIMAL(3,2),
    degrees_of_separation INTEGER
) AS $$
DECLARE
    entity_name VARCHAR(255);
    resolved_names VARCHAR(255)[];
    check_id UUID;
BEGIN
    -- Generate check ID for audit trail
    check_id := uuid_generate_v4();
    
    -- Log the conflict check
    INSERT INTO conflict_checks (check_id, checked_entities, check_parameters, checked_at)
    VALUES (check_id, array_to_json(entity_names)::jsonb, 
            jsonb_build_object('matter_description', matter_description, 'max_degrees', 3), 
            CURRENT_TIMESTAMP);
    
    -- For each entity name, resolve all aliases and check for conflicts
    FOREACH entity_name IN ARRAY entity_names LOOP
        -- Get all name variants for this entity
        SELECT array_agg(name_variant) INTO resolved_names
        FROM get_all_entity_names(entity_name);
        
        -- Check pre-computed conflict matrix first (fastest)
        RETURN QUERY
        SELECT 
            TRUE as conflict_found,
            cm.conflict_type,
            'Pre-computed conflict: ' || array_to_string(cm.conflict_path::text[], ' → ') as conflict_description,
            entity_name as conflicted_entity,
            COALESCE(n1.primary_name, n2.primary_name) as existing_entity,
            array_to_string(cm.conflict_path::text[], ' → ') as relationship_path,
            cm.conflict_strength,
            cm.degrees_of_separation
        FROM conflict_matrix cm
        LEFT JOIN nodes n1 ON cm.entity_a_id = n1.node_id
        LEFT JOIN nodes n2 ON cm.entity_b_id = n2.node_id
        WHERE (n1.primary_name = ANY(resolved_names) OR n2.primary_name = ANY(resolved_names))
        AND (cm.expires_at IS NULL OR cm.expires_at > CURRENT_TIMESTAMP);
        
        -- Dynamic conflict detection for relationships not in pre-computed matrix
        -- Check for direct representation conflicts (1 degree)
        RETURN QUERY
        WITH entity_nodes AS (
            SELECT n.node_id, n.primary_name
            FROM nodes n
            WHERE n.primary_name = ANY(resolved_names)
            AND n.status = 'active'
        ),
        our_attorneys AS (
            SELECT DISTINCT r.source_node_id as attorney_id, a.primary_name as attorney_name
            FROM entity_nodes en
            JOIN relationships r ON en.node_id = r.target_node_id
            JOIN nodes a ON r.source_node_id = a.node_id
            WHERE r.relationship_type = 'Legal_Counsel'
            AND r.status = 'active'
        ),
        conflicting_representations AS (
            SELECT DISTINCT 
                oa.attorney_name,
                r.target_node_id as other_client_id,
                n.primary_name as other_client_name
            FROM our_attorneys oa
            JOIN relationships r ON oa.attorney_id = r.source_node_id
            JOIN nodes n ON r.target_node_id = n.node_id
            WHERE r.relationship_type = 'Legal_Counsel'
            AND r.status = 'active'
            AND NOT (n.primary_name = ANY(resolved_names)) -- Not the same entity
        )
        SELECT 
            TRUE as conflict_found,
            'Direct_Representation_Conflict'::VARCHAR(100) as conflict_type,
            'Attorney ' || cr.attorney_name || ' already represents ' || cr.other_client_name as conflict_description,
            entity_name as conflicted_entity,
            cr.other_client_name as existing_entity,
            entity_name || ' ← Legal_Counsel → ' || cr.attorney_name || ' ← Legal_Counsel → ' || cr.other_client_name as relationship_path,
            1.0::DECIMAL(3,2) as conflict_strength,
            1 as degrees_of_separation
        FROM conflicting_representations cr;
        
    END LOOP;
    
    -- If no conflicts found, return a single record indicating this
    IF NOT FOUND THEN
        RETURN QUERY SELECT 
            FALSE as conflict_found,
            'No_Conflict'::VARCHAR(100) as conflict_type,
            'No conflicts detected for provided entities' as conflict_description,
            ''::VARCHAR(255) as conflicted_entity,
            ''::VARCHAR(255) as existing_entity,
            ''::TEXT as relationship_path,
            0.0::DECIMAL(3,2) as conflict_strength,
            0 as degrees_of_separation;
    END IF;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- Test Cases
-- =============================================

-- Test Case 1: Should find conflict (TechCorp vs ACME via same attorney)
SELECT '=== TEST CASE 1: Direct Representation Conflict ===' as test_case;
SELECT * FROM comprehensive_conflict_check(ARRAY['TechCorp Industries'], 'New IP litigation matter');

-- Test Case 2: Should find family conflict (Amanda Brown family connection)
SELECT '=== TEST CASE 2: Family Connection Conflict ===' as test_case;
SELECT * FROM comprehensive_conflict_check(ARRAY['Amanda Brown'], 'Personal injury case against TechCorp');

-- Test Case 3: Should find no conflict (new entity)
SELECT '=== TEST CASE 3: No Conflict Expected ===' as test_case;
SELECT * FROM comprehensive_conflict_check(ARRAY['XYZ New Company'], 'Corporate formation matter');

-- Test Case 4: Should find conflict using alias
SELECT '=== TEST CASE 4: Alias-Based Conflict Detection ===' as test_case;
SELECT * FROM comprehensive_conflict_check(ARRAY['ACME Corp'], 'New employment law matter');

-- Test Case 5: Multiple entities, some conflicted
SELECT '=== TEST CASE 5: Multiple Entity Conflict Check ===' as test_case;
SELECT * FROM comprehensive_conflict_check(
    ARRAY['Jennifer White', 'New Client Corp', 'Mike Taylor'], 
    'Complex business dispute'
);

-- =============================================
-- Performance Test Queries
-- =============================================

-- Test query performance with EXPLAIN ANALYZE
SELECT '=== PERFORMANCE ANALYSIS ===' as section;

-- Test 1: Index usage on name lookups
EXPLAIN ANALYZE
SELECT n.*, array_agg(a.attribute_value) as aliases
FROM nodes n
LEFT JOIN attributes a ON n.node_id = a.node_id AND a.attribute_type = 'nameAlias'
WHERE normalize_name(n.primary_name) = normalize_name('ACME Corporation')
GROUP BY n.node_id, n.node_type, n.primary_name, n.normalized_name, n.created_at;

-- Test 2: Relationship traversal performance
EXPLAIN ANALYZE  
WITH RECURSIVE path_finder AS (
    SELECT 
        source_node_id, target_node_id, relationship_type, 1 as depth,
        ARRAY[source_node_id] as path
    FROM relationships 
    WHERE source_node_id = '33333333-3333-3333-3333-333333333331'::UUID
    AND status = 'active'
    
    UNION ALL
    
    SELECT 
        r.source_node_id, r.target_node_id, r.relationship_type, pf.depth + 1,
        pf.path || r.source_node_id
    FROM relationships r
    JOIN path_finder pf ON r.source_node_id = pf.target_node_id
    WHERE pf.depth < 3
    AND r.status = 'active'
    AND NOT (r.source_node_id = ANY(pf.path))
)
SELECT * FROM path_finder;

-- =============================================
-- Cleanup Functions
-- =============================================

-- Function to clean up old audit records
CREATE OR REPLACE FUNCTION cleanup_old_conflict_checks(days_to_keep INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM conflict_checks 
    WHERE checked_at < CURRENT_DATE - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;