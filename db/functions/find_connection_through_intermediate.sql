-- =============================================
-- Function: find_connection_through_intermediate
-- Purpose: Find if two nodes are connected through exactly one intermediate node
-- Returns: Table of intermediate nodes and their relationships
-- =============================================

CREATE OR REPLACE FUNCTION find_connection_through_intermediate(
    p_node1_id VARCHAR(26),
    p_node2_id VARCHAR(26)
)
RETURNS TABLE (
    node1_id VARCHAR(26),
    node1_name VARCHAR(255),
    intermediate_node_id VARCHAR(26),
    intermediate_name VARCHAR(255),
    node2_id VARCHAR(26),
    node2_name VARCHAR(255),
    relationship1_type VARCHAR(50),
    relationship1_direction TEXT,
    relationship2_type VARCHAR(50),
    relationship2_direction TEXT,
    path_description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        p_node1_id as node1_id,
        n1.primary_name as node1_name,
        intermediate.node_id as intermediate_node_id,
        intermediate.primary_name as intermediate_name,
        p_node2_id as node2_id,
        n2.primary_name as node2_name,
        r1.relationship_type as relationship1_type,
        CASE 
            WHEN r1.source_node_id = p_node1_id THEN 'outgoing'
            ELSE 'incoming'
        END as relationship1_direction,
        r2.relationship_type as relationship2_type,
        CASE 
            WHEN r2.source_node_id = intermediate.node_id THEN 'outgoing'
            ELSE 'incoming'
        END as relationship2_direction,
        CONCAT(
            n1.primary_name, 
            CASE 
                WHEN r1.source_node_id = p_node1_id THEN ' --['
                ELSE ' <--['
            END,
            r1.relationship_type,
            CASE 
                WHEN r1.source_node_id = p_node1_id THEN ']--> '
                ELSE ']-- '
            END,
            intermediate.primary_name,
            CASE 
                WHEN r2.source_node_id = intermediate.node_id THEN ' --['
                ELSE ' <--['
            END,
            r2.relationship_type,
            CASE 
                WHEN r2.source_node_id = intermediate.node_id THEN ']--> '
                ELSE ']-- '
            END,
            n2.primary_name
        ) as path_description
    FROM nodes intermediate
    JOIN relationships r1 ON (
        (r1.source_node_id = p_node1_id AND r1.target_node_id = intermediate.node_id) OR
        (r1.target_node_id = p_node1_id AND r1.source_node_id = intermediate.node_id)
    )
    JOIN relationships r2 ON (
        (r2.source_node_id = intermediate.node_id AND r2.target_node_id = p_node2_id) OR
        (r2.target_node_id = intermediate.node_id AND r2.source_node_id = p_node2_id)
    )
    JOIN nodes n1 ON n1.node_id = p_node1_id
    JOIN nodes n2 ON n2.node_id = p_node2_id
    WHERE intermediate.node_id NOT IN (p_node1_id, p_node2_id)
      AND intermediate.status = 'active'
      AND r1.status = 'active'
      AND r2.status = 'active';
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION find_connection_through_intermediate TO graph_admin;

-- Add helpful comment
COMMENT ON FUNCTION find_connection_through_intermediate IS 
'Finds all paths between two nodes that go through exactly one intermediate node (2 degrees of separation).
Returns details about the intermediate node and both relationships in the path.
The path_description provides a visual representation of the connection.';

-- Example usage:
-- SELECT * FROM find_connection_through_intermediate('01K6GN09SN262NG7MZVTXVHPK4', '01K6GN0A81AR204MWXJ6P1VC6K');

-- =============================================
-- Simpler version that just returns intermediate node IDs
-- =============================================

CREATE OR REPLACE FUNCTION find_intermediate_nodes(
    p_node1_id VARCHAR(26),
    p_node2_id VARCHAR(26)
)
RETURNS TABLE (
    intermediate_node_id VARCHAR(26),
    intermediate_name VARCHAR(255),
    intermediate_type VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        intermediate.node_id as intermediate_node_id,
        intermediate.primary_name as intermediate_name,
        intermediate.node_type as intermediate_type
    FROM nodes intermediate
    JOIN relationships r1 ON (
        (r1.source_node_id = p_node1_id AND r1.target_node_id = intermediate.node_id) OR
        (r1.target_node_id = p_node1_id AND r1.source_node_id = intermediate.node_id)
    )
    JOIN relationships r2 ON (
        (r2.source_node_id = intermediate.node_id AND r2.target_node_id = p_node2_id) OR
        (r2.target_node_id = intermediate.node_id AND r2.source_node_id = p_node2_id)
    )
    WHERE intermediate.node_id NOT IN (p_node1_id, p_node2_id)
      AND intermediate.status = 'active';
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION find_intermediate_nodes TO graph_admin;

-- Add helpful comment
COMMENT ON FUNCTION find_intermediate_nodes IS 
'Simple function that returns just the intermediate nodes connecting two given nodes.
Use find_connection_through_intermediate for full path details.';

-- Example usage:
-- SELECT * FROM find_intermediate_nodes('01K6GN09SN262NG7MZVTXVHPK4', '01K6GN0A81AR204MWXJ6P1VC6K');