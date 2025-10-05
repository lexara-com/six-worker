-- =============================================
-- V27: Fix Provenance Deduplication in create_provenance_record
-- =============================================
-- Problem: create_provenance_record creates duplicate provenance records
-- when the same source documents the same fact multiple times, violating
-- the idx_prov_unique_asset_source constraint.
--
-- Solution: Check for existing active provenance before inserting.
-- If a provenance record already exists for (asset_id, source_name, source_type),
-- return the existing ID instead of creating a duplicate.
-- =============================================

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
    -- Check if active provenance already exists for this (asset, source) combination
    -- This prevents duplicate provenance records from the same source
    SELECT p.provenance_id INTO provenance_id
    FROM provenance p
    WHERE p.asset_id = p_asset_id
      AND p.source_name = p_source_name
      AND p.source_type = p_source_type
      AND p.status = 'active'
    LIMIT 1;

    IF FOUND THEN
        -- Provenance already exists, return existing ID
        RETURN provenance_id;
    END IF;

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

COMMENT ON FUNCTION create_provenance_record IS
    'Creates or returns existing provenance record. Idempotent - prevents duplicates from same source.';
