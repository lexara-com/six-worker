-- =============================================
-- Add Source Type Reference to Provenance
-- Version: 5.0 (Fixed for existing schema)
-- Purpose: Add foreign key relationship to existing source_types table
-- =============================================

-- First, add missing source types that we're actually using
INSERT INTO source_types (source_type, description, default_reliability, requires_license) 
VALUES 
('manual_test', 'Manual testing and debugging', 'low', FALSE),
('iowa_gov_database', 'Official Iowa government business entity database', 'high', FALSE),
('crm_system', 'Customer relationship management system', 'medium', FALSE),
('hr_system', 'HR database and employee records', 'medium', FALSE),
('conflict_check', 'Conflict checking system queries', 'low', FALSE)
ON CONFLICT (source_type) DO NOTHING;

-- Add index on source_type in provenance for better FK performance
CREATE INDEX IF NOT EXISTS idx_provenance_source_type ON provenance(source_type);

-- Add foreign key constraint from provenance.source_type to source_types.source_type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'fk_provenance_source_type' 
        AND table_name = 'provenance'
    ) THEN
        -- First, make sure all source types in provenance exist in source_types
        INSERT INTO source_types (source_type, description, default_reliability, requires_license)
        SELECT DISTINCT 
            p.source_type,
            'Auto-added: ' || p.source_type,
            'medium',
            FALSE
        FROM provenance p
        WHERE p.source_type IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM source_types st 
              WHERE st.source_type = p.source_type
          );
        
        -- Now add the foreign key constraint
        ALTER TABLE provenance ADD CONSTRAINT fk_provenance_source_type 
            FOREIGN KEY (source_type) REFERENCES source_types(source_type);
    END IF;
END
$$;

-- Create optimized view for querying provenance with source details
CREATE OR REPLACE VIEW provenance_with_source_details AS
SELECT 
    p.provenance_id,
    p.asset_type,
    p.asset_id,
    p.source_name,
    p.source_type,
    st.description as source_description,
    st.default_reliability,
    st.requires_license,
    p.confidence_score,
    p.reliability_rating,
    p.created_at,
    p.created_by,
    p.status,
    p.metadata
FROM provenance p
LEFT JOIN source_types st ON p.source_type = st.source_type;

-- Update create_provenance_record function to ensure source_type exists
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
BEGIN
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

-- Show optimization results
\echo '=== Source Type Optimization Results ==='

\echo '1. Source types with usage counts:'
SELECT 
    st.source_type,
    st.description,
    st.default_reliability,
    st.requires_license,
    COUNT(p.provenance_id) as usage_count,
    CASE 
        WHEN SUM(COUNT(p.provenance_id)) OVER() > 0 
        THEN ROUND(COUNT(p.provenance_id) * 100.0 / SUM(COUNT(p.provenance_id)) OVER(), 1) 
        ELSE 0 
    END as usage_percent
FROM source_types st
LEFT JOIN provenance p ON st.source_type = p.source_type
GROUP BY st.source_type, st.description, st.default_reliability, st.requires_license
ORDER BY usage_count DESC;

\echo '2. Referential integrity check:'
SELECT 
    COUNT(*) as total_provenance_records,
    COUNT(st.source_type) as records_with_valid_source_types,
    COUNT(*) - COUNT(st.source_type) as records_with_invalid_source_types
FROM provenance p
LEFT JOIN source_types st ON p.source_type = st.source_type;

\echo '3. Most active source types:'
SELECT 
    p.source_type,
    COUNT(*) as record_count,
    MIN(p.created_at)::date as first_used,
    MAX(p.created_at)::date as last_used
FROM provenance p
GROUP BY p.source_type
ORDER BY record_count DESC
LIMIT 10;