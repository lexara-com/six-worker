-- =============================================
-- Add Source Type ID Optimization to Provenance
-- Version: 5.0 (Corrected)
-- Purpose: Link provenance to existing source_types table for normalization
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

-- Add source_type_id column to provenance table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'provenance' AND column_name = 'source_type_id'
    ) THEN
        ALTER TABLE provenance ADD COLUMN source_type_id INTEGER;
    END IF;
END
$$;

-- Add foreign key constraint if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'fk_provenance_source_type'
    ) THEN
        ALTER TABLE provenance ADD CONSTRAINT fk_provenance_source_type 
            FOREIGN KEY (source_type_id) REFERENCES source_types(source_type_id);
    END IF;
END
$$;

-- Add index if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_provenance_source_type_id'
    ) THEN
        CREATE INDEX idx_provenance_source_type_id ON provenance(source_type_id);
    END IF;
END
$$;

-- Update existing provenance records with source_type_id
UPDATE provenance 
SET source_type_id = (
    SELECT source_type_id 
    FROM source_types 
    WHERE source_types.source_type = provenance.source_type
)
WHERE source_type_id IS NULL;

-- Check for any unmatched source types and report them
DO $$
DECLARE
    unmatched_count INTEGER;
    unmatched_types TEXT[];
    rec RECORD;
BEGIN
    -- Count unmatched source types
    SELECT COUNT(*), array_agg(DISTINCT source_type)
    INTO unmatched_count, unmatched_types
    FROM provenance 
    WHERE source_type_id IS NULL;
    
    IF unmatched_count > 0 THEN
        RAISE NOTICE 'Found % provenance records with unmatched source types: %', 
            unmatched_count, array_to_string(unmatched_types, ', ');
        
        -- Add missing source types with default values
        FOR rec IN 
            SELECT DISTINCT source_type 
            FROM provenance 
            WHERE source_type_id IS NULL AND source_type IS NOT NULL
        LOOP
            INSERT INTO source_types (source_type, description, default_reliability, requires_license)
            VALUES (rec.source_type, 'Auto-added: ' || rec.source_type, 'medium', FALSE);
            
            RAISE NOTICE 'Added missing source type: %', rec.source_type;
        END LOOP;
        
        -- Update the newly created source types
        UPDATE provenance 
        SET source_type_id = (
            SELECT source_type_id 
            FROM source_types 
            WHERE source_types.source_type = provenance.source_type
        )
        WHERE source_type_id IS NULL;
        
    ELSE
        RAISE NOTICE 'All provenance records successfully matched to source types';
    END IF;
END
$$;

-- Create helper view for easier querying with source type details
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
LEFT JOIN source_types st ON p.source_type_id = st.source_type_id;

-- Update create_provenance_record function to populate source_type_id
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
    source_type_id_val INTEGER;
BEGIN
    -- Generate ULID for provenance record
    provenance_id := generate_ulid();
    
    -- Get source_type_id (create if doesn't exist)
    SELECT source_type_id INTO source_type_id_val
    FROM source_types
    WHERE source_type = p_source_type;
    
    IF source_type_id_val IS NULL THEN
        INSERT INTO source_types (source_type, description, default_reliability, requires_license)
        VALUES (p_source_type, 'Auto-created: ' || p_source_type, 'medium', FALSE)
        RETURNING source_type_id INTO source_type_id_val;
    END IF;
    
    -- Insert provenance record with source_type_id
    INSERT INTO provenance (
        provenance_id, asset_type, asset_id, source_name, source_type, source_type_id,
        confidence_score, reliability_rating, data_obtained_at, created_by, status, metadata
    ) VALUES (
        provenance_id, p_asset_type, p_asset_id, p_source_name, p_source_type, source_type_id_val,
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
    ROUND(COUNT(p.provenance_id) * 100.0 / SUM(COUNT(p.provenance_id)) OVER(), 1) as usage_percent
FROM source_types st
LEFT JOIN provenance p ON st.source_type_id = p.source_type_id
GROUP BY st.source_type_id, st.source_type, st.description, st.default_reliability, st.requires_license
ORDER BY usage_count DESC;

\echo '2. Storage optimization estimate:'
SELECT 
    'Before optimization' as scenario,
    COUNT(*) as records,
    AVG(LENGTH(source_type)) as avg_source_type_length,
    COUNT(*) * AVG(LENGTH(source_type)) as total_storage_bytes
FROM provenance
UNION ALL
SELECT 
    'After optimization' as scenario,
    COUNT(*) as records,
    4 as avg_source_type_id_bytes,  -- INTEGER is 4 bytes
    COUNT(*) * 4 as total_storage_bytes
FROM provenance;

\echo '3. Provenance records without source_type_id (should be 0):'
SELECT COUNT(*) as unmatched_records
FROM provenance 
WHERE source_type_id IS NULL;