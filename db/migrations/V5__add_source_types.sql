-- =============================================
-- Add Source Types Optimization Table
-- Version: 5.0
-- Purpose: Normalize source types to reduce storage and improve consistency
-- =============================================

-- Create source_types table for normalization
CREATE TABLE source_types (
    source_type_id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    default_reliability DECIMAL(3,2) DEFAULT 0.8,
    is_official_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add some initial source types based on what we've been using
INSERT INTO source_types (source_type, description, default_reliability, is_official_source) VALUES
('manual_test', 'Manual testing and debugging', 0.5, FALSE),
('iowa_gov_database', 'Official Iowa government business entity database', 0.95, TRUE),
('client_intake', 'Law firm client intake forms', 0.8, FALSE),
('bar_association', 'State bar association records', 0.95, TRUE),
('court_records', 'Official court filings and records', 0.98, TRUE),
('contracts', 'Signed legal contracts and agreements', 0.90, FALSE),
('linkedin', 'LinkedIn professional profiles', 0.7, FALSE),
('business_cards', 'Business card collections', 0.6, FALSE),
('hr_system', 'HR database and employee records', 0.85, FALSE),
('crm_system', 'Customer relationship management system', 0.8, FALSE),
('sec_filings', 'SEC corporate filings', 0.98, TRUE),
('legal_records', 'General legal documentation', 0.85, FALSE),
('public_records', 'Government public records', 0.90, TRUE),
('conflict_check', 'Conflict checking system queries', 0.5, FALSE);

-- Add source_type_id column to provenance table (nullable for migration)
ALTER TABLE provenance ADD COLUMN source_type_id INTEGER REFERENCES source_types(source_type_id);

-- Create index for better performance
CREATE INDEX idx_provenance_source_type_id ON provenance(source_type_id);

-- Update existing provenance records with source_type_id
UPDATE provenance 
SET source_type_id = st.source_type_id
FROM source_types st
WHERE provenance.source_type = st.source_type;

-- Check for any unmatched source types
DO $$
DECLARE
    unmatched_count INTEGER;
    unmatched_types TEXT[];
BEGIN
    -- Count unmatched source types
    SELECT COUNT(*), array_agg(DISTINCT source_type)
    INTO unmatched_count, unmatched_types
    FROM provenance 
    WHERE source_type_id IS NULL;
    
    IF unmatched_count > 0 THEN
        RAISE NOTICE 'Found % provenance records with unmatched source types: %', 
            unmatched_count, array_to_string(unmatched_types, ', ');
        
        -- Insert any missing source types with default values
        INSERT INTO source_types (source_type, description, default_reliability)
        SELECT DISTINCT source_type, 
               'Auto-added during migration: ' || source_type,
               0.7
        FROM provenance 
        WHERE source_type_id IS NULL
          AND source_type IS NOT NULL;
        
        -- Update the newly created source types
        UPDATE provenance 
        SET source_type_id = st.source_type_id
        FROM source_types st
        WHERE provenance.source_type = st.source_type
          AND provenance.source_type_id IS NULL;
    ELSE
        RAISE NOTICE 'All provenance records successfully matched to source types';
    END IF;
END
$$;

-- Add constraint to ensure source_type_id is not null for new records
-- (We'll keep the old source_type column for backwards compatibility during transition)
-- ALTER TABLE provenance ALTER COLUMN source_type_id SET NOT NULL;

-- Create updated views for easier querying
CREATE OR REPLACE VIEW provenance_with_source_details AS
SELECT 
    p.*,
    st.description as source_description,
    st.default_reliability,
    st.is_official_source
FROM provenance p
LEFT JOIN source_types st ON p.source_type_id = st.source_type_id;

-- Update the create_provenance_record function to use source_type_id
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
    
    -- Get or create source_type_id
    SELECT st.source_type_id INTO source_type_id_val
    FROM source_types st
    WHERE st.source_type = p_source_type;
    
    -- If source type doesn't exist, create it
    IF source_type_id_val IS NULL THEN
        INSERT INTO source_types (source_type, description, default_reliability)
        VALUES (p_source_type, 'Auto-created: ' || p_source_type, 0.7)
        RETURNING source_type_id INTO source_type_id_val;
    END IF;
    
    -- Insert provenance record
    INSERT INTO provenance (
        provenance_id, asset_type, asset_id, source_name, source_type, source_type_id,
        confidence_score, reliability_rating, created_by, metadata, status
    ) VALUES (
        provenance_id, p_asset_type, p_asset_id, p_source_name, p_source_type, source_type_id_val,
        p_confidence_score, p_reliability_rating, p_created_by, p_metadata, 'active'
    );
    
    RETURN provenance_id;
END;
$$ LANGUAGE plpgsql;

-- Show summary of source types after migration
\echo '=== Source Types Summary ==='
SELECT 
    st.source_type,
    st.description,
    st.default_reliability,
    st.is_official_source,
    COUNT(p.provenance_id) as usage_count
FROM source_types st
LEFT JOIN provenance p ON st.source_type_id = p.source_type_id
GROUP BY st.source_type_id, st.source_type, st.description, st.default_reliability, st.is_official_source
ORDER BY usage_count DESC, st.source_type;