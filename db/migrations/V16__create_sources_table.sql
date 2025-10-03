-- =============================================
-- V16: Create Sources Table for Version Tracking
-- =============================================
-- This table tracks each specific import/download instance
-- allowing us to distinguish between quarterly updates of the same data source
-- =============================================

-- Create sources table to track each import session/file version
CREATE TABLE IF NOT EXISTS sources (
    source_id VARCHAR(26) PRIMARY KEY DEFAULT generate_ulid(),
    source_type VARCHAR(100) NOT NULL,  -- References source_types.source_type
    source_name VARCHAR(255) NOT NULL,   -- Human-readable name
    source_version VARCHAR(50),          -- Version identifier (e.g., "2024-Q1", "2024-Q2")
    
    -- File/Import metadata
    file_name VARCHAR(255),              -- Original filename
    file_hash VARCHAR(64),               -- SHA256 hash of the file
    file_size_bytes BIGINT,              -- File size for verification
    download_url TEXT,                   -- Where it was downloaded from
    download_date TIMESTAMP,             -- When it was downloaded
    
    -- Processing metadata
    import_started_at TIMESTAMP,         -- When import began
    import_completed_at TIMESTAMP,       -- When import finished
    records_in_file INTEGER,             -- Total records in source file
    records_processed INTEGER,           -- How many were processed
    records_imported INTEGER,            -- How many were successfully imported
    records_skipped INTEGER,             -- How many were skipped
    records_failed INTEGER,              -- How many failed
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed, partial
    error_message TEXT,                  -- If import failed, why?
    
    -- Metadata
    metadata JSONB,                      -- Additional flexible metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure each version is unique for a given source_type
    CONSTRAINT unique_source_version UNIQUE (source_type, source_version),
    
    -- Foreign key to source_types
    CONSTRAINT fk_source_type 
        FOREIGN KEY (source_type) 
        REFERENCES source_types(source_type)
);

-- Add index for common queries
CREATE INDEX idx_sources_type_date ON sources(source_type, download_date DESC);
CREATE INDEX idx_sources_status ON sources(status);

-- Update provenance table to reference sources
ALTER TABLE provenance 
    ADD COLUMN IF NOT EXISTS source_id VARCHAR(26);

-- Add foreign key after column exists
ALTER TABLE provenance
    ADD CONSTRAINT fk_provenance_source
    FOREIGN KEY (source_id) 
    REFERENCES sources(source_id);

-- Add index for provenance source lookups
CREATE INDEX IF NOT EXISTS idx_provenance_source ON provenance(source_id);

-- Example: Create a source record for the current Iowa import
INSERT INTO sources (
    source_type,
    source_name,
    source_version,
    file_name,
    download_url,
    download_date,
    import_started_at,
    records_in_file,
    status,
    created_by,
    metadata
) VALUES (
    'iowa_gov_database',
    'Active Iowa Business Entities',
    '2025-Q4',  -- Or '2025-10-01' for daily versioning
    'Active_Iowa_Business_Entities_20251001.csv',
    'https://data.iowa.gov/api/views/ez5t-3qay/rows.csv',
    '2025-10-01 04:17:00'::timestamp,
    CURRENT_TIMESTAMP,
    300034,  -- From our earlier count
    'processing',
    'import_script',
    jsonb_build_object(
        'dataset_id', 'ez5t-3qay',
        'dataset_title', 'Active Iowa Business Entities',
        'publisher', 'Iowa Secretary of State',
        'update_frequency', 'Quarterly'
    )
);

-- Function to get or create a source record
CREATE OR REPLACE FUNCTION get_or_create_source(
    p_source_type VARCHAR(100),
    p_source_name VARCHAR(255),
    p_source_version VARCHAR(50),
    p_file_name VARCHAR(255) DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS VARCHAR(26) AS $$
DECLARE
    v_source_id VARCHAR(26);
BEGIN
    -- Check if source already exists
    SELECT source_id INTO v_source_id
    FROM sources
    WHERE source_type = p_source_type
      AND source_version = p_source_version;
    
    -- Create if doesn't exist
    IF v_source_id IS NULL THEN
        INSERT INTO sources (
            source_type,
            source_name,
            source_version,
            file_name,
            import_started_at,
            status,
            metadata
        ) VALUES (
            p_source_type,
            p_source_name,
            p_source_version,
            p_file_name,
            CURRENT_TIMESTAMP,
            'processing',
            p_metadata
        )
        RETURNING source_id INTO v_source_id;
    END IF;
    
    RETURN v_source_id;
END;
$$ LANGUAGE plpgsql;

-- View to see source history
CREATE VIEW source_version_history AS
SELECT 
    s.source_type,
    st.description as source_description,
    s.source_name,
    s.source_version,
    s.download_date,
    s.import_completed_at,
    s.records_imported,
    s.status,
    COUNT(DISTINCT p.asset_id) as linked_assets
FROM sources s
JOIN source_types st ON s.source_type = st.source_type
LEFT JOIN provenance p ON s.source_id = p.source_id
GROUP BY 
    s.source_id, s.source_type, st.description, s.source_name, 
    s.source_version, s.download_date, s.import_completed_at,
    s.records_imported, s.status
ORDER BY s.source_type, s.download_date DESC;

-- Comments
COMMENT ON TABLE sources IS 'Tracks specific instances of data imports, allowing version control of quarterly/periodic updates';
COMMENT ON COLUMN sources.source_version IS 'Version identifier like 2024-Q1, 2024-Q2 for quarterly updates';
COMMENT ON COLUMN sources.file_hash IS 'SHA256 hash to verify file integrity and detect changes';