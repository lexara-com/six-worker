-- =============================================
-- V24: Partition Provenance Table by Month
-- =============================================
-- The provenance table is growing rapidly (300K+ rows)
-- and is primarily queried by date ranges.
-- Partitioning by month will improve query performance
-- and make archival easier.
-- =============================================

-- Step 1: Create new partitioned table structure
-- Note: We'll create it alongside the existing table first
CREATE TABLE IF NOT EXISTS provenance_partitioned (
    provenance_id VARCHAR(26),
    asset_type VARCHAR(50) NOT NULL,
    asset_id VARCHAR(26) NOT NULL,
    source_name VARCHAR(500),
    source_type VARCHAR(100),
    source_url VARCHAR(1000),
    source_license VARCHAR(200),
    confidence_score DECIMAL(3,2) DEFAULT 0.50,
    reliability_rating VARCHAR(50),
    data_obtained_at TIMESTAMP,
    data_valid_from DATE,
    data_valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    status VARCHAR(20) DEFAULT 'active',
    metadata JSONB,
    notes TEXT,
    source_type_id INTEGER,
    source_id VARCHAR(26),
    reviewed_at TIMESTAMP,
    reviewed_by VARCHAR(100),
    review_status VARCHAR(50),
    review_notes TEXT,

    -- Constraints
    PRIMARY KEY (provenance_id, created_at),
    CONSTRAINT valid_confidence CHECK (confidence_score >= 0 AND confidence_score <= 1),
    CONSTRAINT valid_asset_type CHECK (asset_type IN ('node', 'relationship', 'attribute'))
) PARTITION BY RANGE (created_at);

-- Step 2: Create partitions for recent and upcoming months
-- Past months
CREATE TABLE IF NOT EXISTS provenance_2025_09 PARTITION OF provenance_partitioned
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS provenance_2025_10 PARTITION OF provenance_partitioned
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

-- Future months (create ahead of time)
CREATE TABLE IF NOT EXISTS provenance_2025_11 PARTITION OF provenance_partitioned
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE IF NOT EXISTS provenance_2025_12 PARTITION OF provenance_partitioned
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS provenance_2026_01 PARTITION OF provenance_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- Default partition for any data outside defined ranges
CREATE TABLE IF NOT EXISTS provenance_default PARTITION OF provenance_partitioned DEFAULT;

-- Step 3: Create indexes on partitioned table
-- These will be inherited by all partitions
CREATE INDEX IF NOT EXISTS idx_prov_part_asset 
    ON provenance_partitioned (asset_type, asset_id);
CREATE INDEX IF NOT EXISTS idx_prov_part_source 
    ON provenance_partitioned (source_type, source_name);
CREATE INDEX IF NOT EXISTS idx_prov_part_confidence 
    ON provenance_partitioned (confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_prov_part_obtained 
    ON provenance_partitioned (data_obtained_at DESC);
CREATE INDEX IF NOT EXISTS idx_prov_part_status 
    ON provenance_partitioned (status, asset_type);
CREATE INDEX IF NOT EXISTS idx_prov_part_created 
    ON provenance_partitioned (created_at DESC);

-- Step 4: Copy data from existing table to partitioned table
-- This will automatically distribute data to appropriate partitions
INSERT INTO provenance_partitioned
SELECT * FROM provenance
ON CONFLICT (provenance_id, created_at) DO NOTHING;

-- Step 5: Verify data migration
DO $$
DECLARE
    original_count INTEGER;
    partitioned_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO original_count FROM provenance;
    SELECT COUNT(*) INTO partitioned_count FROM provenance_partitioned;
    
    RAISE NOTICE 'Original table rows: %', original_count;
    RAISE NOTICE 'Partitioned table rows: %', partitioned_count;
    
    IF original_count != partitioned_count THEN
        RAISE WARNING 'Row count mismatch! Original: %, Partitioned: %', 
                      original_count, partitioned_count;
    END IF;
END $$;

-- Step 6: Create a view to make transition easier
-- This allows existing code to work unchanged
CREATE OR REPLACE VIEW provenance_view AS 
SELECT * FROM provenance_partitioned;

-- Step 7: Report on partition sizes
SELECT 
    'Partition created' as status,
    child.relname as partition_name,
    pg_size_pretty(pg_relation_size(child.oid)) as size,
    (SELECT COUNT(*) FROM provenance_partitioned 
     WHERE created_at >= 
        CASE 
            WHEN child.relname LIKE '%_2025_09' THEN '2025-09-01'::timestamp
            WHEN child.relname LIKE '%_2025_10' THEN '2025-10-01'::timestamp
            WHEN child.relname LIKE '%_2025_11' THEN '2025-11-01'::timestamp
            WHEN child.relname LIKE '%_2025_12' THEN '2025-12-01'::timestamp
            WHEN child.relname LIKE '%_2026_01' THEN '2026-01-01'::timestamp
            ELSE '1900-01-01'::timestamp
        END
     AND created_at < 
        CASE
            WHEN child.relname LIKE '%_2025_09' THEN '2025-10-01'::timestamp
            WHEN child.relname LIKE '%_2025_10' THEN '2025-11-01'::timestamp
            WHEN child.relname LIKE '%_2025_11' THEN '2025-12-01'::timestamp
            WHEN child.relname LIKE '%_2025_12' THEN '2026-01-01'::timestamp
            WHEN child.relname LIKE '%_2026_01' THEN '2026-02-01'::timestamp
            ELSE '2100-01-01'::timestamp
        END
    ) as row_count
FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'provenance_partitioned'
ORDER BY child.relname;

-- Note: To complete the migration:
-- 1. Update application code to use provenance_partitioned instead of provenance
-- 2. Once verified, rename tables:
--    ALTER TABLE provenance RENAME TO provenance_old;
--    ALTER TABLE provenance_partitioned RENAME TO provenance;
-- 3. Drop the old table: DROP TABLE provenance_old;

-- Add helpful comments
COMMENT ON TABLE provenance_partitioned IS 'Partitioned provenance table - partitioned by month on created_at';
COMMENT ON TABLE provenance_2025_10 IS 'Provenance partition for October 2025';
COMMENT ON TABLE provenance_2025_11 IS 'Provenance partition for November 2025';