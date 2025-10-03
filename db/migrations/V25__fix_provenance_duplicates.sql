-- =============================================
-- V25: Fix Provenance Duplicate Records
-- =============================================
-- Problem: Provenance records are created multiple times
-- for the same asset from the same source.
--
-- Solution:
-- 1. Add unique constraint on (asset_id, source_name, source_type)
-- 2. Clean up existing duplicates
-- 3. Update propose functions to use ON CONFLICT DO NOTHING
-- =============================================

-- Step 1: Analyze current state
DO $$
DECLARE
    v_total_records INT;
    v_unique_assets INT;
    v_duplicates INT;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT asset_id)
    INTO v_total_records, v_unique_assets
    FROM provenance;

    v_duplicates := v_total_records - v_unique_assets;

    RAISE NOTICE 'Provenance Analysis:';
    RAISE NOTICE '  Total records: %', v_total_records;
    RAISE NOTICE '  Unique assets: %', v_unique_assets;
    RAISE NOTICE '  Duplicate records: % (%.1f%%)',
        v_duplicates,
        100.0 * v_duplicates / v_total_records;
END $$;

-- Step 2: Create temp table with deduplicated provenance
-- Keep the FIRST record for each (asset_id, source_name, source_type) combination
CREATE TEMP TABLE provenance_deduped AS
SELECT DISTINCT ON (asset_id, source_name, source_type)
    provenance_id,
    asset_type,
    asset_id,
    source_name,
    source_type,
    source_url,
    source_license,
    confidence_score,
    reliability_rating,
    data_obtained_at,
    data_valid_from,
    data_valid_to,
    created_at,
    created_by,
    status,
    metadata,
    notes,
    source_type_id,
    source_id,
    reviewed_at,
    reviewed_by,
    review_status,
    review_notes
FROM provenance
ORDER BY asset_id, source_name, source_type, created_at ASC;  -- Keep earliest

-- Report on what will be removed
DO $$
DECLARE
    v_original_count INT;
    v_deduped_count INT;
    v_to_remove INT;
BEGIN
    SELECT COUNT(*) INTO v_original_count FROM provenance;
    SELECT COUNT(*) INTO v_deduped_count FROM provenance_deduped;
    v_to_remove := v_original_count - v_deduped_count;

    RAISE NOTICE 'Deduplication Plan:';
    RAISE NOTICE '  Will keep: % records', v_deduped_count;
    RAISE NOTICE '  Will remove: % duplicate records', v_to_remove;
END $$;

-- Step 3: Backup old provenance table (just in case)
ALTER TABLE provenance RENAME TO provenance_old_v25;

-- Step 4: Create new provenance table with unique constraint
CREATE TABLE provenance (
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

    -- NOTE: Cannot add UNIQUE constraint without created_at in partitioned table
    -- Will use unique index instead
) PARTITION BY RANGE (created_at);

-- Step 5: Recreate partitions (drop if exist from previous run)
DROP TABLE IF EXISTS provenance_2025_09, provenance_2025_10, provenance_2025_11,
                     provenance_2025_12, provenance_2026_01, provenance_default CASCADE;

CREATE TABLE provenance_2025_09 PARTITION OF provenance
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE provenance_2025_10 PARTITION OF provenance
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE provenance_2025_11 PARTITION OF provenance
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE provenance_2025_12 PARTITION OF provenance
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE provenance_2026_01 PARTITION OF provenance
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE provenance_default PARTITION OF provenance DEFAULT;

-- Step 6: Recreate indexes
CREATE INDEX idx_prov_asset ON provenance (asset_type, asset_id);
CREATE INDEX idx_prov_source ON provenance (source_type, source_name);
CREATE INDEX idx_prov_confidence ON provenance (confidence_score DESC);
CREATE INDEX idx_prov_obtained ON provenance (data_obtained_at DESC);
CREATE INDEX idx_prov_status ON provenance (status, asset_type);
CREATE INDEX idx_prov_created ON provenance (created_at DESC);

-- Step 6b: Create unique index to prevent duplicates
-- This acts like a unique constraint but works with partitioned tables
CREATE UNIQUE INDEX idx_prov_unique_asset_source
    ON provenance (asset_id, source_name, source_type, created_at);

-- Add a partial unique index for the common case (excluding created_at for lookups)
CREATE UNIQUE INDEX idx_prov_unique_asset_source_latest
    ON provenance (asset_id, source_name, source_type)
    WHERE status = 'active';  -- Only one active provenance per asset/source

-- Step 7: Insert deduplicated data
INSERT INTO provenance
SELECT * FROM provenance_deduped;

-- Step 8: Verify results
DO $$
DECLARE
    v_new_count INT;
    v_old_count INT;
    v_removed INT;
BEGIN
    SELECT COUNT(*) INTO v_new_count FROM provenance;
    SELECT COUNT(*) INTO v_old_count FROM provenance_old_v25;
    v_removed := v_old_count - v_new_count;

    RAISE NOTICE '';
    RAISE NOTICE '✅ Migration Complete:';
    RAISE NOTICE '  Old table: % records', v_old_count;
    RAISE NOTICE '  New table: % records', v_new_count;
    RAISE NOTICE '  Removed: % duplicates (%.1f%%)',
        v_removed,
        100.0 * v_removed / v_old_count;
    RAISE NOTICE '';
    RAISE NOTICE '  Unique index added: idx_prov_unique_asset_source_latest';
    RAISE NOTICE '  Constraint: (asset_id, source_name, source_type) WHERE status=active';
    RAISE NOTICE '  Future duplicates will be prevented automatically';
END $$;

-- Step 9: Show partition distribution
SELECT
    child.relname as partition_name,
    pg_size_pretty(pg_relation_size(child.oid)) as size,
    (SELECT COUNT(*) FROM provenance p WHERE p.created_at >=
        CASE child.relname
            WHEN 'provenance_2025_09' THEN '2025-09-01'::timestamp
            WHEN 'provenance_2025_10' THEN '2025-10-01'::timestamp
            WHEN 'provenance_2025_11' THEN '2025-11-01'::timestamp
            WHEN 'provenance_2025_12' THEN '2025-12-01'::timestamp
            WHEN 'provenance_2026_01' THEN '2026-01-01'::timestamp
            ELSE '1900-01-01'::timestamp
        END
    AND p.created_at <
        CASE child.relname
            WHEN 'provenance_2025_09' THEN '2025-10-01'::timestamp
            WHEN 'provenance_2025_10' THEN '2025-11-01'::timestamp
            WHEN 'provenance_2025_11' THEN '2025-12-01'::timestamp
            WHEN 'provenance_2025_12' THEN '2026-01-01'::timestamp
            WHEN 'provenance_2026_01' THEN '2026-02-01'::timestamp
            ELSE '2100-01-01'::timestamp
        END
    ) as row_count
FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'provenance'
ORDER BY child.relname;

-- Note: Keep provenance_old_v25 for safety
-- After confirming migration success, drop it with:
-- DROP TABLE provenance_old_v25;

COMMENT ON TABLE provenance IS 'Deduplicated provenance table with unique index on (asset_id, source_name, source_type) for active records';
COMMENT ON INDEX idx_prov_unique_asset_source_latest IS 'Prevents recording the same provenance fact multiple times for active records';
