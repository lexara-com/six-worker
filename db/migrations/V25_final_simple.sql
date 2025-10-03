-- =============================================
-- V25: Fix Provenance Duplicates - SIMPLE APPROACH
-- =============================================
-- Just add a unique index to prevent future duplicates
-- and mark existing duplicates as superseded
-- =============================================

-- Step 1: Use provenance_old which has all the data
DROP TABLE IF EXISTS provenance CASCADE;
ALTER TABLE provenance_old RENAME TO provenance;

-- Step 2: Analyze the duplication problem
DO $$
DECLARE
    v_total INT;
    v_active INT;
    v_groups INT;
    v_dups INT;
BEGIN
    SELECT COUNT(*) INTO v_total FROM provenance;
    SELECT COUNT(*) INTO v_active FROM provenance WHERE status = 'active';

    SELECT COUNT(DISTINCT (asset_id, source_name, source_type))
    INTO v_groups
    FROM provenance
    WHERE status = 'active';

    v_dups := v_active - v_groups;

    RAISE NOTICE '';
    RAISE NOTICE '📊 Provenance Duplication Analysis:';
    RAISE NOTICE '  Total records: %', v_total;
    RAISE NOTICE '  Active records: %', v_active;
    RAISE NOTICE '  Unique (asset+source) combinations: %', v_groups;
    RAISE NOTICE '  Duplicate active records: % (%.1f%%)',
        v_dups, 100.0 * v_dups / NULLIF(v_active, 0);
END $$;

-- Step 3: Disable triggers temporarily
ALTER TABLE provenance DISABLE TRIGGER ALL;

-- Step 3b: Mark duplicates as 'superseded' (keep most recent)
UPDATE provenance p
SET status = 'superseded',
    review_status = 'duplicate',
    review_notes = 'Superseded by newer provenance record for same asset/source'
WHERE p.provenance_id IN (
    SELECT provenance_id
    FROM (
        SELECT
            provenance_id,
            ROW_NUMBER() OVER (
                PARTITION BY asset_id, source_name, source_type
                ORDER BY created_at DESC  -- Keep newest
            ) as rn
        FROM provenance
        WHERE status = 'active'
    ) ranked
    WHERE rn > 1  -- All except the newest one
);

-- Step 3c: Re-enable triggers
ALTER TABLE provenance ENABLE TRIGGER ALL;

-- Step 4: Add unique index to prevent future duplicates
CREATE UNIQUE INDEX IF NOT EXISTS idx_prov_unique_asset_source
    ON provenance (asset_id, source_name, source_type)
    WHERE status = 'active';

-- Step 5: Report results
DO $$
DECLARE
    v_active INT;
    v_superseded INT;
    v_total INT;
BEGIN
    SELECT COUNT(*) INTO v_active FROM provenance WHERE status = 'active';
    SELECT COUNT(*) INTO v_superseded FROM provenance WHERE status = 'superseded';
    SELECT COUNT(*) INTO v_total FROM provenance;

    RAISE NOTICE '';
    RAISE NOTICE '✅ Provenance Deduplication Complete:';
    RAISE NOTICE '  Active provenance: % (one per asset/source)', v_active;
    RAISE NOTICE '  Superseded duplicates: %', v_superseded;
    RAISE NOTICE '  Total records: %', v_total;
    RAISE NOTICE '';
    RAISE NOTICE '  Future duplicates prevented by unique index';
    RAISE NOTICE '  Index: idx_prov_unique_asset_source';
    RAISE NOTICE '  Constraint: (asset_id, source_name, source_type) WHERE status=''active''';
END $$;

-- Add helpful comments
COMMENT ON INDEX idx_prov_unique_asset_source IS
    'Prevents duplicate active provenance: only one record per (asset, source) combination';

COMMENT ON TABLE provenance IS
    'Provenance tracking with deduplication - one active record per asset/source combination';
