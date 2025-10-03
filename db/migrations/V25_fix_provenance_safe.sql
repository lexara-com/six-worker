-- =============================================
-- V25: Fix Provenance Duplicates (SAFE VERSION)
-- =============================================

-- Step 1: Check current state
DO $$
DECLARE
    v_has_provenance BOOLEAN;
    v_has_old BOOLEAN;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'provenance') INTO v_has_provenance;
    SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'provenance_old') INTO v_has_old;

    RAISE NOTICE 'Current state:';
    RAISE NOTICE '  provenance table exists: %', v_has_provenance;
    RAISE NOTICE '  provenance_old exists: %', v_has_old;

    IF NOT v_has_provenance AND NOT v_has_old THEN
        RAISE EXCEPTION 'No provenance table found!';
    END IF;
END $$;

-- Step 2: If provenance doesn't exist, restore from provenance_old
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'provenance') THEN
        RAISE NOTICE 'Restoring provenance from provenance_old...';
        ALTER TABLE provenance_old RENAME TO provenance;
    END IF;
END $$;

-- Step 3: Analyze duplicates
DO $$
DECLARE
    v_total INT;
    v_unique INT;
    v_dups INT;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT (asset_id, source_name, source_type))
    INTO v_total, v_unique
    FROM provenance;

    v_dups := v_total - v_unique;

    RAISE NOTICE '';
    RAISE NOTICE 'Provenance Analysis:';
    RAISE NOTICE '  Total records: %', v_total;
    RAISE NOTICE '  Unique (asset+source): %', v_unique;
    RAISE NOTICE '  Duplicates: % (%.1f%%)', v_dups, 100.0 * v_dups / v_total;
END $$;

-- Step 4: Add unique index if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE indexname = 'idx_prov_unique_asset_source_latest'
    ) THEN
        RAISE NOTICE 'Creating unique index to prevent future duplicates...';

        CREATE UNIQUE INDEX idx_prov_unique_asset_source_latest
            ON provenance (asset_id, source_name, source_type)
            WHERE status = 'active';

        RAISE NOTICE '✅ Unique index created successfully';
    ELSE
        RAISE NOTICE 'Unique index already exists';
    END IF;
END $$;

-- Step 5: Clean up duplicates (mark older ones as 'superseded')
UPDATE provenance p
SET status = 'superseded'
WHERE p.provenance_id IN (
    SELECT provenance_id
    FROM (
        SELECT
            provenance_id,
            asset_id,
            source_name,
            source_type,
            ROW_NUMBER() OVER (
                PARTITION BY asset_id, source_name, source_type
                ORDER BY created_at DESC
            ) as rn
        FROM provenance
        WHERE status = 'active'
    ) sub
    WHERE rn > 1  -- Keep newest, supersede older ones
);

-- Step 6: Report results
DO $$
DECLARE
    v_active INT;
    v_superseded INT;
BEGIN
    SELECT COUNT(*) INTO v_active FROM provenance WHERE status = 'active';
    SELECT COUNT(*) INTO v_superseded FROM provenance WHERE status = 'superseded';

    RAISE NOTICE '';
    RAISE NOTICE '✅ Deduplication Complete:';
    RAISE NOTICE '  Active provenance records: %', v_active;
    RAISE NOTICE '  Superseded (old duplicates): %', v_superseded;
    RAISE NOTICE '  Total records: %', v_active + v_superseded;
    RAISE NOTICE '';
    RAISE NOTICE 'Future duplicates prevented by unique index';
END $$;

-- Step 7: Add helpful comment
COMMENT ON INDEX idx_prov_unique_asset_source_latest IS
    'Prevents duplicate provenance records for the same asset from the same source';
