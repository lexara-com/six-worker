-- =============================================
-- V26: Consolidate Provenance by Source
-- =============================================
-- Problem: Multiple provenance records per asset from same source
-- due to per-record source_name construction
--
-- Solution:
-- 1. Normalize source_name to remove record-specific suffixes
-- 2. Keep only one provenance record per (asset_id, source_name, source_type)
-- 3. Add unique constraint to prevent future duplicates
-- =============================================

-- Step 1: Analyze current duplication
DO $$
DECLARE
    v_total INT;
    v_unique_assets INT;
    v_unique_combinations INT;
BEGIN
    SELECT
        COUNT(*) as total,
        COUNT(DISTINCT asset_id) as unique_assets,
        COUNT(DISTINCT (asset_id,
            regexp_replace(source_name, ' - \d+$', ''),  -- Remove record suffix
            source_type
        )) as unique_combos
    INTO v_total, v_unique_assets, v_unique_combinations
    FROM provenance
    WHERE status = 'active';

    RAISE NOTICE '';
    RAISE NOTICE '📊 Provenance Consolidation Analysis:';
    RAISE NOTICE '  Total active records: %', v_total;
    RAISE NOTICE '  Unique assets: %', v_unique_assets;
    RAISE NOTICE '  Unique (asset+source) after normalization: %', v_unique_combinations;
    RAISE NOTICE '  Records to consolidate: %', v_total - v_unique_combinations;
    RAISE NOTICE '  Savings: %.1f%%', 100.0 * (v_total - v_unique_combinations) / v_total;
END $$;

-- Step 2a: Drop existing unique index if it exists
DROP INDEX IF EXISTS idx_prov_unique_asset_source;

-- Step 2b: Normalize source_name to remove record-specific suffixes
-- Pattern: "Active Iowa Business Entities - ... - 000123" -> "Active Iowa Business Entities - ..."
UPDATE provenance
SET source_name = regexp_replace(source_name, ' - \d{6}$', '')
WHERE source_name ~ ' - \d{6}$'
  AND status = 'active';

-- Step 3: Now consolidate duplicates (keep oldest as it was created first)
WITH ranked AS (
    SELECT
        provenance_id,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id, source_name, source_type
            ORDER BY created_at ASC  -- Keep oldest (first occurrence)
        ) as rn
    FROM provenance
    WHERE status = 'active'
)
UPDATE provenance p
SET status = 'superseded',
    review_status = 'consolidated',
    review_notes = 'Consolidated: multiple provenance records from same source for same asset'
FROM ranked r
WHERE p.provenance_id = r.provenance_id
  AND r.rn > 1;

-- Step 4: Create unique index to prevent future duplicates
DROP INDEX IF EXISTS idx_prov_unique_asset_source;

CREATE UNIQUE INDEX idx_prov_unique_asset_source
    ON provenance (asset_id, source_name, source_type)
    WHERE status = 'active';

-- Step 5: Report final state
DO $$
DECLARE
    v_active INT;
    v_superseded INT;
    v_unique_combos INT;
BEGIN
    SELECT COUNT(*) INTO v_active FROM provenance WHERE status = 'active';
    SELECT COUNT(*) INTO v_superseded FROM provenance WHERE status = 'superseded';

    SELECT COUNT(DISTINCT (asset_id, source_name, source_type))
    INTO v_unique_combos
    FROM provenance
    WHERE status = 'active';

    RAISE NOTICE '';
    RAISE NOTICE '✅ Provenance Consolidation Complete:';
    RAISE NOTICE '  Active provenance records: %', v_active;
    RAISE NOTICE '  Unique (asset+source) combinations: %', v_unique_combos;
    RAISE NOTICE '  Superseded duplicates: %', v_superseded;
    RAISE NOTICE '  Total records: %', v_active + v_superseded;
    RAISE NOTICE '';
    RAISE NOTICE '  ✓ Source names normalized';
    RAISE NOTICE '  ✓ One provenance record per asset/source';
    RAISE NOTICE '  ✓ Unique constraint enforces: (asset_id, source_name, source_type)';
    RAISE NOTICE '  ✓ Future duplicates prevented';

    IF v_active != v_unique_combos THEN
        RAISE WARNING 'Still have duplicates! Active: %, Unique: %', v_active, v_unique_combos;
    END IF;
END $$;

-- Step 6: Verify the fix with sample queries
DO $$
DECLARE
    v_sample_asset VARCHAR(26);
    v_dup_count INT;
BEGIN
    -- Pick a previously problematic asset (Des Moines)
    v_sample_asset := '01K6FNYHM79C07QTQZ8BNH9FFX';

    SELECT COUNT(*)
    INTO v_dup_count
    FROM provenance
    WHERE asset_id = v_sample_asset
      AND source_name LIKE 'Iowa Secretary%'
      AND status = 'active';

    RAISE NOTICE '';
    RAISE NOTICE '🔍 Verification (Des Moines city node):';
    RAISE NOTICE '  Asset ID: %', v_sample_asset;
    RAISE NOTICE '  Active provenance records: %', v_dup_count;
    RAISE NOTICE '  Expected: 1 (was 1,415 before fix)';

    IF v_dup_count != 1 THEN
        RAISE WARNING 'Des Moines still has % provenance records!', v_dup_count;
    END IF;
END $$;

-- Add helpful comments
COMMENT ON INDEX idx_prov_unique_asset_source IS
    'Enforces one active provenance record per (asset, source) - answers "where did this fact come from?"';

COMMENT ON COLUMN provenance.source_name IS
    'Source dataset name (consistent per dataset, not per record)';
