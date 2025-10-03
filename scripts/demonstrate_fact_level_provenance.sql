-- =============================================
-- Demonstrate Fact-Level Provenance Adjustments
-- =============================================

-- Current provenance structure already supports fact-level notes
SELECT 'Current provenance columns for fact-level tracking:' as info;
SELECT 
    column_name,
    data_type,
    column_default,
    CASE 
        WHEN column_name = 'notes' THEN '✓ For human review comments'
        WHEN column_name = 'metadata' THEN '✓ For structured annotations'
        WHEN column_name = 'confidence_score' THEN '✓ Can be adjusted post-review'
        WHEN column_name = 'reliability_rating' THEN '✓ Can be updated after verification'
        WHEN column_name = 'created_by' THEN '✓ Track who added/reviewed'
        ELSE ''
    END as usage
FROM information_schema.columns
WHERE table_name = 'provenance'
  AND column_name IN ('notes', 'metadata', 'confidence_score', 'reliability_rating', 'created_by', 'updated_at')
ORDER BY 
    CASE column_name
        WHEN 'notes' THEN 1
        WHEN 'metadata' THEN 2
        WHEN 'confidence_score' THEN 3
        WHEN 'reliability_rating' THEN 4
        ELSE 5
    END;

-- Add columns for better review tracking
ALTER TABLE provenance 
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR(100),
    ADD COLUMN IF NOT EXISTS review_status VARCHAR(50) CHECK (review_status IN ('pending', 'reviewed', 'verified', 'disputed', 'corrected')),
    ADD COLUMN IF NOT EXISTS review_notes TEXT;

-- Example: Mark a specific fact as human-reviewed
SELECT 'Example of human review annotation:' as info;

-- Find a sample company node to annotate
WITH sample_company AS (
    SELECT p.provenance_id, n.primary_name, p.asset_id
    FROM provenance p
    JOIN nodes n ON p.asset_id = n.node_id
    WHERE p.asset_type = 'node' 
      AND n.node_type = 'Company'
      AND n.primary_name LIKE '%TUBBS%'
    LIMIT 1
)
UPDATE provenance p
SET 
    notes = COALESCE(notes || E'\n', '') || 'Human reviewed on 10/2/2025 - Verified against state records',
    reviewed_at = '2025-10-02 14:30:00'::timestamp,
    reviewed_by = 'John Smith',
    review_status = 'verified',
    review_notes = 'Confirmed company is active. Address verified via Google Maps. 
                    Registered agent confirmed via phone call.',
    confidence_score = 1.0,  -- Increased from default after verification
    reliability_rating = 'high',  -- Changed from 'unknown' after verification
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'review_method', 'manual_verification',
        'verification_sources', ARRAY['state_records', 'google_maps', 'phone_call'],
        'last_verified', '2025-10-02'
    )
FROM sample_company sc
WHERE p.provenance_id = sc.provenance_id
RETURNING 
    p.provenance_id,
    p.asset_id,
    p.notes,
    p.reviewed_at,
    p.review_status;

-- Example: Track corrections/disputes
SELECT 'Example of disputed/corrected data:' as info;

WITH sample_address AS (
    SELECT p.provenance_id
    FROM provenance p
    JOIN nodes n ON p.asset_id = n.node_id
    WHERE p.asset_type = 'node' 
      AND n.node_type = 'Address'
    LIMIT 1
)
UPDATE provenance p
SET 
    review_status = 'corrected',
    review_notes = 'Original address had wrong ZIP code. Corrected from 52037 to 52038.',
    reviewed_at = CURRENT_TIMESTAMP,
    reviewed_by = 'Jane Doe',
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'original_value', '52037',
        'corrected_value', '52038',
        'correction_reason', 'USPS database showed different ZIP'
    )
FROM sample_address sa
WHERE p.provenance_id = sa.provenance_id
RETURNING 
    p.provenance_id,
    p.review_status,
    p.review_notes;

-- Create view for review workflow
CREATE OR REPLACE VIEW provenance_review_status AS
SELECT 
    p.asset_type,
    p.asset_id,
    n.primary_name as asset_name,
    p.source_name,
    p.confidence_score,
    p.review_status,
    p.reviewed_at,
    p.reviewed_by,
    p.review_notes,
    CASE 
        WHEN p.review_status = 'verified' THEN '✓'
        WHEN p.review_status = 'disputed' THEN '⚠'
        WHEN p.review_status = 'corrected' THEN '✎'
        WHEN p.review_status = 'reviewed' THEN '👁'
        ELSE '○'
    END as status_icon,
    p.metadata->>'last_verified' as last_verified_date
FROM provenance p
LEFT JOIN nodes n ON p.asset_id = n.node_id AND p.asset_type = 'node'
WHERE p.review_status IS NOT NULL
ORDER BY p.reviewed_at DESC;

-- Show review statistics
SELECT 'Review statistics:' as info;
SELECT 
    COALESCE(review_status, 'not_reviewed') as status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM provenance
GROUP BY review_status
ORDER BY count DESC;

-- Example queries for compliance/audit
SELECT 'Useful queries for compliance:' as info;

-- 1. Find all facts reviewed in last 30 days
SELECT 'Facts reviewed in last 30 days:' as query_1;
SELECT COUNT(*) as recently_reviewed
FROM provenance
WHERE reviewed_at > CURRENT_DATE - INTERVAL '30 days';

-- 2. Find facts that need review (low confidence, not reviewed)
SELECT 'Facts needing review (low confidence):' as query_2;
SELECT COUNT(*) as needs_review
FROM provenance
WHERE confidence_score < 0.8
  AND review_status IS NULL;

-- 3. Track reviewer productivity
SELECT 'Reviewer productivity:' as query_3;
SELECT 
    reviewed_by,
    COUNT(*) as facts_reviewed,
    DATE_TRUNC('week', reviewed_at) as review_week
FROM provenance
WHERE reviewed_by IS NOT NULL
  AND reviewed_at > CURRENT_DATE - INTERVAL '3 months'
GROUP BY reviewed_by, DATE_TRUNC('week', reviewed_at)
ORDER BY review_week DESC, facts_reviewed DESC
LIMIT 10;

-- Create trigger to track updates
CREATE OR REPLACE FUNCTION track_provenance_updates()
RETURNS TRIGGER AS $$
BEGIN
    -- Auto-update the updated_at timestamp
    NEW.updated_at = CURRENT_TIMESTAMP;
    
    -- If review fields are being set, ensure reviewed_at is set
    IF NEW.review_status IS NOT NULL AND OLD.review_status IS NULL THEN
        NEW.reviewed_at = COALESCE(NEW.reviewed_at, CURRENT_TIMESTAMP);
    END IF;
    
    -- Log significant changes to metadata
    IF OLD.confidence_score != NEW.confidence_score THEN
        NEW.metadata = COALESCE(NEW.metadata, '{}'::jsonb) || 
            jsonb_build_object(
                'confidence_history', 
                COALESCE(NEW.metadata->'confidence_history', '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'old_score', OLD.confidence_score,
                    'new_score', NEW.confidence_score,
                    'changed_at', CURRENT_TIMESTAMP,
                    'changed_by', NEW.reviewed_by
                ))
            );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add the trigger if it doesn't exist
DROP TRIGGER IF EXISTS trg_provenance_updates ON provenance;
CREATE TRIGGER trg_provenance_updates
    BEFORE UPDATE ON provenance
    FOR EACH ROW
    EXECUTE FUNCTION track_provenance_updates();

SELECT 'Fact-level provenance tracking enhancements complete!' as status;