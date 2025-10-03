-- =============================================
-- Demonstrate Source Versioning for Quarterly Updates
-- =============================================

-- Check the source we just created
SELECT 'Current source record:' as info;
SELECT 
    source_id,
    source_type,
    source_name,
    source_version,
    file_name,
    download_date,
    records_in_file,
    status
FROM sources;

-- Simulate what would happen with quarterly updates
SELECT 'Simulating quarterly updates:' as info;

-- Q1 2025 Import (hypothetical - already imported)
INSERT INTO sources (
    source_type, source_name, source_version, file_name,
    download_url, download_date, import_completed_at,
    records_in_file, records_imported, status
) VALUES (
    'iowa_gov_database',
    'Active Iowa Business Entities', 
    '2025-Q1',
    'Active_Iowa_Business_Entities_20250101.csv',
    'https://data.iowa.gov/api/views/ez5t-3qay/rows.csv',
    '2025-01-02 10:00:00'::timestamp,
    '2025-01-02 14:30:00'::timestamp,
    298500, 298450, 'completed'
);

-- Q2 2025 Import (hypothetical future)
INSERT INTO sources (
    source_type, source_name, source_version, file_name,
    download_url, download_date, status
) VALUES (
    'iowa_gov_database',
    'Active Iowa Business Entities',
    '2025-Q2',
    'Active_Iowa_Business_Entities_20250401.csv',
    'https://data.iowa.gov/api/views/ez5t-3qay/rows.csv',
    '2025-04-01 10:00:00'::timestamp,
    'pending'
);

-- Show version history
SELECT 'Version history for Iowa data:' as info;
SELECT 
    source_version,
    download_date,
    import_completed_at,
    records_in_file,
    records_imported,
    status,
    CASE 
        WHEN import_completed_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (import_completed_at - import_started_at))/3600 || ' hours'
        ELSE 'N/A'
    END as processing_time
FROM sources
WHERE source_type = 'iowa_gov_database'
ORDER BY download_date DESC;

-- How to link provenance to specific source versions
SELECT 'How provenance would link to sources:' as info;
SELECT '
Future provenance records would include source_id:

INSERT INTO provenance (
    asset_type, 
    asset_id, 
    source_name,
    source_type,
    source_id,  -- NEW: Links to specific version!
    confidence_score
) VALUES (
    ''node'',
    ''<node_id>'',
    ''Active Iowa Business Entities'',
    ''iowa_gov_database'',
    ''<source_id_for_2025_Q2>'',  -- Specific quarterly version
    0.95
);
' as example;

-- Benefits demonstration
SELECT 'Benefits of source versioning:' as info;
SELECT '
1. Track changes between quarters:
   - Compare Q1 vs Q2 to find new companies
   - Identify companies that closed
   - Track name changes

2. Audit trail:
   - Know exactly which version of data each record came from
   - Can rollback a bad import
   - Can re-process a specific version

3. Data quality:
   - Track import success rates over time
   - Identify problematic source files
   - Monitor data source reliability

4. Compliance:
   - Maintain chain of custody for legal data
   - Prove when data was obtained
   - Document data freshness for conflicts checking
' as benefits;

-- Query example: Find all nodes from a specific quarterly import
SELECT 'Example: Count nodes by source version:' as info;
SELECT 
    s.source_version,
    s.download_date,
    COUNT(DISTINCT p.asset_id) as node_count
FROM sources s
LEFT JOIN provenance p ON s.source_id = p.source_id AND p.asset_type = 'node'
WHERE s.source_type = 'iowa_gov_database'
GROUP BY s.source_version, s.download_date
ORDER BY s.download_date DESC;

-- Clean up hypothetical records (keep original)
DELETE FROM sources 
WHERE source_version IN ('2025-Q1', '2025-Q2');

SELECT 'Demonstration complete - hypothetical records removed' as status;