UPDATE sources
SET status = 'pending',
    import_completed_at = NULL,
    records_imported = 0
WHERE source_type = 'iowa_gov_database'
  AND source_name LIKE '%Active_Iowa_Business%';

SELECT source_id, source_name, status, records_imported
FROM sources
WHERE source_type = 'iowa_gov_database'
ORDER BY created_at DESC
LIMIT 3;
