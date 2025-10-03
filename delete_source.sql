-- Delete the source so we can run fresh
DELETE FROM sources
WHERE source_type = 'iowa_gov_database'
  AND source_name LIKE '%Active_Iowa_Business%';

SELECT COUNT(*) as sources_remaining
FROM sources
WHERE source_type = 'iowa_gov_database';
