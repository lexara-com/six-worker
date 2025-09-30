-- =============================================
-- Performance Testing Suite
-- Purpose: Benchmark queries and identify bottlenecks
-- =============================================

-- =============================================
-- Performance Test Setup
-- =============================================

-- Enable timing for all queries
\timing on

-- Show query plans and execution stats
SET work_mem = '256MB';
SET random_page_cost = 1.1; -- SSD optimization
SET effective_cache_size = '4GB';

-- =============================================
-- Test 1: Name Resolution Performance
-- Tests the most common operation: finding entities by name/alias
-- =============================================

SELECT '=== PERFORMANCE TEST 1: Name Resolution ===' as test_section;

-- Baseline: Direct name lookup (should be very fast)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT n.node_id, n.primary_name, n.node_type
FROM nodes n 
WHERE n.normalized_name = normalize_name('John Smith')
AND n.status = 'active';

-- Alias lookup performance (more complex, involves JOIN)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) 
SELECT DISTINCT n.node_id, n.primary_name, n.node_type
FROM nodes n
LEFT JOIN attributes a ON n.node_id = a.node_id
WHERE (n.normalized_name = normalize_name('J. Smith') 
       OR (a.attribute_type = 'nameAlias' 
           AND a.normalized_value = normalize_name('J. Smith')
           AND a.status = 'active'))
AND n.status = 'active';

-- Fuzzy name matching using trigram indexes
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT n.node_id, n.primary_name, n.node_type,
       similarity(n.primary_name, 'John Smith') as similarity_score
FROM nodes n
WHERE n.primary_name % 'John Smith'  -- Uses pg_trgm index
AND n.status = 'active'
ORDER BY similarity_score DESC
LIMIT 10;

-- =============================================
-- Test 2: Relationship Traversal Performance  
-- Tests graph walking performance for conflict detection
-- =============================================

SELECT '=== PERFORMANCE TEST 2: Relationship Traversal ===' as test_section;

-- 1-degree relationship lookup (direct connections)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT r.relationship_type, 
       s.primary_name as source_name,
       t.primary_name as target_name,
       r.strength
FROM relationships r
JOIN nodes s ON r.source_node_id = s.node_id
JOIN nodes t ON r.target_node_id = t.node_id  
WHERE r.source_node_id = '22222222-2222-2222-2222-222222222221'::UUID
AND r.status = 'active'
AND s.status = 'active' 
AND t.status = 'active';

-- 2-degree relationship traversal
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH two_degree_paths AS (
    SELECT 
        r1.source_node_id as start_node,
        r2.target_node_id as end_node,
        r1.relationship_type as first_relationship,
        r2.relationship_type as second_relationship,
        r1.strength * r2.strength as combined_strength
    FROM relationships r1
    JOIN relationships r2 ON r1.target_node_id = r2.source_node_id
    WHERE r1.source_node_id = '22222222-2222-2222-2222-222222222221'::UUID
    AND r1.status = 'active'
    AND r2.status = 'active'
)
SELECT 
    s.primary_name as start_name,
    tdp.first_relationship,
    tdp.second_relationship, 
    e.primary_name as end_name,
    tdp.combined_strength
FROM two_degree_paths tdp
JOIN nodes s ON tdp.start_node = s.node_id
JOIN nodes e ON tdp.end_node = e.node_id
WHERE s.status = 'active' AND e.status = 'active'
ORDER BY tdp.combined_strength DESC;

-- 3-degree recursive traversal (most complex)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH RECURSIVE relationship_paths AS (
    -- Base case: 1-degree relationships
    SELECT 
        r.source_node_id as start_id,
        r.target_node_id as current_id,
        r.relationship_type,
        1 as degree,
        ARRAY[r.source_node_id, r.target_node_id] as path_ids,
        ARRAY[r.relationship_type] as relationship_types,
        r.strength
    FROM relationships r
    WHERE r.source_node_id = '33333333-3333-3333-3333-333333333331'::UUID
    AND r.status = 'active'
    
    UNION ALL
    
    -- Recursive case: extend paths
    SELECT 
        rp.start_id,
        r.target_node_id as current_id,
        r.relationship_type,
        rp.degree + 1,
        rp.path_ids || r.target_node_id,
        rp.relationship_types || r.relationship_type,
        rp.strength * r.strength
    FROM relationship_paths rp
    JOIN relationships r ON rp.current_id = r.source_node_id
    WHERE rp.degree < 3
    AND r.status = 'active'
    AND NOT (r.target_node_id = ANY(rp.path_ids)) -- Prevent cycles
)
SELECT 
    rp.degree,
    array_to_string(rp.relationship_types, ' → ') as relationship_chain,
    rp.strength,
    COUNT(*) as path_count
FROM relationship_paths rp
GROUP BY rp.degree, rp.relationship_types, rp.strength
ORDER BY rp.degree, rp.strength DESC;

-- =============================================
-- Test 3: Conflict Matrix Performance
-- Tests pre-computed conflict detection
-- =============================================

SELECT '=== PERFORMANCE TEST 3: Conflict Matrix Lookups ===' as test_section;

-- Direct conflict matrix lookup (should be very fast)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT 
    cm.conflict_type,
    cm.conflict_path,
    cm.conflict_strength,
    cm.degrees_of_separation,
    n1.primary_name as entity_a_name,
    n2.primary_name as entity_b_name
FROM conflict_matrix cm
JOIN nodes n1 ON cm.entity_a_id = n1.node_id
JOIN nodes n2 ON cm.entity_b_id = n2.node_id
WHERE (cm.entity_a_id = '33333333-3333-3333-3333-333333333331'::UUID
       OR cm.entity_b_id = '33333333-3333-3333-3333-333333333331'::UUID)
AND (cm.expires_at IS NULL OR cm.expires_at > CURRENT_TIMESTAMP);

-- Bulk conflict check for multiple entities
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT 
    cm.conflict_type,
    cm.conflict_strength,
    n1.primary_name as entity_a,
    n2.primary_name as entity_b
FROM conflict_matrix cm
JOIN nodes n1 ON cm.entity_a_id = n1.node_id
JOIN nodes n2 ON cm.entity_b_id = n2.node_id
WHERE (cm.entity_a_id = ANY(ARRAY[
        '33333333-3333-3333-3333-333333333331'::UUID,
        '44444444-4444-4444-4444-444444444441'::UUID,
        '77777777-7777-7777-7777-777777777771'::UUID
    ]::UUID[])
    OR cm.entity_b_id = ANY(ARRAY[
        '33333333-3333-3333-3333-333333333331'::UUID,
        '44444444-4444-4444-4444-444444444441'::UUID,
        '77777777-7777-7777-7777-777777777771'::UUID
    ]::UUID[]))
AND (cm.expires_at IS NULL OR cm.expires_at > CURRENT_TIMESTAMP)
ORDER BY cm.conflict_strength DESC;

-- =============================================
-- Test 4: Comprehensive Conflict Check Performance
-- Tests the full conflict checking function
-- =============================================

SELECT '=== PERFORMANCE TEST 4: Full Conflict Check ===' as test_section;

-- Time the comprehensive conflict check function
SELECT 'Starting comprehensive conflict check...' as status;
\timing on

SELECT * FROM comprehensive_conflict_check(
    ARRAY['ACME Corporation', 'Robert Brown', 'Jennifer White'],
    'Complex multi-party litigation'
);

-- =============================================
-- Test 5: Bulk Operations Performance
-- Tests performance under load
-- =============================================

SELECT '=== PERFORMANCE TEST 5: Bulk Operations ===' as test_section;

-- Bulk entity lookup (simulating batch processing)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT 
    n.node_id,
    n.primary_name,
    n.node_type,
    COUNT(r.relationship_id) as relationship_count,
    COUNT(a.attribute_id) as attribute_count
FROM nodes n
LEFT JOIN relationships r ON (n.node_id = r.source_node_id OR n.node_id = r.target_node_id)
    AND r.status = 'active'
LEFT JOIN attributes a ON n.node_id = a.node_id AND a.status = 'active'  
WHERE n.primary_name = ANY(ARRAY[
    'John Smith', 'ACME Corporation', 'TechCorp Industries',
    'Mary Johnson', 'Robert Brown', 'Jennifer White'
])
AND n.status = 'active'
GROUP BY n.node_id, n.primary_name, n.node_type;

-- Bulk relationship insertion performance test
SELECT 'Testing bulk relationship insertion...' as status;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    i INTEGER;
BEGIN
    start_time := clock_timestamp();
    
    -- Insert 1000 test relationships
    FOR i IN 1..1000 LOOP
        INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength)
        VALUES (
            '22222222-2222-2222-2222-222222222221'::UUID,
            '33333333-3333-3333-3333-333333333331'::UUID,
            'Test_Relationship',
            random()
        );
    END LOOP;
    
    end_time := clock_timestamp();
    
    RAISE NOTICE 'Bulk insert of 1000 relationships took: %', (end_time - start_time);
    
    -- Clean up test data
    DELETE FROM relationships WHERE relationship_type = 'Test_Relationship';
END $$;

-- =============================================
-- Test 6: Index Usage Analysis
-- Verifies that our indexes are being used effectively
-- =============================================

SELECT '=== PERFORMANCE TEST 6: Index Usage Analysis ===' as test_section;

-- Check index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    CASE WHEN idx_scan = 0 THEN 'UNUSED' ELSE 'USED' END as status
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Check table scan statistics
SELECT 
    schemaname,
    tablename,
    seq_scan as sequential_scans,
    seq_tup_read as seq_tuples_read,  
    idx_scan as index_scans,
    idx_tup_fetch as idx_tuples_fetched,
    ROUND(
        CASE WHEN (seq_scan + idx_scan) = 0 THEN 0 
        ELSE (idx_scan::FLOAT / (seq_scan + idx_scan) * 100) 
        END, 2
    ) as index_usage_percent
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY index_usage_percent DESC;

-- =============================================
-- Test 7: Memory and Cache Performance
-- Tests query memory usage and cache efficiency
-- =============================================

SELECT '=== PERFORMANCE TEST 7: Cache and Memory Analysis ===' as test_section;

-- Check PostgreSQL cache hit ratio
SELECT 
    'Buffer Cache Hit Ratio' as metric,
    ROUND(
        (blks_hit::FLOAT / (blks_hit + blks_read) * 100), 2
    ) as percentage
FROM pg_stat_database 
WHERE datname = current_database();

-- Check individual table cache ratios
SELECT 
    schemaname,
    tablename,
    ROUND(
        CASE WHEN (heap_blks_hit + heap_blks_read) = 0 THEN 0
        ELSE (heap_blks_hit::FLOAT / (heap_blks_hit + heap_blks_read) * 100)
        END, 2
    ) as cache_hit_ratio,
    heap_blks_read as disk_reads,
    heap_blks_hit as cache_hits
FROM pg_statio_user_tables
WHERE schemaname = 'public'
ORDER BY cache_hit_ratio DESC;

-- =============================================
-- Performance Summary and Recommendations
-- =============================================

SELECT '=== PERFORMANCE SUMMARY ===' as section;

-- Table sizes and row counts
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Most time-consuming queries (if pg_stat_statements is enabled)
SELECT 'Query performance analysis requires pg_stat_statements extension' as note;

-- Vacuum and analyze recommendations  
SELECT 
    'VACUUM ANALYZE ' || schemaname || '.' || tablename || ';' as maintenance_command
FROM pg_stat_user_tables
WHERE schemaname = 'public'
AND (n_dead_tup > n_live_tup * 0.1 OR analyze_count = 0);

-- =============================================
-- Cleanup
-- =============================================

-- Reset settings
RESET work_mem;
RESET random_page_cost; 
RESET effective_cache_size;

\timing off

SELECT 'Performance testing completed. Review EXPLAIN ANALYZE outputs above for detailed analysis.' as conclusion;