# Database Index Optimization Results

## Problem Identified
The data loaders were experiencing severe performance degradation as data volume grew:
- Initial rate: 1-2 companies/minute 
- Would take 104 days to load 300,000 records
- Root cause: Missing indexes causing full table scans during entity resolution

## Analysis Results

### Table Sizes
- **Nodes**: 67,219 rows
- **Relationships**: 118,021 rows  
- **Attributes**: 132,799 rows
- **Provenance**: 286,632 rows

### Critical Missing Indexes Identified
1. `nodes(node_type, primary_name)` - For exact entity matching
2. `nodes(node_type, UPPER(primary_name))` - For case-insensitive matching
3. `relationships(source_node_id, target_node_id, relationship_type)` - For duplicate detection
4. `attributes(attribute_type, attribute_value)` - For attribute-based lookups

## Indexes Created

```sql
-- Entity resolution indexes
CREATE INDEX idx_nodes_type_primary_name ON nodes(node_type, primary_name);
CREATE INDEX idx_nodes_type_upper_name ON nodes(node_type, UPPER(primary_name));

-- Relationship duplicate detection
CREATE INDEX idx_relationships_triple ON relationships(source_node_id, target_node_id, relationship_type);
CREATE INDEX idx_relationships_target_source_type ON relationships(target_node_id, source_node_id, relationship_type);

-- Attribute lookups
CREATE INDEX idx_attributes_type_value_exact ON attributes(attribute_type, attribute_value);
CREATE INDEX idx_attributes_lookup ON attributes(attribute_type, attribute_value, node_id);
```

## Performance Improvements

### Query Performance (Before vs After)
| Query Type | Before | After | Improvement |
|------------|--------|-------|-------------|
| Entity lookup by name | ~500ms+ | 0.06ms | **8,000x faster** |
| Case-insensitive lookup | ~600ms+ | 0.08ms | **7,500x faster** |
| Relationship duplicate check | ~400ms+ | 0.05ms | **8,000x faster** |

### Loader Performance
- **Before indexes**: 1-2 companies/minute
- **After indexes**: 79 companies/minute  
- **Improvement**: **40-50x faster**

## Remaining Issues

Despite the dramatic improvement, loading 300,000 records would still take ~58 hours. Additional optimizations needed:

1. **Batch Processing**: Process multiple records in single transaction
2. **Connection Pooling**: Reuse database connections
3. **Parallel Processing**: Use multiple workers
4. **Bulk Operations**: Use COPY instead of individual INSERTs for initial load
5. **Stored Procedures**: Move propose_fact logic closer to data

## Recommendations

### Immediate Actions
1. ✅ Indexes have been created and are working
2. ⚠️ Consider running VACUUM ANALYZE regularly as data grows
3. ⚠️ Monitor index bloat and rebuild if necessary

### For Production Deployment
1. Add these indexes to the base schema migrations
2. Set up automatic VACUUM and ANALYZE schedules
3. Consider partitioning large tables (provenance) by date
4. Implement connection pooling in the application layer
5. Use bulk loading for initial data imports

## Monitoring Queries

```sql
-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read
FROM pg_stat_user_indexes
WHERE idx_scan > 0
ORDER BY idx_scan DESC;

-- Check for slow queries (requires pg_stat_statements)
SELECT 
    query,
    mean_exec_time,
    calls
FROM pg_stat_statements
WHERE mean_exec_time > 10
ORDER BY mean_exec_time DESC;

-- Check table bloat
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as size,
    n_dead_tup,
    n_live_tup
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC;
```

## Conclusion

The index optimization was highly successful, achieving a **40-50x performance improvement**. However, for datasets of 300,000+ records, additional architectural changes are needed beyond indexing alone.