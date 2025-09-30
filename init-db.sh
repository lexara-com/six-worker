#!/bin/bash
set -e

echo "🚀 Initializing Six Worker Graph Database..."

# Wait for PostgreSQL to be ready
until pg_isready -h localhost -p 5432 -U $POSTGRES_USER -d $POSTGRES_DB; do
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "✅ PostgreSQL is ready!"

# Run migrations in order
echo "📦 Running schema migrations..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/V1__initial_schema.sql
echo "✅ V1 schema migration completed"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/V2__indexes_and_performance.sql
echo "✅ V2 performance migration completed"

# Insert test data
echo "📊 Inserting test data..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/test-data/insert_test_data.sql
echo "✅ Test data insertion completed"

# Refresh materialized views
echo "🔄 Refreshing materialized views..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "REFRESH MATERIALIZED VIEW mv_entity_summary;"
echo "✅ Materialized views refreshed"

echo "🎉 Database initialization completed successfully!"

# Show summary
echo "📋 Database Summary:"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" << 'EOF'
SELECT 
    'Tables created:' as metric, 
    COUNT(*) as value 
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'

UNION ALL

SELECT 
    'Indexes created:', 
    COUNT(*) 
FROM pg_indexes 
WHERE schemaname = 'public'

UNION ALL

SELECT 
    'Test entities:', 
    COUNT(*) 
FROM nodes

UNION ALL

SELECT 
    'Test relationships:', 
    COUNT(*) 
FROM relationships

UNION ALL

SELECT 
    'Test attributes:', 
    COUNT(*) 
FROM attributes;
EOF

echo ""
echo "🌐 Access pgAdmin at: http://localhost:8080"
echo "   Email: admin@sixworker.com"
echo "   Password: admin123"
echo ""
echo "🔗 Direct PostgreSQL connection:"
echo "   Host: localhost"
echo "   Port: 5432"
echo "   Database: graph_db"
echo "   Username: graph_admin"
echo "   Password: dev_password_123"