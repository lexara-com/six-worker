#!/bin/bash

# =============================================
# Local PostgreSQL Setup Script
# =============================================

set -e

echo "🐳 Setting up local PostgreSQL instance for Six Worker..."

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ docker-compose not found. Please install Docker Compose."
    exit 1
fi

echo "✅ Docker is ready"

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down -v 2>/dev/null || true

# Build and start services
echo "🚀 Starting PostgreSQL and pgAdmin..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for PostgreSQL to be ready
echo "   Waiting for PostgreSQL..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker exec six_worker_postgres pg_isready -U graph_admin -d graph_db >/dev/null 2>&1; then
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    echo "❌ PostgreSQL failed to start within 60 seconds"
    docker-compose logs postgres
    exit 1
fi

echo "✅ PostgreSQL is ready!"

# Wait for pgAdmin to be ready
echo "   Waiting for pgAdmin..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -f http://localhost:8080 >/dev/null 2>&1; then
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    echo "⚠️  pgAdmin may not be ready yet, but continuing..."
fi

echo "✅ Services are running!"

# Test database connection
echo "🔍 Testing database connection..."
docker exec six_worker_postgres psql -U graph_admin -d graph_db -c "SELECT 'Database connection successful!' as status;"

# Show table counts
echo "📊 Database statistics:"
docker exec six_worker_postgres psql -U graph_admin -d graph_db -c "
SELECT 
    'Tables' as type, COUNT(*) as count 
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'

UNION ALL

SELECT 
    'Entities', COUNT(*) 
FROM nodes

UNION ALL

SELECT 
    'Relationships', COUNT(*) 
FROM relationships

UNION ALL

SELECT 
    'Attributes', COUNT(*) 
FROM attributes

UNION ALL

SELECT 
    'Conflict Matrix', COUNT(*) 
FROM conflict_matrix;
"

echo ""
echo "🎉 Local setup completed successfully!"
echo ""
echo "📋 Access Information:"
echo "   🐘 PostgreSQL: localhost:5432"
echo "      Database: graph_db"
echo "      Username: graph_admin"
echo "      Password: dev_password_123"
echo ""
echo "   🌐 pgAdmin: http://localhost:8080"
echo "      Email: admin@sixworker.com"
echo "      Password: admin123"
echo ""
echo "🔧 Useful Commands:"
echo "   Connect to database: docker exec -it six_worker_postgres psql -U graph_admin -d graph_db"
echo "   View logs: docker-compose logs -f"
echo "   Stop services: docker-compose down"
echo "   Reset database: docker-compose down -v && ./local-setup.sh"
echo ""
echo "🧪 Run conflict tests:"
echo "   docker exec -i six_worker_postgres psql -U graph_admin -d graph_db < db/test-scripts/conflict_check_tests.sql"