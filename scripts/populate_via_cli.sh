#!/bin/bash

# =============================================
# Aurora PostgreSQL Population via AWS CLI
# Uses RDS Data API to execute SQL commands
# =============================================

set -e

# Configuration
CLUSTER_ARN="arn:aws:rds:us-east-1:492149691043:cluster:dev-six-worker-cluster"
SECRET_ARN="arn:aws:secretsmanager:us-east-1:492149691043:secret:dev/six-worker/database-fmJYO8"
DATABASE="graph_db"
PROFILE="lexara_super_agent"
REGION="us-east-1"

echo "============================================================"
echo "🐘 Six Worker Aurora PostgreSQL Database Populator (CLI)"
echo "============================================================"
echo

echo "📍 Cluster: $CLUSTER_ARN"
echo "🔐 Secret: $SECRET_ARN"
echo "💾 Database: $DATABASE"
echo

# Function to execute SQL via RDS Data API
execute_sql() {
    local sql="$1"
    local description="$2"
    
    echo "   🔧 $description"
    echo "   SQL: ${sql:0:60}..."
    
    aws rds-data execute-statement \
        --resource-arn "$CLUSTER_ARN" \
        --secret-arn "$SECRET_ARN" \
        --database "$DATABASE" \
        --sql "$sql" \
        --profile "$PROFILE" \
        --region "$REGION" \
        --output json > /dev/null
        
    if [ $? -eq 0 ]; then
        echo "   ✅ Success"
    else
        echo "   ❌ Failed"
        return 1
    fi
}

# Step 1: Create extensions and basic schema
echo "🏗️  Step 1: Creating database schema..."

execute_sql 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";' "Enable UUID extension"

execute_sql 'CREATE TABLE IF NOT EXISTS nodes (
    node_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_type VARCHAR(50) NOT NULL CHECK (node_type IN ('"'"'Person'"'"', '"'"'Company'"'"', '"'"'Place'"'"', '"'"'Thing'"'"', '"'"'Event'"'"')),
    primary_name VARCHAR(255) NOT NULL,
    normalized_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT '"'"'active'"'"' CHECK (status IN ('"'"'active'"'"', '"'"'inactive'"'"', '"'"'deleted'"'"')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100)
);' "Create nodes table"

execute_sql 'CREATE TABLE IF NOT EXISTS relationships (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_node_id UUID NOT NULL,
    target_node_id UUID NOT NULL,
    relationship_type VARCHAR(50) NOT NULL,
    strength DECIMAL(3,2) DEFAULT 1.0,
    status VARCHAR(20) DEFAULT '"'"'active'"'"',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_relationships_source FOREIGN KEY (source_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT fk_relationships_target FOREIGN KEY (target_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);' "Create relationships table"

execute_sql 'CREATE TABLE IF NOT EXISTS attributes (
    attribute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_id UUID NOT NULL,
    attribute_type VARCHAR(50) NOT NULL,
    attribute_value VARCHAR(500) NOT NULL,
    normalized_value VARCHAR(500) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_attributes_node FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);' "Create attributes table"

echo "✅ Schema created successfully"
echo

# Step 2: Create indexes
echo "📊 Step 2: Creating indexes..."

execute_sql 'CREATE INDEX IF NOT EXISTS idx_nodes_normalized_name ON nodes(normalized_name);' "Index on normalized names"
execute_sql 'CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_node_id, relationship_type);' "Index on relationship sources"
execute_sql 'CREATE INDEX IF NOT EXISTS idx_attributes_node_type ON attributes(node_id, attribute_type);' "Index on attributes"

echo "✅ Indexes created successfully"
echo

# Step 3: Create helper functions
echo "🔧 Step 3: Creating helper functions..."

execute_sql 'CREATE OR REPLACE FUNCTION normalize_name(input_name TEXT) 
RETURNS TEXT AS $$
BEGIN
    RETURN LOWER(TRIM(REGEXP_REPLACE(input_name, '"'"'\s+'"'"', '"'"' '"'"', '"'"'g'"'"')));
END;
$$ LANGUAGE plpgsql IMMUTABLE;' "Create normalize_name function"

echo "✅ Helper functions created successfully"
echo

# Step 4: Insert test data
echo "👥 Step 4: Inserting test entities..."

# Insert nodes with explicit normalized names
execute_sql "INSERT INTO nodes (node_id, node_type, primary_name, normalized_name) VALUES 
('11111111-1111-1111-1111-111111111111', 'Company', 'Smith & Associates Law Firm', 'smith & associates law firm'),
('22222222-2222-2222-2222-222222222221', 'Person', 'John Smith', 'john smith'),
('22222222-2222-2222-2222-222222222222', 'Person', 'Mary Johnson', 'mary johnson'),
('33333333-3333-3333-3333-333333333331', 'Company', 'ACME Corporation', 'acme corporation'),
('44444444-4444-4444-4444-444444444441', 'Person', 'Robert Brown', 'robert brown'),
('55555555-5555-5555-5555-555555555551', 'Company', 'TechCorp Industries', 'techcorp industries'),
('66666666-6666-6666-6666-666666666661', 'Person', 'Jennifer White', 'jennifer white'),
('77777777-7777-7777-7777-777777777771', 'Person', 'Amanda Brown', 'amanda brown')
ON CONFLICT (node_id) DO NOTHING;" "Insert core entities"

echo "✅ Test entities inserted successfully"
echo

# Step 5: Insert attributes (aliases)
echo "🏷️  Step 5: Inserting attributes..."

execute_sql "INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value) VALUES 
('22222222-2222-2222-2222-222222222221', 'nameAlias', 'J. Smith', 'j. smith'),
('22222222-2222-2222-2222-222222222221', 'title', 'Senior Partner', 'senior partner'),
('22222222-2222-2222-2222-222222222222', 'nameAlias', 'M. Johnson', 'm. johnson'),
('33333333-3333-3333-3333-333333333331', 'nameAlias', 'ACME Corp', 'acme corp'),
('44444444-4444-4444-4444-444444444441', 'nameAlias', 'Bob Brown', 'bob brown'),
('44444444-4444-4444-4444-444444444441', 'title', 'CEO', 'ceo'),
('55555555-5555-5555-5555-555555555551', 'nameAlias', 'TechCorp Inc', 'techcorp inc'),
('66666666-6666-6666-6666-666666666661', 'nameAlias', 'Jenny White', 'jenny white'),
('77777777-7777-7777-7777-777777777771', 'nameAlias', 'A. Brown', 'a. brown')
ON CONFLICT DO NOTHING;" "Insert entity attributes"

echo "✅ Attributes inserted successfully"
echo

# Step 6: Insert relationships
echo "🔗 Step 6: Inserting relationships..."

execute_sql "INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('22222222-2222-2222-2222-222222222221', '11111111-1111-1111-1111-111111111111', 'Employment', 1.0),
('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'Employment', 1.0),
('44444444-4444-4444-4444-444444444441', '33333333-3333-3333-3333-333333333331', 'Employment', 1.0),
('66666666-6666-6666-6666-666666666661', '55555555-5555-5555-5555-555555555551', 'Employment', 1.0),
('22222222-2222-2222-2222-222222222221', '33333333-3333-3333-3333-333333333331', 'Legal_Counsel', 1.0),
('22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333331', 'Legal_Counsel', 1.0),
('22222222-2222-2222-2222-222222222222', '55555555-5555-5555-5555-555555555551', 'Legal_Counsel', 1.0),
('77777777-7777-7777-7777-777777777771', '44444444-4444-4444-4444-444444444441', 'Family', 1.0)
ON CONFLICT DO NOTHING;" "Insert relationships"

echo "✅ Relationships inserted successfully"
echo

# Step 7: Verify data
echo "🔍 Step 7: Verifying data..."

echo "   📊 Checking table counts..."
aws rds-data execute-statement \
    --resource-arn "$CLUSTER_ARN" \
    --secret-arn "$SECRET_ARN" \
    --database "$DATABASE" \
    --sql "SELECT 'nodes' as table_name, COUNT(*) as count FROM nodes UNION ALL SELECT 'relationships', COUNT(*) FROM relationships UNION ALL SELECT 'attributes', COUNT(*) FROM attributes;" \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query 'records[*][*].{Table:stringValue,Count:longValue}' \
    --output table

echo "   🧪 Testing conflict detection query..."
aws rds-data execute-statement \
    --resource-arn "$CLUSTER_ARN" \
    --secret-arn "$SECRET_ARN" \
    --database "$DATABASE" \
    --sql "SELECT p.primary_name as attorney_name, array_agg(DISTINCT c.primary_name) as companies FROM nodes p JOIN relationships r ON p.node_id = r.source_node_id JOIN nodes c ON r.target_node_id = c.node_id WHERE p.node_type = 'Person' AND c.node_type = 'Company' AND r.relationship_type = 'Legal_Counsel' GROUP BY p.node_id, p.primary_name HAVING COUNT(DISTINCT c.node_id) > 1;" \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query 'records[*][*].stringValue' \
    --output table

echo "✅ Data verification completed"
echo

echo "🎉 Database population completed successfully!"
echo
echo "🎯 WHAT'S BEEN CREATED:"
echo "├── 8 entities (people & companies)"
echo "├── 9 attributes (aliases & titles)" 
echo "├── 8 relationships (employment & legal counsel)"
echo "├── 1 MAJOR CONFLICT: Mary Johnson represents both ACME and TechCorp!"
echo "└── 1 family conflict: Amanda Brown vs Robert Brown's company"
echo
echo "🔧 NEXT STEPS:"
echo "1. Connect to database and explore tables"
echo "2. Test additional conflict scenarios"
echo "3. Implement Cloudflare Worker integration"
echo "4. Add more sophisticated conflict detection functions"
echo