#!/usr/bin/env python3
"""
Aurora PostgreSQL Database Population Script
Uses AWS RDS Data API to populate tables with test data
"""

import boto3
import json
import time
import sys
from typing import Dict, List, Any

class DatabasePopulator:
    def __init__(self, profile_name: str = 'lexara_super_agent', region: str = 'us-east-1'):
        """Initialize the database populator with AWS credentials"""
        self.session = boto3.Session(profile_name=profile_name)
        self.rds_client = self.session.client('rds-data', region_name=region)
        self.cluster_arn = f"arn:aws:rds:us-east-1:492149691043:cluster:dev-six-worker-cluster"
        self.secret_arn = "arn:aws:secretsmanager:us-east-1:492149691043:secret:dev/six-worker/database-fmJYO8"
        self.database_name = "graph_db"
        
    def execute_sql(self, sql: str, parameters: List[Dict] = None) -> Dict[str, Any]:
        """Execute SQL statement using RDS Data API"""
        try:
            params = {
                'resourceArn': self.cluster_arn,
                'secretArn': self.secret_arn,
                'database': self.database_name,
                'sql': sql
            }
            
            if parameters:
                params['parameters'] = parameters
                
            response = self.rds_client.execute_statement(**params)
            return response
            
        except Exception as e:
            print(f"❌ Error executing SQL: {e}")
            print(f"SQL: {sql[:200]}...")
            return None

    def execute_sql_file(self, file_path: str) -> bool:
        """Execute SQL commands from a file"""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            # Split into individual statements (basic splitting)
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            print(f"📄 Executing {len(statements)} statements from {file_path}")
            
            for i, statement in enumerate(statements, 1):
                if statement.upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                    continue  # Skip query statements in migration files
                    
                print(f"   [{i}/{len(statements)}] Executing: {statement[:60]}...")
                result = self.execute_sql(statement)
                
                if result is None:
                    print(f"❌ Failed at statement {i}")
                    return False
                    
                time.sleep(0.1)  # Small delay between statements
                
            print(f"✅ Successfully executed all statements from {file_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")
            return False

    def create_schema(self) -> bool:
        """Create the initial database schema"""
        print("🏗️  Creating database schema...")
        
        # Core schema SQL (extracted from V1__initial_schema.sql)
        schema_sql = """
        -- Enable extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        -- Create nodes table
        CREATE TABLE IF NOT EXISTS nodes (
            node_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            node_type VARCHAR(50) NOT NULL CHECK (node_type IN ('Person', 'Company', 'Place', 'Thing', 'Event')),
            primary_name VARCHAR(255) NOT NULL,
            normalized_name VARCHAR(255) NOT NULL,
            status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100),
            CONSTRAINT nodes_name_not_empty CHECK (LENGTH(TRIM(primary_name)) > 0)
        );
        """
        
        result = self.execute_sql(schema_sql)
        if result is None:
            return False
            
        # Create relationships table
        relationships_sql = """
        CREATE TABLE IF NOT EXISTS relationships (
            relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            source_node_id UUID NOT NULL,
            target_node_id UUID NOT NULL,
            relationship_type VARCHAR(50) NOT NULL,
            strength DECIMAL(3,2) DEFAULT 1.0 CHECK (strength BETWEEN 0.0 AND 1.0),
            status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
            valid_from DATE DEFAULT CURRENT_DATE,
            valid_to DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100),
            metadata JSONB,
            CONSTRAINT fk_relationships_source FOREIGN KEY (source_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
            CONSTRAINT fk_relationships_target FOREIGN KEY (target_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
            CONSTRAINT relationships_no_self_reference CHECK (source_node_id != target_node_id),
            CONSTRAINT relationships_valid_period CHECK (valid_to IS NULL OR valid_to >= valid_from)
        );
        """
        
        result = self.execute_sql(relationships_sql)
        if result is None:
            return False
            
        # Create attributes table
        attributes_sql = """
        CREATE TABLE IF NOT EXISTS attributes (
            attribute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            node_id UUID NOT NULL,
            attribute_type VARCHAR(50) NOT NULL,
            attribute_value VARCHAR(500) NOT NULL,
            normalized_value VARCHAR(500) NOT NULL,
            confidence DECIMAL(3,2) DEFAULT 1.0 CHECK (confidence BETWEEN 0.0 AND 1.0),
            source VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100),
            CONSTRAINT fk_attributes_node FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
            CONSTRAINT attributes_value_not_empty CHECK (LENGTH(TRIM(attribute_value)) > 0)
        );
        """
        
        result = self.execute_sql(attributes_sql)
        if result is None:
            return False
            
        print("✅ Core schema created successfully")
        return True

    def create_indexes(self) -> bool:
        """Create performance indexes"""
        print("📊 Creating performance indexes...")
        
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_nodes_type_status ON nodes(node_type, status);",
            "CREATE INDEX IF NOT EXISTS idx_nodes_normalized_name ON nodes(normalized_name);",
            "CREATE INDEX IF NOT EXISTS idx_relationships_source_type ON relationships(source_node_id, relationship_type, status);",
            "CREATE INDEX IF NOT EXISTS idx_relationships_target_type ON relationships(target_node_id, relationship_type, status);",
            "CREATE INDEX IF NOT EXISTS idx_attributes_node_type ON attributes(node_id, attribute_type, status);",
            "CREATE INDEX IF NOT EXISTS idx_attributes_type_value ON attributes(attribute_type, normalized_value, status);"
        ]
        
        for sql in indexes_sql:
            result = self.execute_sql(sql)
            if result is None:
                return False
                
        print("✅ Indexes created successfully")
        return True

    def create_helper_functions(self) -> bool:
        """Create helper functions"""
        print("🔧 Creating helper functions...")
        
        normalize_function = """
        CREATE OR REPLACE FUNCTION normalize_name(input_name TEXT) 
        RETURNS TEXT AS $$
        BEGIN
            RETURN LOWER(TRIM(REGEXP_REPLACE(input_name, '\\s+', ' ', 'g')));
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
        """
        
        result = self.execute_sql(normalize_function)
        if result is None:
            return False
            
        # Create trigger function
        trigger_function = """
        CREATE OR REPLACE FUNCTION update_normalized_name() 
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.normalized_name = normalize_name(NEW.primary_name);
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        result = self.execute_sql(trigger_function)
        if result is None:
            return False
            
        # Create trigger
        trigger_sql = """
        DROP TRIGGER IF EXISTS trg_nodes_normalize_name ON nodes;
        CREATE TRIGGER trg_nodes_normalize_name
            BEFORE INSERT OR UPDATE ON nodes
            FOR EACH ROW 
            EXECUTE FUNCTION update_normalized_name();
        """
        
        result = self.execute_sql(trigger_sql)
        if result is None:
            return False
            
        print("✅ Helper functions created successfully")
        return True

    def insert_test_entities(self) -> bool:
        """Insert test entities (people and companies)"""
        print("👥 Inserting test entities...")
        
        # Law firm entities
        entities = [
            # Law Firm
            ("11111111-1111-1111-1111-111111111111", "Company", "Smith & Associates Law Firm"),
            
            # Partners and Associates
            ("22222222-2222-2222-2222-222222222221", "Person", "John Smith"),
            ("22222222-2222-2222-2222-222222222222", "Person", "Mary Johnson"),
            ("22222222-2222-2222-2222-222222222223", "Person", "David Wilson"),
            ("22222222-2222-2222-2222-222222222224", "Person", "Sarah Davis"),
            
            # ACME Corporation
            ("33333333-3333-3333-3333-333333333331", "Company", "ACME Corporation"),
            ("44444444-4444-4444-4444-444444444441", "Person", "Robert Brown"),
            ("44444444-4444-4444-4444-444444444442", "Person", "Lisa Anderson"),
            ("44444444-4444-4444-4444-444444444443", "Person", "Michael Taylor"),
            
            # TechCorp Industries
            ("55555555-5555-5555-5555-555555555551", "Company", "TechCorp Industries"),
            ("66666666-6666-6666-6666-666666666661", "Person", "Jennifer White"),
            ("66666666-6666-6666-6666-666666666662", "Person", "Thomas Green"),
            
            # Individual clients and family
            ("77777777-7777-7777-7777-777777777771", "Person", "Amanda Brown"),
            ("88888888-8888-8888-8888-888888888881", "Company", "Brown Family Trust"),
            ("99999999-9999-9999-9999-999999999991", "Event", "Brown vs TechCorp Lawsuit"),
            ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "Person", "Kevin Miller"),
        ]
        
        for node_id, node_type, primary_name in entities:
            sql = f"""
            INSERT INTO nodes (node_id, node_type, primary_name, normalized_name) 
            VALUES ('{node_id}', '{node_type}', '{primary_name}', normalize_name('{primary_name}'))
            ON CONFLICT (node_id) DO NOTHING;
            """
            
            result = self.execute_sql(sql)
            if result is None:
                return False
                
        print("✅ Test entities inserted successfully")
        return True

    def insert_test_attributes(self) -> bool:
        """Insert test attributes (aliases, titles, etc.)"""
        print("🏷️  Inserting test attributes...")
        
        attributes = [
            # John Smith aliases
            ("22222222-2222-2222-2222-222222222221", "nameAlias", "J. Smith"),
            ("22222222-2222-2222-2222-222222222221", "nameAlias", "Johnny Smith"),
            ("22222222-2222-2222-2222-222222222221", "title", "Senior Partner"),
            
            # Mary Johnson aliases
            ("22222222-2222-2222-2222-222222222222", "nameAlias", "Mary J. Johnson"),
            ("22222222-2222-2222-2222-222222222222", "nameAlias", "M. Johnson"),
            ("22222222-2222-2222-2222-222222222222", "title", "Partner"),
            
            # ACME Corp aliases
            ("33333333-3333-3333-3333-333333333331", "nameAlias", "ACME Corp"),
            ("33333333-3333-3333-3333-333333333331", "nameAlias", "ACME Inc"),
            
            # Robert Brown aliases
            ("44444444-4444-4444-4444-444444444441", "nameAlias", "Bob Brown"),
            ("44444444-4444-4444-4444-444444444441", "nameAlias", "R. Brown"),
            ("44444444-4444-4444-4444-444444444441", "title", "CEO"),
            
            # TechCorp aliases
            ("55555555-5555-5555-5555-555555555551", "nameAlias", "TechCorp Inc"),
            ("55555555-5555-5555-5555-555555555551", "nameAlias", "Tech Corp"),
            
            # Jennifer White aliases
            ("66666666-6666-6666-6666-666666666661", "nameAlias", "Jenny White"),
            ("66666666-6666-6666-6666-666666666661", "nameAlias", "J. White"),
            ("66666666-6666-6666-6666-666666666661", "title", "CEO"),
            
            # Amanda Brown aliases
            ("77777777-7777-7777-7777-777777777771", "nameAlias", "Amanda B. Brown"),
            ("77777777-7777-7777-7777-777777777771", "nameAlias", "A. Brown"),
        ]
        
        for node_id, attr_type, attr_value in attributes:
            sql = f"""
            INSERT INTO attributes (node_id, attribute_type, attribute_value, normalized_value) 
            VALUES ('{node_id}', '{attr_type}', '{attr_value}', normalize_name('{attr_value}'))
            ON CONFLICT DO NOTHING;
            """
            
            result = self.execute_sql(sql)
            if result is None:
                return False
                
        print("✅ Test attributes inserted successfully")
        return True

    def insert_test_relationships(self) -> bool:
        """Insert test relationships"""
        print("🔗 Inserting test relationships...")
        
        relationships = [
            # Employment at law firm
            ("22222222-2222-2222-2222-222222222221", "11111111-1111-1111-1111-111111111111", "Employment", 1.0),
            ("22222222-2222-2222-2222-222222222222", "11111111-1111-1111-1111-111111111111", "Employment", 1.0),
            ("22222222-2222-2222-2222-222222222223", "11111111-1111-1111-1111-111111111111", "Employment", 0.9),
            
            # Employment at ACME
            ("44444444-4444-4444-4444-444444444441", "33333333-3333-3333-3333-333333333331", "Employment", 1.0),
            ("44444444-4444-4444-4444-444444444442", "33333333-3333-3333-3333-333333333331", "Employment", 1.0),
            ("44444444-4444-4444-4444-444444444443", "33333333-3333-3333-3333-333333333331", "Employment", 0.9),
            
            # Employment at TechCorp
            ("66666666-6666-6666-6666-666666666661", "55555555-5555-5555-5555-555555555551", "Employment", 1.0),
            ("66666666-6666-6666-6666-666666666662", "55555555-5555-5555-5555-555555555551", "Employment", 1.0),
            
            # Legal counsel relationships (CONFLICT SCENARIO!)
            ("22222222-2222-2222-2222-222222222221", "33333333-3333-3333-3333-333333333331", "Legal_Counsel", 1.0),
            ("22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333331", "Legal_Counsel", 1.0),
            ("22222222-2222-2222-2222-222222222222", "55555555-5555-5555-5555-555555555551", "Legal_Counsel", 1.0),
            
            # Family relationship (FAMILY CONFLICT!)
            ("77777777-7777-7777-7777-777777777771", "44444444-4444-4444-4444-444444444441", "Family", 1.0),
            
            # Kevin Miller's job history (HISTORICAL CONFLICT!)
            ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "55555555-5555-5555-5555-555555555551", "Employment", 1.0),
        ]
        
        for source_id, target_id, rel_type, strength in relationships:
            sql = f"""
            INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) 
            VALUES ('{source_id}', '{target_id}', '{rel_type}', {strength})
            ON CONFLICT DO NOTHING;
            """
            
            result = self.execute_sql(sql)
            if result is None:
                return False
                
        print("✅ Test relationships inserted successfully")
        return True

    def verify_data(self) -> bool:
        """Verify that data was inserted correctly"""
        print("🔍 Verifying data insertion...")
        
        # Check table counts
        tables = ['nodes', 'relationships', 'attributes']
        
        for table in tables:
            result = self.execute_sql(f"SELECT COUNT(*) FROM {table};")
            if result and 'records' in result:
                count = result['records'][0][0]['longValue'] if result['records'] else 0
                print(f"   📊 {table}: {count} records")
            else:
                print(f"   ❌ Could not verify {table}")
                return False
                
        # Test a simple query
        result = self.execute_sql("""
            SELECT n.primary_name, n.node_type 
            FROM nodes n 
            WHERE n.node_type = 'Person' 
            LIMIT 3;
        """)
        
        if result and 'records' in result:
            print("   📋 Sample person records:")
            for record in result['records']:
                name = record[0]['stringValue']
                node_type = record[1]['stringValue']
                print(f"      - {name} ({node_type})")
        
        print("✅ Data verification completed successfully")
        return True

    def run_test_conflict_query(self) -> bool:
        """Run a test conflict detection query"""
        print("🧪 Running test conflict detection...")
        
        # Simple conflict query - find attorneys representing multiple companies
        conflict_query = """
        SELECT 
            p.primary_name as attorney_name,
            array_agg(DISTINCT c.primary_name) as companies_represented
        FROM nodes p
        JOIN relationships r1 ON p.node_id = r1.source_node_id
        JOIN nodes c ON r1.target_node_id = c.node_id
        WHERE p.node_type = 'Person' 
        AND c.node_type = 'Company'
        AND r1.relationship_type = 'Legal_Counsel'
        GROUP BY p.node_id, p.primary_name
        HAVING COUNT(DISTINCT c.node_id) > 1;
        """
        
        result = self.execute_sql(conflict_query)
        
        if result and 'records' in result:
            print("   🚨 CONFLICTS DETECTED:")
            for record in result['records']:
                attorney = record[0]['stringValue']
                companies_json = record[1]['stringValue']
                print(f"      - {attorney} represents: {companies_json}")
        else:
            print("   ✅ No conflicts detected in test query")
            
        return True

    def populate_database(self) -> bool:
        """Main method to populate the database with test data"""
        print("🚀 Starting database population...")
        print(f"   📍 Cluster: {self.cluster_arn}")
        print(f"   🔐 Secret: {self.secret_arn}")
        print(f"   💾 Database: {self.database_name}")
        print()
        
        steps = [
            ("Creating schema", self.create_schema),
            ("Creating indexes", self.create_indexes),
            ("Creating helper functions", self.create_helper_functions),
            ("Inserting test entities", self.insert_test_entities),
            ("Inserting test attributes", self.insert_test_attributes),
            ("Inserting test relationships", self.insert_test_relationships),
            ("Verifying data", self.verify_data),
            ("Running test conflict query", self.run_test_conflict_query),
        ]
        
        for step_name, step_func in steps:
            print(f"📍 {step_name}...")
            if not step_func():
                print(f"❌ Failed at: {step_name}")
                return False
            print()
            
        print("🎉 Database population completed successfully!")
        return True


def main():
    """Main execution function"""
    print("=" * 60)
    print("🐘 Six Worker Aurora PostgreSQL Database Populator")
    print("=" * 60)
    print()
    
    try:
        populator = DatabasePopulator()
        success = populator.populate_database()
        
        if success:
            print("✅ All operations completed successfully!")
            print()
            print("🎯 NEXT STEPS:")
            print("1. Connect to database and explore tables")
            print("2. Test conflict detection functions")
            print("3. Load additional test scenarios")
            print("4. Implement Cloudflare Worker integration")
            return 0
        else:
            print("❌ Database population failed!")
            return 1
            
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())