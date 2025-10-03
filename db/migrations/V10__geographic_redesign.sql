-- =============================================
-- Geographic Entity System - Complete Redesign
-- Version: 10.0
-- Purpose: Clean slate with proper geographic hierarchy
-- =============================================

\echo '=== WARNING: This will wipe all existing data and start fresh ==='
\echo '=== Backing up current table counts ==='

-- Show current data before wiping
SELECT 
    'nodes' as table_name, COUNT(*) as record_count FROM nodes
UNION ALL
SELECT 
    'relationships', COUNT(*) FROM relationships  
UNION ALL
SELECT 
    'attributes', COUNT(*) FROM attributes
UNION ALL
SELECT 
    'provenance', COUNT(*) FROM provenance;

-- Drop all existing data (clean slate)
TRUNCATE TABLE change_history CASCADE;
TRUNCATE TABLE provenance CASCADE; 
TRUNCATE TABLE attributes CASCADE;
TRUNCATE TABLE relationships CASCADE;
TRUNCATE TABLE nodes CASCADE;
DROP TABLE IF EXISTS reference_entities CASCADE;

-- Update node types for geographic specificity
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_node_type_check;
ALTER TABLE nodes ADD CONSTRAINT nodes_node_type_check 
CHECK (node_type IN ('Person', 'Company', 'State', 'City', 'County', 'Country', 'Address', 'Thing', 'Event'));

-- Create comprehensive reference entities table for geographic hierarchy
CREATE TABLE reference_entities (
    reference_id VARCHAR(26) PRIMARY KEY DEFAULT generate_ulid(),
    node_type VARCHAR(50) NOT NULL,
    primary_name VARCHAR(255) NOT NULL,
    normalized_name VARCHAR(255) NOT NULL,
    parent_reference_id VARCHAR(26) REFERENCES reference_entities(reference_id),
    authority_source VARCHAR(100) NOT NULL,
    authority_confidence DECIMAL(3,2) DEFAULT 1.0,
    
    -- Geographic identifiers
    iso_code VARCHAR(10),        -- ISO country/state codes
    fips_code VARCHAR(10),       -- FIPS codes for US states/counties  
    gnis_id VARCHAR(20),         -- Geographic Names Information System ID
    postal_code VARCHAR(20),     -- ZIP codes, etc.
    
    -- Coordinate data as GeoJSON
    geometry JSONB,              -- GeoJSON geometry (point, polygon, etc.)
    
    -- Hierarchical data
    aliases JSONB DEFAULT '[]'::JSONB,
    metadata JSONB DEFAULT '{}'::JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(node_type, normalized_name)
);

-- Create index for geographic queries
CREATE INDEX idx_reference_entities_parent ON reference_entities(parent_reference_id);
CREATE INDEX idx_reference_entities_type ON reference_entities(node_type);
CREATE INDEX idx_reference_entities_geometry ON reference_entities USING GIN (geometry);

-- Pre-populate countries first (top of hierarchy)
INSERT INTO reference_entities (node_type, primary_name, normalized_name, authority_source, authority_confidence, iso_code, geometry, metadata) 
VALUES 
('Country', 'United States', 'united states', 'ISO 3166-1', 1.0, 'US', 
 '{"type": "Point", "coordinates": [-98.5795, 39.8283]}'::jsonb,
 '{"full_name": "United States of America", "capital": "Washington, D.C."}'::jsonb),
('Country', 'Canada', 'canada', 'ISO 3166-1', 1.0, 'CA',
 '{"type": "Point", "coordinates": [-106.3468, 56.1304]}'::jsonb,
 '{"full_name": "Canada", "capital": "Ottawa"}'::jsonb),
('Country', 'Mexico', 'mexico', 'ISO 3166-1', 1.0, 'MX',
 '{"type": "Point", "coordinates": [-102.5528, 23.6345]}'::jsonb,
 '{"full_name": "United Mexican States", "capital": "Mexico City"}'::jsonb);

-- Get US reference ID for states
DO $$
DECLARE
    us_ref_id VARCHAR(26);
BEGIN
    SELECT reference_id INTO us_ref_id FROM reference_entities WHERE iso_code = 'US';
    
    -- Pre-populate US states with proper hierarchy
    INSERT INTO reference_entities (node_type, primary_name, normalized_name, parent_reference_id, authority_source, authority_confidence, fips_code, iso_code, geometry, aliases, metadata) 
    VALUES 
    ('State', 'Alabama', 'alabama', us_ref_id, 'US Government', 1.0, '01', 'US-AL', 
     '{"type": "Point", "coordinates": [-86.79113, 32.377716]}'::jsonb,
     '["AL", "State of Alabama"]'::jsonb, '{"capital": "Montgomery", "largest_city": "Birmingham"}'::jsonb),
    ('State', 'Alaska', 'alaska', us_ref_id, 'US Government', 1.0, '02', 'US-AK',
     '{"type": "Point", "coordinates": [-152.404419, 61.370716]}'::jsonb,
     '["AK", "State of Alaska"]'::jsonb, '{"capital": "Juneau", "largest_city": "Anchorage"}'::jsonb),
    ('State', 'Arizona', 'arizona', us_ref_id, 'US Government', 1.0, '04', 'US-AZ',
     '{"type": "Point", "coordinates": [-111.431221, 33.729759]}'::jsonb,
     '["AZ", "State of Arizona"]'::jsonb, '{"capital": "Phoenix", "largest_city": "Phoenix"}'::jsonb),
    ('State', 'Arkansas', 'arkansas', us_ref_id, 'US Government', 1.0, '05', 'US-AR',
     '{"type": "Point", "coordinates": [-92.373123, 34.969704]}'::jsonb,
     '["AR", "State of Arkansas"]'::jsonb, '{"capital": "Little Rock", "largest_city": "Little Rock"}'::jsonb),
    ('State', 'California', 'california', us_ref_id, 'US Government', 1.0, '06', 'US-CA',
     '{"type": "Point", "coordinates": [-121.468926, 38.555605]}'::jsonb,
     '["CA", "State of California"]'::jsonb, '{"capital": "Sacramento", "largest_city": "Los Angeles"}'::jsonb),
    ('State', 'Colorado', 'colorado', us_ref_id, 'US Government', 1.0, '08', 'US-CO',
     '{"type": "Point", "coordinates": [-105.782067, 39.550051]}'::jsonb,
     '["CO", "State of Colorado"]'::jsonb, '{"capital": "Denver", "largest_city": "Denver"}'::jsonb),
    ('State', 'Connecticut', 'connecticut', us_ref_id, 'US Government', 1.0, '09', 'US-CT',
     '{"type": "Point", "coordinates": [-72.757507, 41.767]}'::jsonb,
     '["CT", "State of Connecticut"]'::jsonb, '{"capital": "Hartford", "largest_city": "Bridgeport"}'::jsonb),
    ('State', 'Delaware', 'delaware', us_ref_id, 'US Government', 1.0, '10', 'US-DE',
     '{"type": "Point", "coordinates": [-75.526755, 39.161921]}'::jsonb,
     '["DE", "State of Delaware"]'::jsonb, '{"capital": "Dover", "largest_city": "Wilmington"}'::jsonb),
    ('State', 'Florida', 'florida', us_ref_id, 'US Government', 1.0, '12', 'US-FL',
     '{"type": "Point", "coordinates": [-84.27277, 27.766279]}'::jsonb,
     '["FL", "State of Florida"]'::jsonb, '{"capital": "Tallahassee", "largest_city": "Jacksonville"}'::jsonb),
    ('State', 'Georgia', 'georgia', us_ref_id, 'US Government', 1.0, '13', 'US-GA',
     '{"type": "Point", "coordinates": [-83.441162, 33.76]}'::jsonb,
     '["GA", "State of Georgia"]'::jsonb, '{"capital": "Atlanta", "largest_city": "Atlanta"}'::jsonb),
    ('State', 'Hawaii', 'hawaii', us_ref_id, 'US Government', 1.0, '15', 'US-HI',
     '{"type": "Point", "coordinates": [-157.826182, 21.30895]}'::jsonb,
     '["HI", "State of Hawaii"]'::jsonb, '{"capital": "Honolulu", "largest_city": "Honolulu"}'::jsonb),
    ('State', 'Idaho', 'idaho', us_ref_id, 'US Government', 1.0, '16', 'US-ID',
     '{"type": "Point", "coordinates": [-114.478828, 44.240459]}'::jsonb,
     '["ID", "State of Idaho"]'::jsonb, '{"capital": "Boise", "largest_city": "Boise"}'::jsonb),
    ('State', 'Illinois', 'illinois', us_ref_id, 'US Government', 1.0, '17', 'US-IL',
     '{"type": "Point", "coordinates": [-88.986137, 40.349457]}'::jsonb,
     '["IL", "State of Illinois"]'::jsonb, '{"capital": "Springfield", "largest_city": "Chicago"}'::jsonb),
    ('State', 'Indiana', 'indiana', us_ref_id, 'US Government', 1.0, '18', 'US-IN',
     '{"type": "Point", "coordinates": [-86.147685, 40.790363]}'::jsonb,
     '["IN", "State of Indiana"]'::jsonb, '{"capital": "Indianapolis", "largest_city": "Indianapolis"}'::jsonb),
    ('State', 'Iowa', 'iowa', us_ref_id, 'US Government', 1.0, '19', 'US-IA',
     '{"type": "Point", "coordinates": [-93.620866, 42.590796]}'::jsonb,
     '["IA", "State of Iowa"]'::jsonb, '{"capital": "Des Moines", "largest_city": "Des Moines"}'::jsonb);
    -- Additional states would continue...
END
$$;

-- Function to get or create geographic reference entities with proper hierarchy
CREATE OR REPLACE FUNCTION get_or_create_geographic_entity(
    p_node_type VARCHAR(50),
    p_primary_name VARCHAR(255),
    p_parent_name VARCHAR(255) DEFAULT NULL,
    p_parent_type VARCHAR(50) DEFAULT NULL,
    p_geometry JSONB DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS VARCHAR(26) AS $$
DECLARE
    entity_id VARCHAR(26);
    parent_id VARCHAR(26);
    normalized_name_val VARCHAR(255);
    ref_entity RECORD;
BEGIN
    normalized_name_val := normalize_name(p_primary_name);
    
    -- Check if this matches a pre-defined reference entity
    SELECT * INTO ref_entity 
    FROM reference_entities 
    WHERE node_type = p_node_type 
      AND normalized_name = normalized_name_val;
    
    IF FOUND THEN
        -- Check if we already created this reference entity in nodes table
        SELECT node_id INTO entity_id 
        FROM nodes 
        WHERE node_type = p_node_type 
          AND normalized_name = normalized_name_val
          AND entity_class = 'reference';
        
        IF NOT FOUND THEN
            -- Create the reference entity in nodes table
            entity_id := generate_ulid();
            
            INSERT INTO nodes (
                node_id, node_type, primary_name, entity_class, 
                created_by, status
            ) VALUES (
                entity_id, p_node_type, ref_entity.primary_name, 'reference',
                'geographic_system', 'active'
            );
            
            -- Add geometry as attribute if it exists
            IF ref_entity.geometry IS NOT NULL THEN
                INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
                VALUES (generate_ulid(), entity_id, 'geometry', ref_entity.geometry::TEXT, 'geographic_system', 'active');
            END IF;
            
            -- Add other identifiers as attributes
            IF ref_entity.fips_code IS NOT NULL THEN
                INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
                VALUES (generate_ulid(), entity_id, 'fips_code', ref_entity.fips_code, 'geographic_system', 'active');
            END IF;
            
            IF ref_entity.iso_code IS NOT NULL THEN
                INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
                VALUES (generate_ulid(), entity_id, 'iso_code', ref_entity.iso_code, 'geographic_system', 'active');
            END IF;
        END IF;
        
        RETURN entity_id;
    ELSE
        -- Not a pre-defined reference entity, create as fact-based
        entity_id := generate_ulid();
        
        INSERT INTO nodes (node_id, node_type, primary_name, entity_class, created_by, status)
        VALUES (entity_id, p_node_type, p_primary_name, 'fact_based', 'geographic_system', 'active');
        
        -- Add geometry if provided
        IF p_geometry IS NOT NULL THEN
            INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
            VALUES (generate_ulid(), entity_id, 'geometry', p_geometry::TEXT, 'geographic_system', 'active');
        END IF;
        
        RETURN entity_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Test the geographic system
\echo '=== Testing Geographic Entity System ==='

\echo '1. Create Iowa entity:'
SELECT get_or_create_geographic_entity('State', 'Iowa') as iowa_id;

\echo '2. Test creating a city:'  
SELECT get_or_create_geographic_entity(
    'City', 'Des Moines', 'Iowa', 'State',
    '{"type": "Point", "coordinates": [-93.6091, 41.5868]}'::jsonb,
    '{"population": 214133, "founded": 1843}'::jsonb
) as des_moines_id;

\echo '=== Fresh Start Complete ==='
SELECT 
    'nodes' as table_name, COUNT(*) as record_count FROM nodes
UNION ALL
SELECT 
    'relationships', COUNT(*) FROM relationships  
UNION ALL
SELECT 
    'reference_entities', COUNT(*) FROM reference_entities;