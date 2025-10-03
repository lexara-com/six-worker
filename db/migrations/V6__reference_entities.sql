-- =============================================
-- Reference Entity System to Prevent Provenance Bloat
-- Version: 6.0
-- Purpose: Classify entities and optimize provenance for well-established facts
-- =============================================

-- Add entity classification column
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS entity_class VARCHAR(20) DEFAULT 'fact_based';

-- Add constraint for valid entity classes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.check_constraints 
        WHERE constraint_name = 'nodes_entity_class_check'
    ) THEN
        ALTER TABLE nodes ADD CONSTRAINT nodes_entity_class_check 
        CHECK (entity_class IN ('fact_based', 'reference', 'computed'));
    END IF;
END
$$;

-- Add index for entity class queries
CREATE INDEX IF NOT EXISTS idx_nodes_entity_class ON nodes(entity_class);
CREATE INDEX IF NOT EXISTS idx_nodes_type_class ON nodes(node_type, entity_class);

-- Create reference entities table for pre-defined common entities
CREATE TABLE IF NOT EXISTS reference_entities (
    reference_id VARCHAR(26) PRIMARY KEY DEFAULT generate_ulid(),
    node_type VARCHAR(50) NOT NULL,
    primary_name VARCHAR(255) NOT NULL,
    normalized_name VARCHAR(255) NOT NULL,
    authority_source VARCHAR(100) NOT NULL,  -- WHO says this is authoritative
    authority_confidence DECIMAL(3,2) DEFAULT 1.0,
    global_identifier VARCHAR(100),  -- ISO codes, FIPS codes, etc.
    aliases JSONB DEFAULT '[]'::JSONB,
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(node_type, normalized_name)
);

-- Pre-populate common reference entities
INSERT INTO reference_entities (node_type, primary_name, normalized_name, authority_source, authority_confidence, global_identifier, aliases, metadata) 
VALUES 
-- US States
('Place', 'State of Alabama', 'state of alabama', 'US Government - Official', 1.0, 'US-AL', '["Alabama", "AL"]'::jsonb, '{"type": "US State", "fips": "01"}'::jsonb),
('Place', 'State of Alaska', 'state of alaska', 'US Government - Official', 1.0, 'US-AK', '["Alaska", "AK"]'::jsonb, '{"type": "US State", "fips": "02"}'::jsonb),
('Place', 'State of Arizona', 'state of arizona', 'US Government - Official', 1.0, 'US-AZ', '["Arizona", "AZ"]'::jsonb, '{"type": "US State", "fips": "04"}'::jsonb),
('Place', 'State of Arkansas', 'state of arkansas', 'US Government - Official', 1.0, 'US-AR', '["Arkansas", "AR"]'::jsonb, '{"type": "US State", "fips": "05"}'::jsonb),
('Place', 'State of California', 'state of california', 'US Government - Official', 1.0, 'US-CA', '["California", "CA"]'::jsonb, '{"type": "US State", "fips": "06"}'::jsonb),
('Place', 'State of Colorado', 'state of colorado', 'US Government - Official', 1.0, 'US-CO', '["Colorado", "CO"]'::jsonb, '{"type": "US State", "fips": "08"}'::jsonb),
('Place', 'State of Connecticut', 'state of connecticut', 'US Government - Official', 1.0, 'US-CT', '["Connecticut", "CT"]'::jsonb, '{"type": "US State", "fips": "09"}'::jsonb),
('Place', 'State of Delaware', 'state of delaware', 'US Government - Official', 1.0, 'US-DE', '["Delaware", "DE"]'::jsonb, '{"type": "US State", "fips": "10"}'::jsonb),
('Place', 'State of Florida', 'state of florida', 'US Government - Official', 1.0, 'US-FL', '["Florida", "FL"]'::jsonb, '{"type": "US State", "fips": "12"}'::jsonb),
('Place', 'State of Georgia', 'state of georgia', 'US Government - Official', 1.0, 'US-GA', '["Georgia", "GA"]'::jsonb, '{"type": "US State", "fips": "13"}'::jsonb),
('Place', 'State of Hawaii', 'state of hawaii', 'US Government - Official', 1.0, 'US-HI', '["Hawaii", "HI"]'::jsonb, '{"type": "US State", "fips": "15"}'::jsonb),
('Place', 'State of Idaho', 'state of idaho', 'US Government - Official', 1.0, 'US-ID', '["Idaho", "ID"]'::jsonb, '{"type": "US State", "fips": "16"}'::jsonb),
('Place', 'State of Illinois', 'state of illinois', 'US Government - Official', 1.0, 'US-IL', '["Illinois", "IL"]'::jsonb, '{"type": "US State", "fips": "17"}'::jsonb),
('Place', 'State of Indiana', 'state of indiana', 'US Government - Official', 1.0, 'US-IN', '["Indiana", "IN"]'::jsonb, '{"type": "US State", "fips": "18"}'::jsonb),
('Place', 'State of Iowa', 'state of iowa', 'US Government - Official', 1.0, 'US-IA', '["Iowa", "IA"]'::jsonb, '{"type": "US State", "fips": "19"}'::jsonb),
('Place', 'State of Kansas', 'state of kansas', 'US Government - Official', 1.0, 'US-KS', '["Kansas", "KS"]'::jsonb, '{"type": "US State", "fips": "20"}'::jsonb),
('Place', 'State of Kentucky', 'state of kentucky', 'US Government - Official', 1.0, 'US-KY', '["Kentucky", "KY"]'::jsonb, '{"type": "US State", "fips": "21"}'::jsonb),
('Place', 'State of Louisiana', 'state of louisiana', 'US Government - Official', 1.0, 'US-LA', '["Louisiana", "LA"]'::jsonb, '{"type": "US State", "fips": "22"}'::jsonb),
('Place', 'State of Maine', 'state of maine', 'US Government - Official', 1.0, 'US-ME', '["Maine", "ME"]'::jsonb, '{"type": "US State", "fips": "23"}'::jsonb),
('Place', 'State of Maryland', 'state of maryland', 'US Government - Official', 1.0, 'US-MD', '["Maryland", "MD"]'::jsonb, '{"type": "US State", "fips": "24"}'::jsonb),
('Place', 'State of Massachusetts', 'state of massachusetts', 'US Government - Official', 1.0, 'US-MA', '["Massachusetts", "MA"]'::jsonb, '{"type": "US State", "fips": "25"}'::jsonb),
('Place', 'State of Michigan', 'state of michigan', 'US Government - Official', 1.0, 'US-MI', '["Michigan", "MI"]'::jsonb, '{"type": "US State", "fips": "26"}'::jsonb),
('Place', 'State of Minnesota', 'state of minnesota', 'US Government - Official', 1.0, 'US-MN', '["Minnesota", "MN"]'::jsonb, '{"type": "US State", "fips": "27"}'::jsonb),
('Place', 'State of Mississippi', 'state of mississippi', 'US Government - Official', 1.0, 'US-MS', '["Mississippi", "MS"]'::jsonb, '{"type": "US State", "fips": "28"}'::jsonb),
('Place', 'State of Missouri', 'state of missouri', 'US Government - Official', 1.0, 'US-MO', '["Missouri", "MO"]'::jsonb, '{"type": "US State", "fips": "29"}'::jsonb),
('Place', 'State of Montana', 'state of montana', 'US Government - Official', 1.0, 'US-MT', '["Montana", "MT"]'::jsonb, '{"type": "US State", "fips": "30"}'::jsonb),
('Place', 'State of Nebraska', 'state of nebraska', 'US Government - Official', 1.0, 'US-NE', '["Nebraska", "NE"]'::jsonb, '{"type": "US State", "fips": "31"}'::jsonb),
('Place', 'State of Nevada', 'state of nevada', 'US Government - Official', 1.0, 'US-NV', '["Nevada", "NV"]'::jsonb, '{"type": "US State", "fips": "32"}'::jsonb),
('Place', 'State of New Hampshire', 'state of new hampshire', 'US Government - Official', 1.0, 'US-NH', '["New Hampshire", "NH"]'::jsonb, '{"type": "US State", "fips": "33"}'::jsonb),
('Place', 'State of New Jersey', 'state of new jersey', 'US Government - Official', 1.0, 'US-NJ', '["New Jersey", "NJ"]'::jsonb, '{"type": "US State", "fips": "34"}'::jsonb),
('Place', 'State of New Mexico', 'state of new mexico', 'US Government - Official', 1.0, 'US-NM', '["New Mexico", "NM"]'::jsonb, '{"type": "US State", "fips": "35"}'::jsonb),
('Place', 'State of New York', 'state of new york', 'US Government - Official', 1.0, 'US-NY', '["New York", "NY"]'::jsonb, '{"type": "US State", "fips": "36"}'::jsonb),
('Place', 'State of North Carolina', 'state of north carolina', 'US Government - Official', 1.0, 'US-NC', '["North Carolina", "NC"]'::jsonb, '{"type": "US State", "fips": "37"}'::jsonb),
('Place', 'State of North Dakota', 'state of north dakota', 'US Government - Official', 1.0, 'US-ND', '["North Dakota", "ND"]'::jsonb, '{"type": "US State", "fips": "38"}'::jsonb),
('Place', 'State of Ohio', 'state of ohio', 'US Government - Official', 1.0, 'US-OH', '["Ohio", "OH"]'::jsonb, '{"type": "US State", "fips": "39"}'::jsonb),
('Place', 'State of Oklahoma', 'state of oklahoma', 'US Government - Official', 1.0, 'US-OK', '["Oklahoma", "OK"]'::jsonb, '{"type": "US State", "fips": "40"}'::jsonb),
('Place', 'State of Oregon', 'state of oregon', 'US Government - Official', 1.0, 'US-OR', '["Oregon", "OR"]'::jsonb, '{"type": "US State", "fips": "41"}'::jsonb),
('Place', 'State of Pennsylvania', 'state of pennsylvania', 'US Government - Official', 1.0, 'US-PA', '["Pennsylvania", "PA"]'::jsonb, '{"type": "US State", "fips": "42"}'::jsonb),
('Place', 'State of Rhode Island', 'state of rhode island', 'US Government - Official', 1.0, 'US-RI', '["Rhode Island", "RI"]'::jsonb, '{"type": "US State", "fips": "44"}'::jsonb),
('Place', 'State of South Carolina', 'state of south carolina', 'US Government - Official', 1.0, 'US-SC', '["South Carolina", "SC"]'::jsonb, '{"type": "US State", "fips": "45"}'::jsonb),
('Place', 'State of South Dakota', 'state of south dakota', 'US Government - Official', 1.0, 'US-SD', '["South Dakota", "SD"]'::jsonb, '{"type": "US State", "fips": "46"}'::jsonb),
('Place', 'State of Tennessee', 'state of tennessee', 'US Government - Official', 1.0, 'US-TN', '["Tennessee", "TN"]'::jsonb, '{"type": "US State", "fips": "47"}'::jsonb),
('Place', 'State of Texas', 'state of texas', 'US Government - Official', 1.0, 'US-TX', '["Texas", "TX"]'::jsonb, '{"type": "US State", "fips": "48"}'::jsonb),
('Place', 'State of Utah', 'state of utah', 'US Government - Official', 1.0, 'US-UT', '["Utah", "UT"]'::jsonb, '{"type": "US State", "fips": "49"}'::jsonb),
('Place', 'State of Vermont', 'state of vermont', 'US Government - Official', 1.0, 'US-VT', '["Vermont", "VT"]'::jsonb, '{"type": "US State", "fips": "50"}'::jsonb),
('Place', 'State of Virginia', 'state of virginia', 'US Government - Official', 1.0, 'US-VA', '["Virginia", "VA"]'::jsonb, '{"type": "US State", "fips": "51"}'::jsonb),
('Place', 'State of Washington', 'state of washington', 'US Government - Official', 1.0, 'US-WA', '["Washington", "WA"]'::jsonb, '{"type": "US State", "fips": "53"}'::jsonb),
('Place', 'State of West Virginia', 'state of west virginia', 'US Government - Official', 1.0, 'US-WV', '["West Virginia", "WV"]'::jsonb, '{"type": "US State", "fips": "54"}'::jsonb),
('Place', 'State of Wisconsin', 'state of wisconsin', 'US Government - Official', 1.0, 'US-WI', '["Wisconsin", "WI"]'::jsonb, '{"type": "US State", "fips": "55"}'::jsonb),
('Place', 'State of Wyoming', 'state of wyoming', 'US Government - Official', 1.0, 'US-WY', '["Wyoming", "WY"]'::jsonb, '{"type": "US State", "fips": "56"}'::jsonb),

-- Federal Districts
('Place', 'District of Columbia', 'district of columbia', 'US Government - Official', 1.0, 'US-DC', '["Washington D.C.", "DC", "Washington DC"]'::jsonb, '{"type": "Federal District", "fips": "11"}'::jsonb),

-- Major Countries
('Place', 'United States', 'united states', 'ISO 3166-1', 1.0, 'US', '["USA", "United States of America", "US"]'::jsonb, '{"type": "Country", "iso2": "US", "iso3": "USA"}'::jsonb),
('Place', 'Canada', 'canada', 'ISO 3166-1', 1.0, 'CA', '["CAN"]'::jsonb, '{"type": "Country", "iso2": "CA", "iso3": "CAN"}'::jsonb),
('Place', 'Mexico', 'mexico', 'ISO 3166-1', 1.0, 'MX', '["MEX", "United Mexican States"]'::jsonb, '{"type": "Country", "iso2": "MX", "iso3": "MEX"}'::jsonb),
('Place', 'United Kingdom', 'united kingdom', 'ISO 3166-1', 1.0, 'GB', '["UK", "Britain", "Great Britain", "GBR"]'::jsonb, '{"type": "Country", "iso2": "GB", "iso3": "GBR"}'::jsonb)

ON CONFLICT (node_type, normalized_name) DO NOTHING;

-- Function to create or match reference entities
CREATE OR REPLACE FUNCTION get_or_create_reference_entity(
    p_node_type VARCHAR(50),
    p_primary_name VARCHAR(255),
    p_source_name VARCHAR(255) DEFAULT 'system',
    p_source_type VARCHAR(50) DEFAULT 'reference_entities'
) RETURNS VARCHAR(26) AS $$
DECLARE
    entity_id VARCHAR(26);
    normalized_name_val VARCHAR(255);
    ref_entity RECORD;
BEGIN
    normalized_name_val := normalize_name(p_primary_name);
    
    -- First, check if this matches a pre-defined reference entity
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
                'reference_system', 'active'
            );
            
            -- Create single authoritative provenance record
            PERFORM create_provenance_record(
                'node', entity_id, ref_entity.authority_source, 'government_authority',
                'reference_system', ref_entity.authority_confidence, 'high'
            );
            
            -- Add aliases as attributes if they exist
            IF ref_entity.aliases IS NOT NULL AND jsonb_array_length(ref_entity.aliases) > 0 THEN
                INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
                SELECT 
                    generate_ulid(), entity_id, 'nameAlias', 
                    alias_value->>'', 'reference_system', 'active'
                FROM jsonb_array_elements_text(ref_entity.aliases) AS alias_value;
            END IF;
        END IF;
        
        RETURN entity_id;
    ELSE
        -- Not a reference entity, return NULL to indicate normal processing needed
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Classify existing entities that should be reference entities
UPDATE nodes 
SET entity_class = 'reference' 
WHERE entity_class = 'fact_based'
  AND node_type = 'Place'
  AND EXISTS (
      SELECT 1 FROM reference_entities re 
      WHERE re.node_type = nodes.node_type 
        AND re.normalized_name = nodes.normalized_name
  );

-- Show results
\echo '=== Reference Entity System Deployment Results ==='

\echo '1. Reference entities catalog:'
SELECT 
    node_type, 
    COUNT(*) as reference_count,
    string_agg(DISTINCT metadata->>'type', ', ') as types
FROM reference_entities 
GROUP BY node_type
ORDER BY reference_count DESC;

\echo '2. Entity classification summary:'
SELECT 
    entity_class,
    node_type,
    COUNT(*) as entity_count
FROM nodes 
GROUP BY entity_class, node_type
ORDER BY entity_class, node_type;

\echo '3. Existing entities reclassified as reference:'
SELECT primary_name, node_type, created_at::date
FROM nodes 
WHERE entity_class = 'reference'
  AND created_by != 'reference_system'
ORDER BY created_at;

\echo '4. Provenance impact analysis (State of Iowa example):'
SELECT 
    'Before reference system' as scenario,
    COUNT(*) as provenance_records
FROM provenance p
JOIN nodes n ON p.asset_id = n.node_id
WHERE n.normalized_name = 'state of iowa' AND p.asset_type = 'node';

-- Add government_authority source type if it doesn't exist
INSERT INTO source_types (source_type, description, default_reliability, requires_license)
VALUES ('government_authority', 'Official government authoritative sources', 'high', FALSE)
ON CONFLICT (source_type) DO NOTHING;