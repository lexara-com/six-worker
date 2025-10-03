-- =============================================
-- Complete Geographic System with Relationships
-- Version: 11.0
-- Purpose: Add remaining states, cities, and bidirectional relationships
-- =============================================

-- Complete the remaining US states
DO $$
DECLARE
    us_ref_id VARCHAR(26);
BEGIN
    SELECT reference_id INTO us_ref_id FROM reference_entities WHERE iso_code = 'US';
    
    INSERT INTO reference_entities (node_type, primary_name, normalized_name, parent_reference_id, authority_source, authority_confidence, fips_code, iso_code, geometry, aliases, metadata) 
    VALUES 
    ('State', 'Kansas', 'kansas', us_ref_id, 'US Government', 1.0, '20', 'US-KS',
     '{"type": "Point", "coordinates": [-96.726486, 38.27312]}'::jsonb,
     '["KS", "State of Kansas"]'::jsonb, '{"capital": "Topeka", "largest_city": "Wichita"}'::jsonb),
    ('State', 'Kentucky', 'kentucky', us_ref_id, 'US Government', 1.0, '21', 'US-KY',
     '{"type": "Point", "coordinates": [-84.86311, 37.669058]}'::jsonb,
     '["KY", "State of Kentucky"]'::jsonb, '{"capital": "Frankfort", "largest_city": "Louisville"}'::jsonb),
    ('State', 'Louisiana', 'louisiana', us_ref_id, 'US Government', 1.0, '22', 'US-LA',
     '{"type": "Point", "coordinates": [-91.867805, 31.161801]}'::jsonb,
     '["LA", "State of Louisiana"]'::jsonb, '{"capital": "Baton Rouge", "largest_city": "New Orleans"}'::jsonb),
    ('State', 'Maine', 'maine', us_ref_id, 'US Government', 1.0, '23', 'US-ME',
     '{"type": "Point", "coordinates": [-69.765261, 44.323535]}'::jsonb,
     '["ME", "State of Maine"]'::jsonb, '{"capital": "Augusta", "largest_city": "Portland"}'::jsonb),
    ('State', 'Maryland', 'maryland', us_ref_id, 'US Government', 1.0, '24', 'US-MD',
     '{"type": "Point", "coordinates": [-76.501157, 38.972945]}'::jsonb,
     '["MD", "State of Maryland"]'::jsonb, '{"capital": "Annapolis", "largest_city": "Baltimore"}'::jsonb),
    ('State', 'Massachusetts', 'massachusetts', us_ref_id, 'US Government', 1.0, '25', 'US-MA',
     '{"type": "Point", "coordinates": [-71.530106, 42.230171]}'::jsonb,
     '["MA", "State of Massachusetts"]'::jsonb, '{"capital": "Boston", "largest_city": "Boston"}'::jsonb),
    ('State', 'Michigan', 'michigan', us_ref_id, 'US Government', 1.0, '26', 'US-MI',
     '{"type": "Point", "coordinates": [-84.536095, 44.182205]}'::jsonb,
     '["MI", "State of Michigan"]'::jsonb, '{"capital": "Lansing", "largest_city": "Detroit"}'::jsonb),
    ('State', 'Minnesota', 'minnesota', us_ref_id, 'US Government', 1.0, '27', 'US-MN',
     '{"type": "Point", "coordinates": [-93.094636, 45.398896]}'::jsonb,
     '["MN", "State of Minnesota"]'::jsonb, '{"capital": "St. Paul", "largest_city": "Minneapolis"}'::jsonb),
    ('State', 'Mississippi', 'mississippi', us_ref_id, 'US Government', 1.0, '28', 'US-MS',
     '{"type": "Point", "coordinates": [-89.674, 32.32]}'::jsonb,
     '["MS", "State of Mississippi"]'::jsonb, '{"capital": "Jackson", "largest_city": "Jackson"}'::jsonb),
    ('State', 'Missouri', 'missouri', us_ref_id, 'US Government', 1.0, '29', 'US-MO',
     '{"type": "Point", "coordinates": [-91.831833, 38.572954]}'::jsonb,
     '["MO", "State of Missouri"]'::jsonb, '{"capital": "Jefferson City", "largest_city": "Kansas City"}'::jsonb),
    ('State', 'Montana', 'montana', us_ref_id, 'US Government', 1.0, '30', 'US-MT',
     '{"type": "Point", "coordinates": [-110.454353, 46.965260]}'::jsonb,
     '["MT", "State of Montana"]'::jsonb, '{"capital": "Helena", "largest_city": "Billings"}'::jsonb),
    ('State', 'Nebraska', 'nebraska', us_ref_id, 'US Government', 1.0, '31', 'US-NE',
     '{"type": "Point", "coordinates": [-99.901813, 41.145548]}'::jsonb,
     '["NE", "State of Nebraska"]'::jsonb, '{"capital": "Lincoln", "largest_city": "Omaha"}'::jsonb),
    ('State', 'Nevada', 'nevada', us_ref_id, 'US Government', 1.0, '32', 'US-NV',
     '{"type": "Point", "coordinates": [-117.055374, 38.313515]}'::jsonb,
     '["NV", "State of Nevada"]'::jsonb, '{"capital": "Carson City", "largest_city": "Las Vegas"}'::jsonb),
    ('State', 'New Hampshire', 'new hampshire', us_ref_id, 'US Government', 1.0, '33', 'US-NH',
     '{"type": "Point", "coordinates": [-71.563896, 43.452492]}'::jsonb,
     '["NH", "State of New Hampshire"]'::jsonb, '{"capital": "Concord", "largest_city": "Manchester"}'::jsonb),
    ('State', 'New Jersey', 'new jersey', us_ref_id, 'US Government', 1.0, '34', 'US-NJ',
     '{"type": "Point", "coordinates": [-74.756138, 40.221741]}'::jsonb,
     '["NJ", "State of New Jersey"]'::jsonb, '{"capital": "Trenton", "largest_city": "Newark"}'::jsonb),
    ('State', 'New Mexico', 'new mexico', us_ref_id, 'US Government', 1.0, '35', 'US-NM',
     '{"type": "Point", "coordinates": [-106.248482, 34.307144]}'::jsonb,
     '["NM", "State of New Mexico"]'::jsonb, '{"capital": "Santa Fe", "largest_city": "Albuquerque"}'::jsonb),
    ('State', 'New York', 'new york', us_ref_id, 'US Government', 1.0, '36', 'US-NY',
     '{"type": "Point", "coordinates": [-74.948051, 42.165726]}'::jsonb,
     '["NY", "State of New York"]'::jsonb, '{"capital": "Albany", "largest_city": "New York City"}'::jsonb),
    ('State', 'North Carolina', 'north carolina', us_ref_id, 'US Government', 1.0, '37', 'US-NC',
     '{"type": "Point", "coordinates": [-79.806419, 35.759573]}'::jsonb,
     '["NC", "State of North Carolina"]'::jsonb, '{"capital": "Raleigh", "largest_city": "Charlotte"}'::jsonb),
    ('State', 'North Dakota', 'north dakota', us_ref_id, 'US Government', 1.0, '38', 'US-ND',
     '{"type": "Point", "coordinates": [-99.784012, 47.528912]}'::jsonb,
     '["ND", "State of North Dakota"]'::jsonb, '{"capital": "Bismarck", "largest_city": "Fargo"}'::jsonb),
    ('State', 'Ohio', 'ohio', us_ref_id, 'US Government', 1.0, '39', 'US-OH',
     '{"type": "Point", "coordinates": [-82.764915, 40.269789]}'::jsonb,
     '["OH", "State of Ohio"]'::jsonb, '{"capital": "Columbus", "largest_city": "Columbus"}'::jsonb),
    ('State', 'Oklahoma', 'oklahoma', us_ref_id, 'US Government', 1.0, '40', 'US-OK',
     '{"type": "Point", "coordinates": [-96.928917, 35.565342]}'::jsonb,
     '["OK", "State of Oklahoma"]'::jsonb, '{"capital": "Oklahoma City", "largest_city": "Oklahoma City"}'::jsonb),
    ('State', 'Oregon', 'oregon', us_ref_id, 'US Government', 1.0, '41', 'US-OR',
     '{"type": "Point", "coordinates": [-122.070938, 44.931109]}'::jsonb,
     '["OR", "State of Oregon"]'::jsonb, '{"capital": "Salem", "largest_city": "Portland"}'::jsonb),
    ('State', 'Pennsylvania', 'pennsylvania', us_ref_id, 'US Government', 1.0, '42', 'US-PA',
     '{"type": "Point", "coordinates": [-77.209755, 40.269789]}'::jsonb,
     '["PA", "State of Pennsylvania"]'::jsonb, '{"capital": "Harrisburg", "largest_city": "Philadelphia"}'::jsonb),
    ('State', 'Rhode Island', 'rhode island', us_ref_id, 'US Government', 1.0, '44', 'US-RI',
     '{"type": "Point", "coordinates": [-71.51178, 41.82355]}'::jsonb,
     '["RI", "State of Rhode Island"]'::jsonb, '{"capital": "Providence", "largest_city": "Providence"}'::jsonb),
    ('State', 'South Carolina', 'south carolina', us_ref_id, 'US Government', 1.0, '45', 'US-SC',
     '{"type": "Point", "coordinates": [-80.945007, 33.856892]}'::jsonb,
     '["SC", "State of South Carolina"]'::jsonb, '{"capital": "Columbia", "largest_city": "Charleston"}'::jsonb),
    ('State', 'South Dakota', 'south dakota', us_ref_id, 'US Government', 1.0, '46', 'US-SD',
     '{"type": "Point", "coordinates": [-99.976726, 44.299782]}'::jsonb,
     '["SD", "State of South Dakota"]'::jsonb, '{"capital": "Pierre", "largest_city": "Sioux Falls"}'::jsonb),
    ('State', 'Tennessee', 'tennessee', us_ref_id, 'US Government', 1.0, '47', 'US-TN',
     '{"type": "Point", "coordinates": [-86.692345, 35.211552]}'::jsonb,
     '["TN", "State of Tennessee"]'::jsonb, '{"capital": "Nashville", "largest_city": "Memphis"}'::jsonb),
    ('State', 'Texas', 'texas', us_ref_id, 'US Government', 1.0, '48', 'US-TX',
     '{"type": "Point", "coordinates": [-97.563461, 31.054487]}'::jsonb,
     '["TX", "State of Texas"]'::jsonb, '{"capital": "Austin", "largest_city": "Houston"}'::jsonb),
    ('State', 'Utah', 'utah', us_ref_id, 'US Government', 1.0, '49', 'US-UT',
     '{"type": "Point", "coordinates": [-111.862434, 39.419220]}'::jsonb,
     '["UT", "State of Utah"]'::jsonb, '{"capital": "Salt Lake City", "largest_city": "Salt Lake City"}'::jsonb),
    ('State', 'Vermont', 'vermont', us_ref_id, 'US Government', 1.0, '50', 'US-VT',
     '{"type": "Point", "coordinates": [-72.710686, 44.0]}'::jsonb,
     '["VT", "State of Vermont"]'::jsonb, '{"capital": "Montpelier", "largest_city": "Burlington"}'::jsonb),
    ('State', 'Virginia', 'virginia', us_ref_id, 'US Government', 1.0, '51', 'US-VA',
     '{"type": "Point", "coordinates": [-78.169968, 37.769337]}'::jsonb,
     '["VA", "State of Virginia"]'::jsonb, '{"capital": "Richmond", "largest_city": "Virginia Beach"}'::jsonb),
    ('State', 'Washington', 'washington', us_ref_id, 'US Government', 1.0, '53', 'US-WA',
     '{"type": "Point", "coordinates": [-121.490494, 47.042418]}'::jsonb,
     '["WA", "State of Washington"]'::jsonb, '{"capital": "Olympia", "largest_city": "Seattle"}'::jsonb),
    ('State', 'West Virginia', 'west virginia', us_ref_id, 'US Government', 1.0, '54', 'US-WV',
     '{"type": "Point", "coordinates": [-80.954453, 38.491226]}'::jsonb,
     '["WV", "State of West Virginia"]'::jsonb, '{"capital": "Charleston", "largest_city": "Charleston"}'::jsonb),
    ('State', 'Wisconsin', 'wisconsin', us_ref_id, 'US Government', 1.0, '55', 'US-WI',
     '{"type": "Point", "coordinates": [-89.616508, 44.268543]}'::jsonb,
     '["WI", "State of Wisconsin"]'::jsonb, '{"capital": "Madison", "largest_city": "Milwaukee"}'::jsonb),
    ('State', 'Wyoming', 'wyoming', us_ref_id, 'US Government', 1.0, '56', 'US-WY',
     '{"type": "Point", "coordinates": [-107.30249, 42.755966]}'::jsonb,
     '["WY", "State of Wyoming"]'::jsonb, '{"capital": "Cheyenne", "largest_city": "Cheyenne"}'::jsonb),
    ('State', 'District of Columbia', 'district of columbia', us_ref_id, 'US Government', 1.0, '11', 'US-DC',
     '{"type": "Point", "coordinates": [-77.026817, 38.907192]}'::jsonb,
     '["DC", "Washington D.C.", "Washington DC"]'::jsonb, '{"type": "Federal District"}'::jsonb);
END
$$;

-- Pre-populate major Iowa cities (for our use case)
DO $$
DECLARE
    iowa_ref_id VARCHAR(26);
BEGIN
    SELECT reference_id INTO iowa_ref_id FROM reference_entities WHERE normalized_name = 'iowa';
    
    INSERT INTO reference_entities (node_type, primary_name, normalized_name, parent_reference_id, authority_source, authority_confidence, geometry, metadata) 
    VALUES 
    ('City', 'Des Moines', 'des moines', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-93.6091, 41.5868]}'::jsonb,
     '{"population": 214133, "county": "Polk County", "founded": 1843}'::jsonb),
    ('City', 'Cedar Rapids', 'cedar rapids', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-91.6706, 42.0080]}'::jsonb,
     '{"population": 137710, "county": "Linn County", "founded": 1849}'::jsonb),
    ('City', 'Davenport', 'davenport', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-90.5776, 41.5236]}'::jsonb,
     '{"population": 101724, "county": "Scott County", "founded": 1836}'::jsonb),
    ('City', 'Sioux City', 'sioux city', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-96.4003, 42.4999]}'::jsonb,
     '{"population": 85797, "county": "Woodbury County", "founded": 1854}'::jsonb),
    ('City', 'Iowa City', 'iowa city', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-91.5302, 41.6611]}'::jsonb,
     '{"population": 75130, "county": "Johnson County", "founded": 1839}'::jsonb),
    ('City', 'Clive', 'clive', iowa_ref_id, 'US Census Bureau', 0.98,
     '{"type": "Point", "coordinates": [-93.7749, 41.6097]}'::jsonb,
     '{"population": 18274, "county": "Polk County", "founded": 1956}'::jsonb);
END
$$;

-- Function to create bidirectional geographic relationships
CREATE OR REPLACE FUNCTION create_bidirectional_relationship(
    p_entity1_id VARCHAR(26),
    p_entity2_id VARCHAR(26), 
    p_relationship_type VARCHAR(50),
    p_reverse_relationship_type VARCHAR(50),
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50)
) RETURNS JSONB AS $$
DECLARE
    rel1_id VARCHAR(26);
    rel2_id VARCHAR(26);
    result JSONB;
BEGIN
    -- Create forward relationship
    rel1_id := generate_ulid();
    INSERT INTO relationships (
        relationship_id, source_node_id, target_node_id, relationship_type,
        strength, created_by, status
    ) VALUES (
        rel1_id, p_entity1_id, p_entity2_id, p_relationship_type,
        1.0, 'geographic_system', 'active'
    );
    
    -- Create reverse relationship
    rel2_id := generate_ulid();
    INSERT INTO relationships (
        relationship_id, source_node_id, target_node_id, relationship_type,
        strength, created_by, status
    ) VALUES (
        rel2_id, p_entity2_id, p_entity1_id, p_reverse_relationship_type,
        1.0, 'geographic_system', 'active'
    );
    
    -- Create provenance for both
    PERFORM create_provenance_record('relationship', rel1_id, p_source_name, p_source_type, 'geographic_system', 1.0);
    PERFORM create_provenance_record('relationship', rel2_id, p_source_name, p_source_type, 'geographic_system', 1.0);
    
    result := jsonb_build_object(
        'forward_id', rel1_id,
        'reverse_id', rel2_id,
        'forward_type', p_relationship_type,
        'reverse_type', p_reverse_relationship_type
    );
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Enhanced Propose API function for geographic entities
CREATE OR REPLACE FUNCTION propose_geographic_fact(
    p_entity_name VARCHAR(255),
    p_entity_type VARCHAR(50),
    p_location_name VARCHAR(255),
    p_location_type VARCHAR(50), 
    p_address VARCHAR(255) DEFAULT NULL,
    p_coordinates JSONB DEFAULT NULL,
    p_source_name VARCHAR(255) DEFAULT 'user_input',
    p_source_type VARCHAR(50) DEFAULT 'manual_entry'
) RETURNS JSONB AS $$
DECLARE
    entity_id VARCHAR(26);
    location_id VARCHAR(26);
    address_id VARCHAR(26);
    relationships JSONB := '[]'::JSONB;
    result JSONB;
BEGIN
    -- Create or match the main entity (Company, Person, etc.)
    entity_id := generate_ulid();
    INSERT INTO nodes (node_id, node_type, primary_name, entity_class, created_by, status)
    VALUES (entity_id, p_entity_type, p_entity_name, 'fact_based', 'propose_api', 'active');
    
    -- Create or match the location (City, State, etc.)
    location_id := get_or_create_geographic_entity(p_location_type, p_location_name);
    
    IF p_address IS NOT NULL THEN
        -- Create address entity
        address_id := generate_ulid();
        INSERT INTO nodes (node_id, node_type, primary_name, entity_class, created_by, status)
        VALUES (address_id, 'Address', p_address, 'fact_based', 'propose_api', 'active');
        
        -- Add coordinates to address if provided
        IF p_coordinates IS NOT NULL THEN
            INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, created_by, status)
            VALUES (generate_ulid(), address_id, 'geometry', p_coordinates::TEXT, 'propose_api', 'active');
        END IF;
        
        -- Entity located_at Address
        relationships := relationships || create_bidirectional_relationship(
            entity_id, address_id, 'Located_At', 'Location_Of', p_source_name, p_source_type
        );
        
        -- Address located_in Location  
        relationships := relationships || create_bidirectional_relationship(
            address_id, location_id, 'Located_In', 'Contains', p_source_name, p_source_type
        );
    ELSE
        -- Direct entity located_in Location
        relationships := relationships || create_bidirectional_relationship(
            entity_id, location_id, 'Located_In', 'Contains', p_source_name, p_source_type
        );
    END IF;
    
    -- Create provenance for entities
    PERFORM create_provenance_record('node', entity_id, p_source_name, p_source_type, 'propose_api', 1.0);
    IF p_address IS NOT NULL THEN
        PERFORM create_provenance_record('node', address_id, p_source_name, p_source_type, 'propose_api', 1.0);
    END IF;
    
    result := jsonb_build_object(
        'entity_id', entity_id,
        'location_id', location_id,
        'address_id', address_id,
        'relationships', relationships
    );
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Test the complete system
\echo '=== Testing Complete Geographic System ==='

\echo '1. Reference entities created:'
SELECT node_type, COUNT(*) as entity_count
FROM reference_entities 
GROUP BY node_type
ORDER BY entity_count DESC;

\echo '2. Test creating Peoples Bank in Clive, Iowa:'
SELECT propose_geographic_fact(
    'Peoples Bank',                    -- entity name
    'Company',                         -- entity type  
    'Clive',                          -- city
    'City',                           -- location type
    '123 Main Street, Clive, IA',     -- address
    '{"type": "Point", "coordinates": [-93.77495, 41.60974]}'::jsonb,  -- coordinates
    'Iowa Business Registry',         -- source
    'iowa_gov_database'              -- source type
);

\echo '3. Check created relationships:'
SELECT 
    r.relationship_type,
    s.primary_name as source_entity,
    s.node_type as source_type,
    t.primary_name as target_entity,
    t.node_type as target_type
FROM relationships r
JOIN nodes s ON r.source_node_id = s.node_id
JOIN nodes t ON r.target_node_id = t.node_id
ORDER BY r.created_at DESC
LIMIT 10;