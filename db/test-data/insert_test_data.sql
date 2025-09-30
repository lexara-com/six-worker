-- =============================================
-- Test Data for Law Firm Conflict Checking
-- Purpose: Comprehensive test scenarios including conflict situations
-- =============================================

BEGIN;

-- =============================================
-- Test Scenario 1: Basic Law Firm Structure
-- =============================================

-- Law Firm
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('11111111-1111-1111-1111-111111111111', 'Company', 'Smith & Associates Law Firm');

-- Partners and Associates
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('22222222-2222-2222-2222-222222222221', 'Person', 'John Smith'),
('22222222-2222-2222-2222-222222222222', 'Person', 'Mary Johnson'),
('22222222-2222-2222-2222-222222222223', 'Person', 'David Wilson'),
('22222222-2222-2222-2222-222222222224', 'Person', 'Sarah Davis');

-- Add aliases for people (common cause of missed conflicts)
INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('22222222-2222-2222-2222-222222222221', 'nameAlias', 'J. Smith'),
('22222222-2222-2222-2222-222222222221', 'nameAlias', 'Johnny Smith'),
('22222222-2222-2222-2222-222222222221', 'title', 'Senior Partner'),
('22222222-2222-2222-2222-222222222222', 'nameAlias', 'Mary J. Johnson'),
('22222222-2222-2222-2222-222222222222', 'nameAlias', 'M. Johnson'),
('22222222-2222-2222-2222-222222222222', 'title', 'Partner'),
('22222222-2222-2222-2222-222222222223', 'nameAlias', 'Dave Wilson'),
('22222222-2222-2222-2222-222222222223', 'title', 'Associate'),
('22222222-2222-2222-2222-222222222224', 'nameAlias', 'Sarah D. Davis'),
('22222222-2222-2222-2222-222222222224', 'title', 'Junior Associate');

-- Employment relationships
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('22222222-2222-2222-2222-222222222221', '11111111-1111-1111-1111-111111111111', 'Employment', 1.0),
('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'Employment', 1.0),
('22222222-2222-2222-2222-222222222223', '11111111-1111-1111-1111-111111111111', 'Employment', 0.9),
('22222222-2222-2222-2222-222222222224', '11111111-1111-1111-1111-111111111111', 'Employment', 0.8);

-- =============================================
-- Test Scenario 2: Client Companies and Employees
-- =============================================

-- Client Company: ACME Corporation
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('33333333-3333-3333-3333-333333333331', 'Company', 'ACME Corporation'),
('44444444-4444-4444-4444-444444444441', 'Person', 'Robert Brown'),
('44444444-4444-4444-4444-444444444442', 'Person', 'Lisa Anderson'),
('44444444-4444-4444-4444-444444444443', 'Person', 'Michael Taylor');

-- ACME Corporation aliases and attributes
INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('33333333-3333-3333-3333-333333333331', 'nameAlias', 'ACME Corp'),
('33333333-3333-3333-3333-333333333331', 'nameAlias', 'ACME Inc'),
('33333333-3333-3333-3333-333333333331', 'category', 'Technology Company'),
('44444444-4444-4444-4444-444444444441', 'nameAlias', 'Bob Brown'),
('44444444-4444-4444-4444-444444444441', 'nameAlias', 'R. Brown'),
('44444444-4444-4444-4444-444444444441', 'title', 'CEO'),
('44444444-4444-4444-4444-444444444442', 'nameAlias', 'L. Anderson'),
('44444444-4444-4444-4444-444444444442', 'title', 'CFO'),
('44444444-4444-4444-4444-444444444443', 'nameAlias', 'Mike Taylor'),
('44444444-4444-4444-4444-444444444443', 'nameAlias', 'M. Taylor'),
('44444444-4444-4444-4444-444444444443', 'title', 'VP Engineering');

-- Employment at ACME
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('44444444-4444-4444-4444-444444444441', '33333333-3333-3333-3333-333333333331', 'Employment', 1.0),
('44444444-4444-4444-4444-444444444442', '33333333-3333-3333-3333-333333333331', 'Employment', 1.0),
('44444444-4444-4444-4444-444444444443', '33333333-3333-3333-3333-333333333331', 'Employment', 0.9);

-- Client relationship: Law firm represents ACME
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('22222222-2222-2222-2222-222222222221', '33333333-3333-3333-3333-333333333331', 'Legal_Counsel', 1.0);

-- =============================================
-- Test Scenario 3: Potential Conflict Situation
-- =============================================

-- Competitor Company: TechCorp Industries  
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('55555555-5555-5555-5555-555555555551', 'Company', 'TechCorp Industries'),
('66666666-6666-6666-6666-666666666661', 'Person', 'Jennifer White'),
('66666666-6666-6666-6666-666666666662', 'Person', 'Thomas Green');

INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('55555555-5555-5555-5555-555555555551', 'nameAlias', 'TechCorp Inc'),
('55555555-5555-5555-5555-555555555551', 'nameAlias', 'Tech Corp'),
('55555555-5555-5555-5555-555555555551', 'category', 'Technology Company'),
('66666666-6666-6666-6666-666666666661', 'nameAlias', 'Jenny White'),
('66666666-6666-6666-6666-666666666661', 'nameAlias', 'J. White'),
('66666666-6666-6666-6666-666666666661', 'title', 'CEO'),
('66666666-6666-6666-6666-666666666662', 'nameAlias', 'Tom Green'),
('66666666-6666-6666-6666-666666666662', 'nameAlias', 'T. Green'),
('66666666-6666-6666-6666-666666666662', 'title', 'Legal Counsel');

-- Employment at TechCorp
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('66666666-6666-6666-6666-666666666661', '55555555-5555-5555-5555-555555555551', 'Employment', 1.0),
('66666666-6666-6666-6666-666666666662', '55555555-5555-5555-5555-555555555551', 'Employment', 1.0);

-- CONFLICT: Same attorney represents competing companies (should be detected)
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('22222222-2222-2222-2222-222222222222', '55555555-5555-5555-5555-555555555551', 'Legal_Counsel', 1.0);

-- =============================================
-- Test Scenario 4: Complex Family/Business Relationships
-- =============================================

-- Individual client with family ties to existing clients
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('77777777-7777-7777-7777-777777777771', 'Person', 'Amanda Brown'),
('88888888-8888-8888-8888-888888888881', 'Company', 'Brown Family Trust'),
('99999999-9999-9999-9999-999999999991', 'Event', 'Brown vs TechCorp Lawsuit');

INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('77777777-7777-7777-7777-777777777771', 'nameAlias', 'Amanda B. Brown'),
('77777777-7777-7777-7777-777777777771', 'nameAlias', 'A. Brown'),
('88888888-8888-8888-8888-888888888881', 'nameAlias', 'Brown Trust'),
('99999999-9999-9999-9999-999999999991', 'category', 'Litigation');

-- Family relationship (potential conflict: Amanda is Robert Brown's daughter)
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('77777777-7777-7777-7777-777777777771', '44444444-4444-4444-4444-444444444441', 'Family', 1.0),
('77777777-7777-7777-7777-777777777771', '88888888-8888-8888-8888-888888888881', 'Ownership', 1.0);

-- Lawsuit participation (Amanda suing TechCorp, but dad works for ACME which law firm represents)
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('77777777-7777-7777-7777-777777777771', '99999999-9999-9999-9999-999999999991', 'Participation', 1.0),
('55555555-5555-5555-5555-555555555551', '99999999-9999-9999-9999-999999999991', 'Participation', 1.0);

-- =============================================
-- Test Scenario 5: Historical Relationships (Expired)
-- =============================================

-- Former employee who left and joined competitor
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Person', 'Kevin Miller');

INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'nameAlias', 'K. Miller'),
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'nameAlias', 'Kevin M. Miller');

-- Historical employment at ACME (expired)
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength, valid_from, valid_to) VALUES 
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '33333333-3333-3333-3333-333333333331', 'Employment', 1.0, '2020-01-01', '2022-12-31');

-- Current employment at TechCorp (active)  
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength, valid_from) VALUES 
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '55555555-5555-5555-5555-555555555551', 'Employment', 1.0, '2023-01-15');

-- =============================================
-- Test Scenario 6: Subsidiary Relationships
-- =============================================

-- Parent company and subsidiary
INSERT INTO nodes (node_id, node_type, primary_name) VALUES 
('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Company', 'Global Holdings Inc'),
('cccccccc-cccc-cccc-cccc-cccccccccccc', 'Company', 'Regional Services LLC');

INSERT INTO attributes (node_id, attribute_type, attribute_value) VALUES 
('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'nameAlias', 'Global Holdings'),
('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'category', 'Holding Company'),
('cccccccc-cccc-cccc-cccc-cccccccccccc', 'nameAlias', 'Regional Services'),
('cccccccc-cccc-cccc-cccc-cccccccccccc', 'category', 'Service Company');

-- Subsidiary relationship
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('cccccccc-cccc-cccc-cccc-cccccccccccc', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Subsidiary', 1.0);

-- ACME is owned by Global Holdings (2-degree relationship)
INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength) VALUES 
('33333333-3333-3333-3333-333333333331', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Subsidiary', 1.0);

-- =============================================
-- Pre-compute some conflict matrix entries
-- =============================================

-- Direct conflict: Law firm represents both ACME and TechCorp (competitors)
INSERT INTO conflict_matrix (entity_a_id, entity_b_id, conflict_type, conflict_path, conflict_strength, degrees_of_separation) VALUES 
('33333333-3333-3333-3333-333333333331', '55555555-5555-5555-5555-555555555551', 'Legal_Counsel_Conflict', 
 '[{"entity":"ACME Corporation","relationship":"Legal_Counsel"}, {"entity":"Smith & Associates Law Firm","relationship":"Legal_Counsel"}, {"entity":"TechCorp Industries","relationship":"Legal_Counsel"}]'::jsonb, 
 0.95, 2);

-- Family conflict: Amanda Brown (daughter) vs TechCorp, but dad (Robert) works for ACME (law firm's client)
INSERT INTO conflict_matrix (entity_a_id, entity_b_id, conflict_type, conflict_path, conflict_strength, degrees_of_separation) VALUES 
('77777777-7777-7777-7777-777777777771', '55555555-5555-5555-5555-555555555551', 'Family_Business_Conflict',
 '[{"entity":"Amanda Brown","relationship":"Family"}, {"entity":"Robert Brown","relationship":"Employment"}, {"entity":"ACME Corporation","relationship":"Legal_Counsel"}, {"entity":"Smith & Associates Law Firm"}]'::jsonb,
 0.75, 3);

COMMIT;

-- =============================================
-- Verification Queries
-- =============================================

-- Check data insertion
SELECT 'Nodes inserted: ' || COUNT(*) FROM nodes;
SELECT 'Relationships inserted: ' || COUNT(*) FROM relationships;
SELECT 'Attributes inserted: ' || COUNT(*) FROM attributes;
SELECT 'Conflict matrix entries: ' || COUNT(*) FROM conflict_matrix;

-- Verify normalization is working
SELECT 
    primary_name,
    normalized_name,
    CASE WHEN normalized_name = normalize_name(primary_name) THEN 'OK' ELSE 'ERROR' END as normalization_check
FROM nodes 
LIMIT 5;