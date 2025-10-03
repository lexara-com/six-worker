-- =============================================
-- Comprehensive Propose API Demonstration
-- Shows intelligent fact ingestion, entity resolution, conflict detection
-- =============================================

\echo '=== PROPOSE API COMPREHENSIVE DEMONSTRATION ==='
\echo ''

-- Test Case 1: Exact Entity Match + New Relationship
\echo '1. TEST: Exact Entity Match + New Relationship'
\echo '   Proposing: John Smith (existing) works at TechCorp Industries (existing)'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions,
    jsonb_pretty((propose_result).conflicts) as conflicts
FROM (
    SELECT propose_fact(
        'Person', 'John Smith',                    -- Source: existing person
        'Company', 'TechCorp Industries',          -- Target: existing company  
        'Employment',                              -- Relationship type
        'Intake Form #2024-001',                   -- Source name
        'client_intake',                           -- Source type
        '[{"type":"title","value":"Software Engineer"}]'::JSONB,  -- Source attributes
        '[]'::JSONB,                              -- Target attributes
        0.95                                       -- Relationship strength
    ) as propose_result
) AS test1;

\echo ''

-- Test Case 2: New Entity Creation + New Relationship
\echo '2. TEST: New Entity Creation + New Relationship' 
\echo '   Proposing: Maria Rodriguez (new) works at Global Holdings Inc (existing)'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'Maria Rodriguez',               -- Source: new person
        'Company', 'Global Holdings Inc',          -- Target: existing company
        'Employment',                              -- Relationship type
        'LinkedIn Profile Import',                 -- Source name
        'linkedin',                                -- Source type
        '[{"type":"title","value":"Chief Marketing Officer"}, {"type":"nameAlias","value":"Maria R."}]'::JSONB,
        '[]'::JSONB,
        0.90
    ) as propose_result
) AS test2;

\echo ''

-- Test Case 3: Alias-Based Entity Resolution
\echo '3. TEST: Alias-Based Entity Resolution'
\echo '   Proposing: J. Smith (alias match) advises ACME Corp'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'J. Smith',                      -- Source: should match existing via alias
        'Company', 'ACME Corporation',             -- Target: existing company
        'Legal_Counsel',                           -- Relationship type
        'Business Card Collection',                -- Source name
        'business_cards',                          -- Source type
        '[{"type":"title","value":"Legal Advisor"}]'::JSONB,
        '[]'::JSONB,
        0.85
    ) as propose_result
) AS test3;

\echo ''

-- Test Case 4: Relationship Strength Update
\echo '4. TEST: Relationship Strength Update'
\echo '   Proposing: Same relationship but with higher confidence'

-- First, let's create a baseline relationship
SELECT propose_fact(
    'Person', 'Jennifer White', 
    'Company', 'TechCorp Industries',
    'Legal_Counsel',
    'Initial Intake', 'client_intake',
    '[]'::JSONB, '[]'::JSONB, 0.75
) as baseline_relationship;

-- Now propose the same relationship with higher confidence
SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'Jennifer White',               -- Source: existing person
        'Company', 'TechCorp Industries',          -- Target: existing company  
        'Legal_Counsel',                           -- Same relationship type
        'Signed Retainer Agreement',              -- Better source
        'contracts',                               -- Higher-quality source type
        '[]'::JSONB, '[]'::JSONB,
        0.95                                       -- Higher strength
    ) as propose_result
) AS test4;

\echo ''

-- Test Case 5: Conflict Detection (Opposing Relationships)
\echo '5. TEST: Conflict Detection (Opposing Relationships)'
\echo '   Proposing: Attorney represents opposing party (should conflict)'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions,
    jsonb_pretty((propose_result).conflicts) as conflicts
FROM (
    SELECT propose_fact(
        'Person', 'Jennifer White',               -- Source: attorney who already represents TechCorp
        'Company', 'ACME Corporation',             -- Target: different company
        'Opposing_Counsel',                        -- Conflicting relationship type
        'Court Filing Notice',                     -- Source name
        'court_records',                           -- Source type
        '[]'::JSONB, '[]'::JSONB,
        0.90
    ) as propose_result
) AS test5;

\echo ''

-- Test Case 6: Fuzzy Entity Matching
\echo '6. TEST: Fuzzy Entity Matching'
\echo '   Proposing: Slightly different name that should fuzzy match'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'John A Smith',                 -- Source: fuzzy match to "John Smith"
        'Company', 'Regional Services LLC',       -- Target: existing company
        'Employment',                              -- Relationship type
        'HR Database Export',                      -- Source name
        'law_firm_records',                        -- Source type
        '[{"type":"title","value":"Consultant"}]'::JSONB,
        '[]'::JSONB,
        0.88
    ) as propose_result
) AS test6;

\echo ''

-- Test Case 7: Multiple New Entities + Relationship
\echo '7. TEST: Multiple New Entities + Relationship'
\echo '   Proposing: New law firm with new managing partner'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'Alexander Johnson',             -- Source: new person
        'Company', 'Johnson & Partners LLP',      -- Target: new company
        'Employment',                              -- Relationship type
        'Business License Application',            -- Source name
        'public_records',                          -- Source type
        '[{"type":"title","value":"Managing Partner"}, {"type":"nameAlias","value":"Alex Johnson"}]'::JSONB,
        '[{"type":"category","value":"Law Firm"}]'::JSONB,
        0.92
    ) as propose_result
) AS test7;

\echo ''

-- Test Case 8: Duplicate Relationship (Same entities, same relationship type)
\echo '8. TEST: Duplicate Relationship Detection'
\echo '   Proposing: Exact same relationship that already exists'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'Alexander Johnson',             -- Source: person we just created
        'Company', 'Johnson & Partners LLP',      -- Target: company we just created
        'Employment',                              -- Same relationship type
        'Duplicate Source Check',                  -- Different source
        'manual_entry',                            -- Source type
        '[]'::JSONB, '[]'::JSONB,
        0.85                                       -- Lower strength
    ) as propose_result
) AS test8;

\echo ''

-- Verification: Show the impact of all our propose operations
\echo '9. VERIFICATION: Impact Summary'

\echo 'New entities created by propose API:'
SELECT node_type, primary_name, created_at::TIMESTAMP(0)
FROM nodes 
WHERE created_by = 'propose_api'
ORDER BY created_at DESC;

\echo ''
\echo 'New relationships created by propose API:'
SELECT r.relationship_type, s.primary_name as source_name, t.primary_name as target_name, 
       r.strength, r.created_at::TIMESTAMP(0)
FROM relationships r
JOIN nodes s ON r.source_node_id = s.node_id
JOIN nodes t ON r.target_node_id = t.node_id
WHERE r.created_by = 'propose_api'
ORDER BY r.created_at DESC;

\echo ''
\echo 'New provenance records created by propose API:'
SELECT p.asset_type, p.source_name, p.source_type, p.confidence_score,
       p.created_at::TIMESTAMP(0)
FROM provenance p
WHERE p.created_by = 'propose_api'
ORDER BY p.created_at DESC
LIMIT 10;

\echo ''
\echo 'Recent change history (from propose operations):'
SELECT ch.table_name, ch.operation, ch.field_name, 
       ch.old_value, ch.new_value, ch.changed_at::TIMESTAMP(0)
FROM change_history ch
WHERE ch.changed_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
  AND ch.changed_by = 'propose_api'
ORDER BY ch.changed_at DESC
LIMIT 15;

\echo ''

-- Test Case 9: Advanced Scenario - Multi-Alias Resolution
\echo '10. TEST: Advanced Multi-Alias Resolution'
\echo '    Proposing: Entity with multiple aliases should resolve to existing'

SELECT 
    (propose_result).status,
    (propose_result).overall_confidence,
    jsonb_pretty((propose_result).actions) as actions
FROM (
    SELECT propose_fact(
        'Person', 'Robert Brown',                  -- Source: existing person (Bob Brown)
        'Company', 'ACME Corporation',             -- Target: existing company
        'Board_Member',                            -- New relationship type
        'Annual Report Filing',                    -- Source name
        'sec_filings',                             -- High-quality source
        '[{"type":"nameAlias","value":"Bob Brown"}, {"type":"nameAlias","value":"R. Brown"}, {"type":"title","value":"Board Chairman"}]'::JSONB,
        '[]'::JSONB,
        0.93
    ) as propose_result
) AS test10;

\echo ''

-- Final Summary
\echo '11. FINAL SUMMARY: Database State After All Operations'

SELECT 
    'Total Entities' as metric, COUNT(*)::TEXT as value 
FROM nodes WHERE status = 'active'
UNION ALL
SELECT 
    'Total Relationships', COUNT(*)::TEXT 
FROM relationships WHERE status = 'active'
UNION ALL
SELECT 
    'Total Provenance Records', COUNT(*)::TEXT 
FROM provenance WHERE status = 'active'
UNION ALL
SELECT 
    'Entities from Propose API', COUNT(*)::TEXT 
FROM nodes WHERE created_by = 'propose_api'
UNION ALL
SELECT 
    'Relationships from Propose API', COUNT(*)::TEXT 
FROM relationships WHERE created_by = 'propose_api'
ORDER BY metric;

\echo ''
\echo '=== PROPOSE API DEMONSTRATION COMPLETE ==='
\echo ''
\echo 'Key Features Demonstrated:'
\echo '- Entity resolution (exact, alias, fuzzy matching)'  
\echo '- Relationship creation, updates, and conflict detection'
\echo '- Automatic provenance tracking'
\echo '- Change history logging'
\echo '- Intelligent decision making based on confidence scores'