-- =============================================
-- V20: Create Name Alias System for Enhanced Searching
-- =============================================
-- This creates a comprehensive name alias system for better conflict detection
-- and person matching, handling nicknames, diminutives, and formal variations
-- =============================================

-- Create the name aliases table
CREATE TABLE IF NOT EXISTS name_aliases (
    alias_id VARCHAR(26) PRIMARY KEY DEFAULT generate_ulid(),
    canonical_name VARCHAR(100) NOT NULL,  -- The standard form (e.g., 'MICHAEL')
    alias_name VARCHAR(100) NOT NULL,      -- The alias (e.g., 'MIKE')
    alias_type VARCHAR(50) NOT NULL CHECK (alias_type IN (
        'nickname',      -- Common nicknames (Mike for Michael)
        'diminutive',    -- Shortened forms (Alex for Alexander)
        'formal',        -- Formal versions (Robert for Bob)
        'alternate',     -- Alternative spellings (Jon for John)
        'cultural'       -- Cultural variations (Juan for John)
    )),
    bidirectional BOOLEAN DEFAULT false,    -- If true, alias works both ways
    confidence DECIMAL(3,2) DEFAULT 0.95,   -- How confident we are in this mapping
    usage_frequency VARCHAR(20) DEFAULT 'common' CHECK (usage_frequency IN ('rare', 'uncommon', 'common', 'very_common')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Ensure uniqueness
    CONSTRAINT unique_canonical_alias UNIQUE (canonical_name, alias_name)
);

-- Create indexes for fast lookups
CREATE INDEX idx_alias_canonical ON name_aliases(canonical_name);
CREATE INDEX idx_alias_name ON name_aliases(alias_name);
CREATE INDEX idx_alias_type ON name_aliases(alias_type);

-- Add common name aliases
INSERT INTO name_aliases (canonical_name, alias_name, alias_type, confidence, usage_frequency, bidirectional) VALUES
-- Michael variations
('MICHAEL', 'MIKE', 'nickname', 0.95, 'very_common', false),
('MICHAEL', 'MICK', 'nickname', 0.85, 'common', false),
('MICHAEL', 'MICKEY', 'nickname', 0.80, 'uncommon', false),
('MICHAEL', 'MICKY', 'nickname', 0.80, 'uncommon', false),

-- Robert variations
('ROBERT', 'BOB', 'nickname', 0.95, 'very_common', false),
('ROBERT', 'BOBBY', 'nickname', 0.90, 'common', false),
('ROBERT', 'ROB', 'diminutive', 0.95, 'very_common', false),
('ROBERT', 'ROBBIE', 'nickname', 0.85, 'common', false),
('ROBERT', 'BERT', 'nickname', 0.70, 'uncommon', false),

-- William variations
('WILLIAM', 'BILL', 'nickname', 0.95, 'very_common', false),
('WILLIAM', 'BILLY', 'nickname', 0.90, 'common', false),
('WILLIAM', 'WILL', 'diminutive', 0.95, 'very_common', false),
('WILLIAM', 'WILLIE', 'nickname', 0.85, 'common', false),
('WILLIAM', 'LIAM', 'diminutive', 0.80, 'common', false),

-- James variations
('JAMES', 'JIM', 'nickname', 0.95, 'very_common', false),
('JAMES', 'JIMMY', 'nickname', 0.90, 'common', false),
('JAMES', 'JAMIE', 'diminutive', 0.85, 'common', false),
('JAMES', 'JAY', 'diminutive', 0.70, 'uncommon', false),

-- John variations
('JOHN', 'JACK', 'nickname', 0.90, 'very_common', false),
('JOHN', 'JOHNNY', 'nickname', 0.90, 'common', false),
('JOHN', 'JON', 'alternate', 0.95, 'very_common', true),  -- Bidirectional

-- Richard variations
('RICHARD', 'DICK', 'nickname', 0.90, 'common', false),
('RICHARD', 'RICK', 'diminutive', 0.95, 'very_common', false),
('RICHARD', 'RICKY', 'nickname', 0.85, 'common', false),
('RICHARD', 'RICH', 'diminutive', 0.90, 'common', false),

-- Charles variations
('CHARLES', 'CHUCK', 'nickname', 0.90, 'common', false),
('CHARLES', 'CHARLIE', 'nickname', 0.95, 'very_common', false),
('CHARLES', 'CHAS', 'diminutive', 0.70, 'uncommon', false),

-- Thomas variations
('THOMAS', 'TOM', 'diminutive', 0.95, 'very_common', false),
('THOMAS', 'TOMMY', 'nickname', 0.90, 'common', false),
('THOMAS', 'THOM', 'alternate', 0.80, 'uncommon', true),

-- Daniel variations
('DANIEL', 'DAN', 'diminutive', 0.95, 'very_common', false),
('DANIEL', 'DANNY', 'nickname', 0.90, 'common', false),

-- Joseph variations
('JOSEPH', 'JOE', 'diminutive', 0.95, 'very_common', false),
('JOSEPH', 'JOEY', 'nickname', 0.85, 'common', false),
('JOSEPH', 'JO', 'diminutive', 0.60, 'rare', false),

-- Christopher variations
('CHRISTOPHER', 'CHRIS', 'diminutive', 0.95, 'very_common', false),
('CHRISTOPHER', 'KIT', 'nickname', 0.60, 'rare', false),
('CHRISTOPHER', 'TOPHER', 'nickname', 0.70, 'uncommon', false),

-- Matthew variations
('MATTHEW', 'MATT', 'diminutive', 0.95, 'very_common', false),
('MATTHEW', 'MATTY', 'nickname', 0.80, 'uncommon', false),

-- Anthony variations
('ANTHONY', 'TONY', 'nickname', 0.95, 'very_common', false),
('ANTHONY', 'ANT', 'diminutive', 0.70, 'uncommon', false),

-- Andrew variations
('ANDREW', 'ANDY', 'nickname', 0.95, 'very_common', false),
('ANDREW', 'DREW', 'diminutive', 0.90, 'common', false),

-- David variations
('DAVID', 'DAVE', 'nickname', 0.95, 'very_common', false),
('DAVID', 'DAVEY', 'nickname', 0.80, 'uncommon', false),

-- Steven/Stephen variations
('STEVEN', 'STEVE', 'diminutive', 0.95, 'very_common', false),
('STEPHEN', 'STEVE', 'diminutive', 0.95, 'very_common', false),
('STEVEN', 'STEPHEN', 'alternate', 0.95, 'very_common', true),  -- Bidirectional

-- Alexander variations
('ALEXANDER', 'ALEX', 'diminutive', 0.95, 'very_common', false),
('ALEXANDER', 'AL', 'diminutive', 0.80, 'common', false),
('ALEXANDER', 'SANDY', 'nickname', 0.70, 'uncommon', false),
('ALEXANDER', 'XANDER', 'nickname', 0.75, 'uncommon', false),

-- Benjamin variations
('BENJAMIN', 'BEN', 'diminutive', 0.95, 'very_common', false),
('BENJAMIN', 'BENNY', 'nickname', 0.85, 'common', false),
('BENJAMIN', 'BENJI', 'nickname', 0.80, 'uncommon', false),

-- Nicholas variations
('NICHOLAS', 'NICK', 'diminutive', 0.95, 'very_common', false),
('NICHOLAS', 'NICKY', 'nickname', 0.85, 'common', false),

-- Timothy variations
('TIMOTHY', 'TIM', 'diminutive', 0.95, 'very_common', false),
('TIMOTHY', 'TIMMY', 'nickname', 0.85, 'common', false),

-- Kenneth variations
('KENNETH', 'KEN', 'diminutive', 0.95, 'very_common', false),
('KENNETH', 'KENNY', 'nickname', 0.85, 'common', false),

-- Gerald variations
('GERALD', 'JERRY', 'nickname', 0.90, 'common', false),
('GERALD', 'GERRY', 'nickname', 0.90, 'common', false),

-- Edward variations
('EDWARD', 'ED', 'diminutive', 0.95, 'very_common', false),
('EDWARD', 'EDDIE', 'nickname', 0.90, 'common', false),
('EDWARD', 'TED', 'nickname', 0.80, 'uncommon', false),
('EDWARD', 'NED', 'nickname', 0.70, 'uncommon', false),

-- Female names
-- Elizabeth variations
('ELIZABETH', 'LIZ', 'diminutive', 0.95, 'very_common', false),
('ELIZABETH', 'BETH', 'diminutive', 0.90, 'common', false),
('ELIZABETH', 'BETTY', 'nickname', 0.85, 'common', false),
('ELIZABETH', 'BETSY', 'nickname', 0.80, 'uncommon', false),
('ELIZABETH', 'ELIZA', 'diminutive', 0.85, 'common', false),
('ELIZABETH', 'LIBBY', 'nickname', 0.75, 'uncommon', false),

-- Margaret variations
('MARGARET', 'MAGGIE', 'nickname', 0.90, 'common', false),
('MARGARET', 'MEG', 'diminutive', 0.85, 'common', false),
('MARGARET', 'PEGGY', 'nickname', 0.80, 'uncommon', false),
('MARGARET', 'MARGE', 'diminutive', 0.75, 'uncommon', false),

-- Katherine/Catherine variations
('KATHERINE', 'KATE', 'diminutive', 0.95, 'very_common', false),
('KATHERINE', 'KATHY', 'nickname', 0.90, 'common', false),
('KATHERINE', 'KATIE', 'nickname', 0.90, 'common', false),
('KATHERINE', 'KAT', 'diminutive', 0.85, 'common', false),
('KATHERINE', 'KITTY', 'nickname', 0.70, 'uncommon', false),
('CATHERINE', 'CATHY', 'nickname', 0.90, 'common', false),
('CATHERINE', 'CAT', 'diminutive', 0.80, 'uncommon', false),
('KATHERINE', 'CATHERINE', 'alternate', 0.95, 'very_common', true),  -- Bidirectional

-- Patricia variations
('PATRICIA', 'PAT', 'diminutive', 0.95, 'very_common', false),
('PATRICIA', 'PATTY', 'nickname', 0.90, 'common', false),
('PATRICIA', 'TRISH', 'diminutive', 0.85, 'common', false),
('PATRICIA', 'TRISHA', 'nickname', 0.85, 'common', false),

-- Jennifer variations
('JENNIFER', 'JEN', 'diminutive', 0.95, 'very_common', false),
('JENNIFER', 'JENNY', 'nickname', 0.90, 'common', false),
('JENNIFER', 'JENN', 'diminutive', 0.90, 'common', false),

-- Susan variations
('SUSAN', 'SUE', 'diminutive', 0.95, 'very_common', false),
('SUSAN', 'SUSIE', 'nickname', 0.85, 'common', false),
('SUSAN', 'SUZY', 'nickname', 0.80, 'uncommon', false),

-- Deborah variations
('DEBORAH', 'DEB', 'diminutive', 0.95, 'very_common', false),
('DEBORAH', 'DEBBIE', 'nickname', 0.90, 'common', false),
('DEBORAH', 'DEBRA', 'alternate', 0.95, 'very_common', true),

-- Rebecca variations
('REBECCA', 'BECCA', 'diminutive', 0.90, 'common', false),
('REBECCA', 'BECKY', 'nickname', 0.90, 'common', false),

-- Jessica variations
('JESSICA', 'JESS', 'diminutive', 0.95, 'very_common', false),
('JESSICA', 'JESSIE', 'nickname', 0.90, 'common', false)
ON CONFLICT (canonical_name, alias_name) DO NOTHING;

-- Create function to get all possible names for a search term
CREATE OR REPLACE FUNCTION get_name_variations(p_name TEXT)
RETURNS TABLE(name_variant TEXT, relation_type TEXT, confidence DECIMAL) AS $$
BEGIN
    RETURN QUERY
    -- Return the input name itself
    SELECT UPPER(p_name)::TEXT, 'original'::TEXT, 1.0::DECIMAL
    
    UNION
    
    -- Get aliases where input is the canonical name
    SELECT UPPER(alias_name)::TEXT, alias_type::TEXT, name_aliases.confidence
    FROM name_aliases
    WHERE UPPER(canonical_name) = UPPER(p_name)
    
    UNION
    
    -- Get canonical names where input is an alias
    SELECT UPPER(canonical_name)::TEXT, 'canonical_of_' || alias_type::TEXT, name_aliases.confidence
    FROM name_aliases
    WHERE UPPER(alias_name) = UPPER(p_name)
    
    UNION
    
    -- Get other aliases of the same canonical name (if input is an alias)
    SELECT UPPER(na2.alias_name)::TEXT, 'co_alias'::TEXT, na2.confidence * 0.9
    FROM name_aliases na1
    JOIN name_aliases na2 ON na1.canonical_name = na2.canonical_name
    WHERE UPPER(na1.alias_name) = UPPER(p_name)
      AND na2.alias_name != na1.alias_name;
END;
$$ LANGUAGE plpgsql;

-- Create function to search for persons considering aliases
CREATE OR REPLACE FUNCTION search_persons_with_aliases(p_first_name TEXT, p_last_name TEXT DEFAULT NULL)
RETURNS TABLE(
    node_id VARCHAR(26),
    primary_name VARCHAR(255),
    matched_on TEXT,
    match_confidence DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    WITH name_variants AS (
        SELECT name_variant, confidence
        FROM get_name_variations(p_first_name)
    )
    SELECT DISTINCT
        n.node_id,
        n.primary_name,
        'first_name: ' || a.attribute_value || ' (via ' || nv.name_variant || ')' as matched_on,
        nv.confidence as match_confidence
    FROM nodes n
    JOIN attributes a ON n.node_id = a.node_id
    JOIN name_variants nv ON UPPER(a.attribute_value) = nv.name_variant
    WHERE n.node_type = 'Person'
      AND a.attribute_type = 'computed_first_name'
      AND (p_last_name IS NULL OR EXISTS (
          SELECT 1 FROM attributes a2
          WHERE a2.node_id = n.node_id
            AND a2.attribute_type = 'computed_surname'
            AND UPPER(a2.attribute_value) = UPPER(p_last_name)
      ))
    ORDER BY match_confidence DESC, n.primary_name;
END;
$$ LANGUAGE plpgsql;

-- Test the system
SELECT 'Testing name alias system:' as info;

-- Test variations of Michael
SELECT 'Searching for variations of MIKE:' as test;
SELECT * FROM get_name_variations('MIKE');

-- Count how many Michaels we have
SELECT 'Count of persons who might be called Mike:' as test;
SELECT COUNT(DISTINCT node_id) as count
FROM search_persons_with_aliases('MIKE');

-- Show sample matches
SELECT 'Sample persons matching MIKE (limit 10):' as test;
SELECT * FROM search_persons_with_aliases('MIKE')
LIMIT 10;

-- Statistics
SELECT 'Alias system statistics:' as info;
SELECT 
    COUNT(DISTINCT canonical_name) as unique_canonical_names,
    COUNT(*) as total_aliases,
    AVG(confidence) as average_confidence
FROM name_aliases;

-- Add index to speed up searches
CREATE INDEX IF NOT EXISTS idx_attributes_type_value 
ON attributes(attribute_type, UPPER(attribute_value));

COMMENT ON TABLE name_aliases IS 'Stores name aliases and nicknames for improved person matching and conflict detection';
COMMENT ON FUNCTION get_name_variations IS 'Returns all possible name variations for a given name, including aliases and canonical forms';
COMMENT ON FUNCTION search_persons_with_aliases IS 'Searches for Person nodes considering name aliases and variations';

SELECT 'Name alias system installed successfully!' as status;