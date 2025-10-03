-- =============================================
-- V21: Expand Name Aliases with Additional Common Variations
-- =============================================
-- Adds more comprehensive name aliases including alternate spellings,
-- cultural variations, and commonly confused names
-- =============================================

-- Add more name aliases
INSERT INTO name_aliases (canonical_name, alias_name, alias_type, confidence, usage_frequency, bidirectional) VALUES

-- Shawn/Sean variations (as requested)
('SHAWN', 'SEAN', 'alternate', 0.95, 'very_common', true),
('SHAWN', 'SHAUN', 'alternate', 0.95, 'common', true),
('SEAN', 'SHAUN', 'alternate', 0.95, 'common', true),

-- Patricia variations (expanded as requested)
('PATRICIA', 'PAT', 'diminutive', 0.95, 'very_common', false),
('PATRICIA', 'PATTY', 'nickname', 0.95, 'very_common', false),
('PATRICIA', 'PATTI', 'nickname', 0.95, 'very_common', false),
('PATRICIA', 'PATTIE', 'nickname', 0.90, 'common', false),
('PATRICIA', 'PATSY', 'nickname', 0.80, 'uncommon', false),
('PATRICIA', 'TRISH', 'diminutive', 0.90, 'common', false),
('PATRICIA', 'TRISHA', 'nickname', 0.90, 'common', false),
('PATRICIA', 'TISH', 'diminutive', 0.70, 'rare', false),

-- Lawrence variations
('LAWRENCE', 'LARRY', 'nickname', 0.95, 'very_common', false),
('LAWRENCE', 'LARS', 'diminutive', 0.70, 'uncommon', false),
('LAWRENCE', 'LAURIE', 'nickname', 0.75, 'uncommon', false),

-- Eugene variations
('EUGENE', 'GENE', 'diminutive', 0.95, 'very_common', false),

-- Francis variations
('FRANCIS', 'FRANK', 'nickname', 0.90, 'very_common', false),
('FRANCIS', 'FRAN', 'diminutive', 0.85, 'common', false),

-- Frederick variations
('FREDERICK', 'FRED', 'diminutive', 0.95, 'very_common', false),
('FREDERICK', 'FREDDY', 'nickname', 0.90, 'common', false),
('FREDERICK', 'FREDDIE', 'nickname', 0.90, 'common', false),
('FREDERICK', 'RICK', 'diminutive', 0.70, 'uncommon', false),

-- Henry variations
('HENRY', 'HANK', 'nickname', 0.90, 'common', false),
('HENRY', 'HARRY', 'nickname', 0.85, 'common', false),
('HENRY', 'HAL', 'nickname', 0.75, 'uncommon', false),

-- Albert variations
('ALBERT', 'AL', 'diminutive', 0.95, 'very_common', false),
('ALBERT', 'BERT', 'diminutive', 0.85, 'common', false),
('ALBERT', 'BERTIE', 'nickname', 0.75, 'uncommon', false),

-- Raymond variations
('RAYMOND', 'RAY', 'diminutive', 0.95, 'very_common', false),

-- Ronald variations
('RONALD', 'RON', 'diminutive', 0.95, 'very_common', false),
('RONALD', 'RONNIE', 'nickname', 0.90, 'common', false),

-- Donald variations
('DONALD', 'DON', 'diminutive', 0.95, 'very_common', false),
('DONALD', 'DONNIE', 'nickname', 0.90, 'common', false),

-- Gregory variations
('GREGORY', 'GREG', 'diminutive', 0.95, 'very_common', false),
('GREGORY', 'GREGG', 'alternate', 0.90, 'common', true),

-- Jeffrey variations
('JEFFREY', 'JEFF', 'diminutive', 0.95, 'very_common', false),
('JEFFREY', 'GEOFFREY', 'alternate', 0.90, 'common', true),
('GEOFFREY', 'GEOFF', 'diminutive', 0.95, 'very_common', false),

-- Jonathan variations
('JONATHAN', 'JON', 'diminutive', 0.95, 'very_common', false),
('JONATHAN', 'JOHN', 'alternate', 0.70, 'common', false),
('JONATHAN', 'NATHAN', 'diminutive', 0.80, 'common', false),

-- Samuel variations
('SAMUEL', 'SAM', 'diminutive', 0.95, 'very_common', false),
('SAMUEL', 'SAMMY', 'nickname', 0.85, 'common', false),

-- Joshua variations
('JOSHUA', 'JOSH', 'diminutive', 0.95, 'very_common', false),

-- Zachary variations
('ZACHARY', 'ZACH', 'diminutive', 0.95, 'very_common', false),
('ZACHARY', 'ZACK', 'diminutive', 0.90, 'common', false),
('ZACHARY', 'ZACKY', 'nickname', 0.75, 'uncommon', false),

-- Philip variations
('PHILIP', 'PHIL', 'diminutive', 0.95, 'very_common', false),
('PHILIP', 'PHILLIP', 'alternate', 0.95, 'very_common', true),

-- Stephen/Steven (additional)
('STEPHEN', 'STEVE', 'diminutive', 0.95, 'very_common', false),
('STEPHEN', 'STEVIE', 'nickname', 0.85, 'common', false),
('STEVEN', 'STEVE', 'diminutive', 0.95, 'very_common', false),
('STEVEN', 'STEVIE', 'nickname', 0.85, 'common', false),

-- Douglas variations
('DOUGLAS', 'DOUG', 'diminutive', 0.95, 'very_common', false),

-- Walter variations
('WALTER', 'WALT', 'diminutive', 0.90, 'common', false),
('WALTER', 'WALLY', 'nickname', 0.80, 'uncommon', false),

-- Harold variations
('HAROLD', 'HAL', 'diminutive', 0.85, 'common', false),
('HAROLD', 'HARRY', 'nickname', 0.90, 'common', false),

-- Leonard variations
('LEONARD', 'LEN', 'diminutive', 0.90, 'common', false),
('LEONARD', 'LENNY', 'nickname', 0.90, 'common', false),
('LEONARD', 'LEO', 'diminutive', 0.85, 'common', false),

-- Theodore variations
('THEODORE', 'TED', 'nickname', 0.90, 'common', false),
('THEODORE', 'TEDDY', 'nickname', 0.85, 'common', false),
('THEODORE', 'THEO', 'diminutive', 0.90, 'common', false),

-- Female names - Additional variations

-- Barbara variations
('BARBARA', 'BARB', 'diminutive', 0.95, 'very_common', false),
('BARBARA', 'BARBIE', 'nickname', 0.80, 'uncommon', false),
('BARBARA', 'BABS', 'nickname', 0.75, 'uncommon', false),

-- Linda variations
('LINDA', 'LIN', 'diminutive', 0.85, 'common', false),
('LINDA', 'LINDY', 'nickname', 0.80, 'uncommon', false),

-- Mary variations
('MARY', 'MOLLY', 'nickname', 0.80, 'common', false),
('MARY', 'POLLY', 'nickname', 0.70, 'uncommon', false),
('MARY', 'MAE', 'alternate', 0.85, 'common', false),
('MARY', 'MAY', 'alternate', 0.85, 'common', false),

-- Maria variations
('MARIA', 'MARIE', 'alternate', 0.90, 'very_common', true),
('MARIA', 'MARY', 'alternate', 0.85, 'common', false),

-- Ann/Anne variations
('ANN', 'ANNE', 'alternate', 0.95, 'very_common', true),
('ANN', 'ANNIE', 'nickname', 0.90, 'common', false),
('ANN', 'ANNA', 'alternate', 0.90, 'common', true),
('ANNE', 'ANNIE', 'nickname', 0.90, 'common', false),
('ANNA', 'ANNIE', 'nickname', 0.85, 'common', false),
('ANN', 'NAN', 'nickname', 0.70, 'uncommon', false),
('ANN', 'NANCY', 'nickname', 0.75, 'uncommon', false),
('ANNE', 'NANCY', 'nickname', 0.75, 'uncommon', false),

-- Nancy variations
('NANCY', 'NAN', 'diminutive', 0.85, 'common', false),

-- Carol variations
('CAROL', 'CAROLE', 'alternate', 0.95, 'very_common', true),
('CAROL', 'CAROLINE', 'formal', 0.80, 'common', false),
('CAROLINE', 'CAROL', 'diminutive', 0.85, 'common', false),
('CAROLINE', 'CARRIE', 'nickname', 0.90, 'common', false),
('CAROL', 'CARRIE', 'nickname', 0.80, 'uncommon', false),

-- Dorothy variations
('DOROTHY', 'DOT', 'diminutive', 0.85, 'common', false),
('DOROTHY', 'DOTTIE', 'nickname', 0.85, 'common', false),
('DOROTHY', 'DOLLY', 'nickname', 0.75, 'uncommon', false),

-- Judith variations
('JUDITH', 'JUDY', 'nickname', 0.95, 'very_common', false),
('JUDITH', 'JUDE', 'diminutive', 0.80, 'uncommon', false),

-- Sandra variations
('SANDRA', 'SANDY', 'nickname', 0.95, 'very_common', false),
('SANDRA', 'SANDI', 'nickname', 0.90, 'common', false),

-- Donna variations
('DONNA', 'DONNIE', 'nickname', 0.75, 'uncommon', false),

-- Janet variations
('JANET', 'JAN', 'diminutive', 0.90, 'common', false),
('JANET', 'JANE', 'alternate', 0.80, 'common', false),
('JANE', 'JANIE', 'nickname', 0.85, 'common', false),
('JANET', 'JANIE', 'nickname', 0.85, 'common', false),

-- Janice variations
('JANICE', 'JAN', 'diminutive', 0.90, 'common', false),

-- Diane variations
('DIANE', 'DI', 'diminutive', 0.85, 'common', false),
('DIANE', 'DIANA', 'alternate', 0.90, 'common', true),

-- Cynthia variations
('CYNTHIA', 'CINDY', 'nickname', 0.95, 'very_common', false),
('CYNTHIA', 'CYNDI', 'nickname', 0.85, 'common', false),

-- Christine variations
('CHRISTINE', 'CHRIS', 'diminutive', 0.90, 'common', false),
('CHRISTINE', 'CHRISSY', 'nickname', 0.85, 'common', false),
('CHRISTINE', 'CHRISTY', 'nickname', 0.85, 'common', false),
('CHRISTINE', 'TINA', 'diminutive', 0.85, 'common', false),
('CHRISTINA', 'CHRIS', 'diminutive', 0.90, 'common', false),
('CHRISTINA', 'TINA', 'diminutive', 0.90, 'common', false),
('CHRISTINE', 'CHRISTINA', 'alternate', 0.95, 'very_common', true),

-- Kathleen variations
('KATHLEEN', 'KATHY', 'nickname', 0.95, 'very_common', false),
('KATHLEEN', 'KATE', 'diminutive', 0.85, 'common', false),
('KATHLEEN', 'KATIE', 'nickname', 0.85, 'common', false),
('KATHLEEN', 'KAT', 'diminutive', 0.80, 'uncommon', false),
('KATHLEEN', 'KITTY', 'nickname', 0.70, 'uncommon', false),

-- Pamela variations
('PAMELA', 'PAM', 'diminutive', 0.95, 'very_common', false),
('PAMELA', 'PAMMY', 'nickname', 0.80, 'uncommon', false),

-- Angela variations
('ANGELA', 'ANGIE', 'nickname', 0.95, 'very_common', false),
('ANGELA', 'ANGEL', 'diminutive', 0.80, 'uncommon', false),

-- Kimberly variations
('KIMBERLY', 'KIM', 'diminutive', 0.95, 'very_common', false),
('KIMBERLY', 'KIMMY', 'nickname', 0.80, 'uncommon', false),

-- Michelle variations
('MICHELLE', 'SHELLY', 'nickname', 0.85, 'common', false),
('MICHELLE', 'SHELLEY', 'nickname', 0.85, 'common', false),

-- Laura variations
('LAURA', 'LAURIE', 'nickname', 0.85, 'common', false),
('LAURA', 'LORI', 'alternate', 0.80, 'common', false),
('LAURIE', 'LORI', 'alternate', 0.90, 'common', true),

-- Teresa variations
('TERESA', 'TERRY', 'nickname', 0.85, 'common', false),
('TERESA', 'TERRI', 'nickname', 0.85, 'common', false),
('TERESA', 'TESS', 'diminutive', 0.75, 'uncommon', false),
('TERESA', 'THERESA', 'alternate', 0.95, 'very_common', true),

-- Victoria variations
('VICTORIA', 'VICKY', 'nickname', 0.95, 'very_common', false),
('VICTORIA', 'VICKI', 'nickname', 0.95, 'very_common', false),
('VICTORIA', 'VIC', 'diminutive', 0.75, 'uncommon', false),
('VICTORIA', 'TORI', 'nickname', 0.85, 'common', false),

-- Jacqueline variations
('JACQUELINE', 'JACKIE', 'nickname', 0.95, 'very_common', false),
('JACQUELINE', 'JACQUI', 'nickname', 0.85, 'common', false),
('JACQUELINE', 'JACK', 'diminutive', 0.60, 'rare', false),

-- Common international/cultural variations
('JOHN', 'JUAN', 'cultural', 0.80, 'common', false),
('JOHN', 'JEAN', 'cultural', 0.70, 'uncommon', false),
('JOHN', 'GIOVANNI', 'cultural', 0.70, 'uncommon', false),
('JOHN', 'IVAN', 'cultural', 0.70, 'uncommon', false),
('JOHN', 'IAN', 'cultural', 0.75, 'common', false),
('JOHN', 'SEAN', 'cultural', 0.80, 'common', false),

('PETER', 'PETE', 'diminutive', 0.95, 'very_common', false),
('PETER', 'PEDRO', 'cultural', 0.75, 'uncommon', false),
('PETER', 'PIERRE', 'cultural', 0.70, 'uncommon', false),

('PAUL', 'PABLO', 'cultural', 0.75, 'uncommon', false),
('PAUL', 'PAOLO', 'cultural', 0.70, 'uncommon', false),

('MICHAEL', 'MIGUEL', 'cultural', 0.75, 'uncommon', false),
('MICHAEL', 'MICHEL', 'cultural', 0.70, 'uncommon', false),
('MICHAEL', 'MIKHAIL', 'cultural', 0.70, 'uncommon', false),

-- Common spelling variations
('ERIC', 'ERIK', 'alternate', 0.95, 'very_common', true),
('BRYAN', 'BRIAN', 'alternate', 0.95, 'very_common', true),
('ALAN', 'ALLAN', 'alternate', 0.95, 'common', true),
('ALAN', 'ALLEN', 'alternate', 0.90, 'common', true),
('ALLAN', 'ALLEN', 'alternate', 0.90, 'common', true),
('CRAIG', 'GREG', 'alternate', 0.70, 'uncommon', false),
('MARC', 'MARK', 'alternate', 0.95, 'very_common', true),
('CARL', 'KARL', 'alternate', 0.90, 'common', true),
('TERRY', 'TERRI', 'alternate', 0.90, 'common', true),
('ROBIN', 'ROBYN', 'alternate', 0.90, 'common', true),
('LINDSAY', 'LINDSEY', 'alternate', 0.95, 'very_common', true),
('SYDNEY', 'SIDNEY', 'alternate', 0.95, 'common', true),
('GEOFFREY', 'JEFFREY', 'alternate', 0.85, 'common', true),
('SARA', 'SARAH', 'alternate', 0.95, 'very_common', true),
('MICHELE', 'MICHELLE', 'alternate', 0.95, 'very_common', true)

ON CONFLICT (canonical_name, alias_name) DO UPDATE 
SET confidence = EXCLUDED.confidence,
    usage_frequency = EXCLUDED.usage_frequency;

-- Show statistics after expansion
SELECT 'Name alias expansion complete:' as info;
SELECT 
    COUNT(DISTINCT canonical_name) as unique_names,
    COUNT(*) as total_aliases,
    COUNT(*) FILTER (WHERE alias_type = 'nickname') as nicknames,
    COUNT(*) FILTER (WHERE alias_type = 'diminutive') as diminutives,
    COUNT(*) FILTER (WHERE alias_type = 'alternate') as alternates,
    COUNT(*) FILTER (WHERE alias_type = 'cultural') as cultural,
    COUNT(*) FILTER (WHERE alias_type = 'formal') as formal
FROM name_aliases;

-- Test the enhanced system with the requested names
SELECT 'Testing SHAWN/SEAN variations:' as test;
SELECT * FROM get_name_variations('SHAWN');

SELECT 'Testing PATRICIA variations:' as test;
SELECT * FROM get_name_variations('PAT') 
WHERE name_variant IN ('PATRICIA', 'PAT', 'PATTY', 'PATTI', 'PATTIE', 'TRISH', 'TRISHA')
ORDER BY confidence DESC;

-- Count how many of each name we have
SELECT 'Count of Shawn/Sean/Shaun in database:' as test;
SELECT 
    attribute_value,
    COUNT(*) as count
FROM attributes 
WHERE attribute_type = 'computed_first_name'
  AND UPPER(attribute_value) IN ('SHAWN', 'SEAN', 'SHAUN')
GROUP BY attribute_value;

SELECT 'Count of Patricia variations in database:' as test;
SELECT 
    attribute_value,
    COUNT(*) as count
FROM attributes 
WHERE attribute_type = 'computed_first_name'
  AND UPPER(attribute_value) IN ('PATRICIA', 'PAT', 'PATTY', 'PATTI', 'PATTIE', 'TRISH', 'TRISHA')
GROUP BY attribute_value;

SELECT 'Alias system now contains ' || COUNT(*) || ' name mappings' as status
FROM name_aliases;