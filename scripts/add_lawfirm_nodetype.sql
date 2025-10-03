-- =============================================
-- Add LawFirm as a valid node type
-- =============================================

-- Drop the existing constraint
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_node_type_check;

-- Add the updated constraint including LawFirm
ALTER TABLE nodes ADD CONSTRAINT nodes_node_type_check 
CHECK (node_type::text = ANY (ARRAY[
    'Person'::character varying, 
    'Company'::character varying,
    'LawFirm'::character varying,  -- Added
    'State'::character varying, 
    'City'::character varying, 
    'County'::character varying, 
    'Country'::character varying, 
    'Address'::character varying, 
    'ZipCode'::character varying,
    'Thing'::character varying, 
    'Event'::character varying
]::text[]));

-- Verify the constraint was updated
SELECT 'Node type constraint updated. Valid types:' as status;
SELECT unnest(ARRAY[
    'Person', 'Company', 'LawFirm', 'State', 'City', 
    'County', 'Country', 'Address', 'ZipCode', 'Thing', 'Event'
]) as valid_node_types
ORDER BY valid_node_types;