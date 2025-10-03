-- =============================================
-- Add ZipCode as a valid node type
-- =============================================

-- Drop the existing constraint
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_node_type_check;

-- Add the updated constraint including ZipCode
ALTER TABLE nodes ADD CONSTRAINT nodes_node_type_check 
CHECK (node_type::text = ANY (ARRAY[
    'Person'::character varying, 
    'Company'::character varying, 
    'State'::character varying, 
    'City'::character varying, 
    'County'::character varying, 
    'Country'::character varying, 
    'Address'::character varying, 
    'ZipCode'::character varying,  -- Added
    'Thing'::character varying, 
    'Event'::character varying
]::text[]));

-- Verify the constraint was updated
SELECT conname, pg_get_constraintdef(oid) as definition
FROM pg_constraint
WHERE conrelid = 'nodes'::regclass 
  AND conname = 'nodes_node_type_check';