-- Simplified ULID function using standard PostgreSQL functions
CREATE OR REPLACE FUNCTION generate_ulid() 
RETURNS VARCHAR(26) AS $$
DECLARE
    timestamp_part VARCHAR(10);
    random_part VARCHAR(16);
    chars VARCHAR(32) := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    ms BIGINT;
    i INT;
    char_index INT;
BEGIN
    -- Get timestamp in milliseconds
    ms := EXTRACT(epoch FROM CURRENT_TIMESTAMP) * 1000;
    
    -- Convert timestamp to Crockford Base32
    timestamp_part := '';
    FOR i IN 1..10 LOOP
        char_index := (ms % 32) + 1;
        timestamp_part := substr(chars, char_index, 1) || timestamp_part;
        ms := ms / 32;
    END LOOP;
    
    -- Generate random part
    random_part := '';
    FOR i IN 1..16 LOOP
        char_index := floor(RANDOM() * 32)::INT + 1;
        random_part := random_part || substr(chars, char_index, 1);
    END LOOP;
    
    RETURN timestamp_part || random_part;
END;
$$ LANGUAGE plpgsql;