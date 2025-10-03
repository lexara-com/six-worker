-- Clear all test data to prepare for ULID migration
TRUNCATE TABLE conflict_matrix, attributes, relationships, nodes RESTART IDENTITY CASCADE;