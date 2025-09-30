-- =============================================
-- Aurora PostgreSQL Graph Database Schema
-- Version: 1.0
-- Purpose: Initial schema for law firm conflict checking
-- =============================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy string matching

-- =============================================
-- Core Tables
-- =============================================

-- Nodes table: Core entities (Person, Company, Place, Thing, Event)
CREATE TABLE nodes (
    node_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_type VARCHAR(50) NOT NULL CHECK (node_type IN ('Person', 'Company', 'Place', 'Thing', 'Event')),
    primary_name VARCHAR(255) NOT NULL,
    normalized_name VARCHAR(255) NOT NULL, -- Lowercase, trimmed for searching
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    CONSTRAINT nodes_name_not_empty CHECK (LENGTH(TRIM(primary_name)) > 0)
);

-- Relationships table: Directed edges between nodes
CREATE TABLE relationships (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_node_id UUID NOT NULL,
    target_node_id UUID NOT NULL,
    relationship_type VARCHAR(50) NOT NULL,
    strength DECIMAL(3,2) DEFAULT 1.0 CHECK (strength BETWEEN 0.0 AND 1.0),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    metadata JSONB,
    
    CONSTRAINT fk_relationships_source FOREIGN KEY (source_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT fk_relationships_target FOREIGN KEY (target_node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT relationships_no_self_reference CHECK (source_node_id != target_node_id),
    CONSTRAINT relationships_valid_period CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

-- Attributes table: Metadata, aliases, and supplemental information
CREATE TABLE attributes (
    attribute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_id UUID NOT NULL,
    attribute_type VARCHAR(50) NOT NULL,
    attribute_value VARCHAR(500) NOT NULL,
    normalized_value VARCHAR(500) NOT NULL, -- For searching
    confidence DECIMAL(3,2) DEFAULT 1.0 CHECK (confidence BETWEEN 0.0 AND 1.0),
    source VARCHAR(100), -- Where this attribute came from
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'deleted')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    CONSTRAINT fk_attributes_node FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT attributes_value_not_empty CHECK (LENGTH(TRIM(attribute_value)) > 0)
);

-- =============================================
-- Conflict Management Tables
-- =============================================

-- Pre-computed conflict matrix for performance
CREATE TABLE conflict_matrix (
    matrix_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_a_id UUID NOT NULL,
    entity_b_id UUID NOT NULL,
    conflict_type VARCHAR(50) NOT NULL,
    conflict_path JSONB NOT NULL, -- Array of relationship steps
    conflict_strength DECIMAL(3,2) NOT NULL CHECK (conflict_strength BETWEEN 0.0 AND 1.0),
    degrees_of_separation INTEGER NOT NULL CHECK (degrees_of_separation BETWEEN 1 AND 3),
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP, -- For cache invalidation
    
    CONSTRAINT fk_conflict_entity_a FOREIGN KEY (entity_a_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT fk_conflict_entity_b FOREIGN KEY (entity_b_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    CONSTRAINT conflict_matrix_no_self_conflict CHECK (entity_a_id != entity_b_id)
);

-- Audit trail for all conflict checks
CREATE TABLE conflict_checks (
    check_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    matter_id VARCHAR(100), -- External matter reference
    checked_entities JSONB NOT NULL, -- Array of entity names/ids that were checked
    conflicts_found JSONB, -- Array of conflicts detected
    check_parameters JSONB, -- Search parameters used
    execution_time_ms INTEGER,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checked_by VARCHAR(100),
    api_key_id VARCHAR(100)
);

-- =============================================
-- Lookup Tables for Reference Data
-- =============================================

-- Valid relationship types
CREATE TABLE relationship_types (
    type_name VARCHAR(50) PRIMARY KEY,
    description TEXT,
    category VARCHAR(50),
    is_bidirectional BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Valid attribute types
CREATE TABLE attribute_types (
    type_name VARCHAR(50) PRIMARY KEY,
    description TEXT,
    data_type VARCHAR(20) DEFAULT 'text' CHECK (data_type IN ('text', 'number', 'date', 'boolean', 'json')),
    is_searchable BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- Helper Functions
-- =============================================

-- Function to normalize names for consistent searching
CREATE OR REPLACE FUNCTION normalize_name(input_name TEXT) 
RETURNS TEXT AS $$
BEGIN
    RETURN LOWER(TRIM(REGEXP_REPLACE(input_name, '\s+', ' ', 'g')));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Trigger to automatically populate normalized_name
CREATE OR REPLACE FUNCTION update_normalized_name() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.normalized_name = normalize_name(NEW.primary_name);
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_nodes_normalize_name
    BEFORE INSERT OR UPDATE ON nodes
    FOR EACH ROW 
    EXECUTE FUNCTION update_normalized_name();

-- Trigger to automatically populate normalized_value for attributes
CREATE OR REPLACE FUNCTION update_normalized_attribute_value() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.normalized_value = normalize_name(NEW.attribute_value);
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_attributes_normalize_value
    BEFORE INSERT OR UPDATE ON attributes
    FOR EACH ROW 
    EXECUTE FUNCTION update_normalized_attribute_value();

-- =============================================
-- Initial Reference Data
-- =============================================

-- Insert standard relationship types
INSERT INTO relationship_types (type_name, description, category, is_bidirectional) VALUES
('Employment', 'Person works for Company', 'Professional', FALSE),
('Ownership', 'Entity owns another entity', 'Financial', FALSE),
('Partnership', 'Business partnership relationship', 'Professional', TRUE),
('Location', 'Entity is located at Place', 'Physical', FALSE),
('Participation', 'Entity participates in Event', 'Activity', FALSE),
('Organizer', 'Entity organizes Event', 'Activity', FALSE),
('Conflict', 'Adversarial relationship', 'Legal', TRUE),
('Family', 'Family relationship', 'Personal', TRUE),
('Board_Member', 'Person serves on Company board', 'Professional', FALSE),
('Legal_Counsel', 'Attorney represents Entity', 'Legal', FALSE),
('Opposing_Counsel', 'Attorney represents opposing party', 'Legal', FALSE),
('Subsidiary', 'Company is subsidiary of another', 'Corporate', FALSE),
('Client_Relationship', 'Professional service relationship', 'Legal', FALSE);

-- Insert standard attribute types  
INSERT INTO attribute_types (type_name, description, data_type, is_searchable) VALUES
('nameAlias', 'Alternative names and nicknames', 'text', TRUE),
('email', 'Email address', 'text', TRUE),
('phone', 'Phone number', 'text', TRUE),
('address', 'Physical address', 'text', TRUE),
('title', 'Professional title or role', 'text', TRUE),
('category', 'Entity category or classification', 'text', TRUE),
('status', 'Current status', 'text', TRUE),
('notes', 'Additional notes', 'text', TRUE),
('external_id', 'External system identifier', 'text', TRUE),
('website', 'Website URL', 'text', FALSE),
('founded_date', 'Company founding date', 'date', FALSE),
('birth_date', 'Person birth date', 'date', FALSE),
('metadata', 'Additional structured data', 'json', FALSE);