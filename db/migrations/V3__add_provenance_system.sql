-- =============================================
-- Aurora PostgreSQL Provenance System
-- Version: 3.0
-- Purpose: Add comprehensive provenance tracking for all data sources and changes
-- =============================================

-- =============================================
-- Provenance Tables
-- =============================================

-- Main provenance table - tracks sources for all assets
CREATE TABLE provenance (
    provenance_id VARCHAR(26) PRIMARY KEY,
    
    -- What asset this provenance record refers to
    asset_type VARCHAR(20) NOT NULL CHECK (asset_type IN ('node', 'relationship', 'attribute')),
    asset_id VARCHAR(26) NOT NULL,
    
    -- Source information
    source_name VARCHAR(255) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    source_url VARCHAR(500),
    source_license VARCHAR(100),
    
    -- Data quality and reliability
    confidence_score DECIMAL(3,2) DEFAULT 1.0 CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    reliability_rating VARCHAR(20) DEFAULT 'unknown' CHECK (reliability_rating IN ('high', 'medium', 'low', 'unknown')),
    
    -- Temporal information
    data_obtained_at TIMESTAMP,
    data_valid_from DATE,
    data_valid_to DATE,
    
    -- Record management
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'disputed', 'superseded')),
    
    -- Additional metadata
    metadata JSONB,
    notes TEXT,
    
    -- Indexes for efficient lookups
    CONSTRAINT provenance_valid_period CHECK (data_valid_to IS NULL OR data_valid_to >= data_valid_from)
);

-- Change history table - tracks all modifications to core data
CREATE TABLE change_history (
    change_id VARCHAR(26) PRIMARY KEY,
    
    -- What was changed
    table_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(26) NOT NULL,
    field_name VARCHAR(50) NOT NULL,
    
    -- Change details
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    old_value TEXT,
    new_value TEXT,
    
    -- Change metadata
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100),
    change_reason VARCHAR(255),
    
    -- Reference to source
    provenance_id VARCHAR(26),
    
    CONSTRAINT fk_change_provenance FOREIGN KEY (provenance_id) REFERENCES provenance(provenance_id)
);

-- Source types lookup table
CREATE TABLE source_types (
    source_type VARCHAR(50) PRIMARY KEY,
    description TEXT,
    default_reliability VARCHAR(20) DEFAULT 'medium',
    requires_license BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- Indexes for Performance
-- =============================================

-- Provenance indexes
CREATE INDEX idx_provenance_asset ON provenance (asset_type, asset_id);
CREATE INDEX idx_provenance_source ON provenance (source_type, source_name);
CREATE INDEX idx_provenance_confidence ON provenance (confidence_score DESC);
CREATE INDEX idx_provenance_obtained ON provenance (data_obtained_at DESC);
CREATE INDEX idx_provenance_status ON provenance (status, asset_type);

-- Change history indexes
CREATE INDEX idx_change_history_record ON change_history (table_name, record_id);
CREATE INDEX idx_change_history_time ON change_history (changed_at DESC);
CREATE INDEX idx_change_history_field ON change_history (table_name, field_name);
CREATE INDEX idx_change_history_operation ON change_history (operation, changed_at DESC);

-- =============================================
-- Helper Functions
-- =============================================

-- Function to generate ULID (reusable across triggers)
CREATE OR REPLACE FUNCTION generate_ulid() 
RETURNS VARCHAR(26) AS $$
DECLARE
    timestamp_part VARCHAR(10);
    random_part VARCHAR(16);
    chars VARCHAR(32) := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    ms BIGINT;
    i INT;
BEGIN
    -- Get timestamp in milliseconds
    ms := EXTRACT(epoch FROM CURRENT_TIMESTAMP) * 1000;
    
    -- Convert timestamp to Crockford Base32
    timestamp_part := '';
    FOR i IN 1..10 LOOP
        timestamp_part := SUBSTRING(chars, (ms % 32) + 1, 1) || timestamp_part;
        ms := ms / 32;
    END LOOP;
    
    -- Generate random part
    random_part := '';
    FOR i IN 1..16 LOOP
        random_part := random_part || SUBSTRING(chars, (RANDOM() * 31)::INT + 1, 1);
    END LOOP;
    
    RETURN timestamp_part || random_part;
END;
$$ LANGUAGE plpgsql;

-- Function to create provenance record
CREATE OR REPLACE FUNCTION create_provenance_record(
    p_asset_type VARCHAR(20),
    p_asset_id VARCHAR(26),
    p_source_name VARCHAR(255),
    p_source_type VARCHAR(50),
    p_created_by VARCHAR(100) DEFAULT NULL,
    p_confidence DECIMAL(3,2) DEFAULT 1.0
) RETURNS VARCHAR(26) AS $$
DECLARE
    provenance_id VARCHAR(26);
BEGIN
    provenance_id := generate_ulid();
    
    INSERT INTO provenance (
        provenance_id, asset_type, asset_id, source_name, source_type,
        confidence_score, created_by, data_obtained_at
    ) VALUES (
        provenance_id, p_asset_type, p_asset_id, p_source_name, p_source_type,
        p_confidence, p_created_by, CURRENT_TIMESTAMP
    );
    
    RETURN provenance_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log changes
CREATE OR REPLACE FUNCTION log_change(
    p_table_name VARCHAR(50),
    p_record_id VARCHAR(26),
    p_field_name VARCHAR(50),
    p_operation VARCHAR(10),
    p_old_value TEXT DEFAULT NULL,
    p_new_value TEXT DEFAULT NULL,
    p_changed_by VARCHAR(100) DEFAULT NULL,
    p_provenance_id VARCHAR(26) DEFAULT NULL
) RETURNS VARCHAR(26) AS $$
DECLARE
    change_id VARCHAR(26);
BEGIN
    change_id := generate_ulid();
    
    INSERT INTO change_history (
        change_id, table_name, record_id, field_name, operation,
        old_value, new_value, changed_by, provenance_id
    ) VALUES (
        change_id, p_table_name, p_record_id, p_field_name, p_operation,
        p_old_value, p_new_value, p_changed_by, p_provenance_id
    );
    
    RETURN change_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- Change Tracking Triggers
-- =============================================

-- Trigger function for nodes table
CREATE OR REPLACE FUNCTION trigger_nodes_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM log_change('nodes', NEW.node_id, 'node_type', 'INSERT', NULL, NEW.node_type, NEW.created_by);
        PERFORM log_change('nodes', NEW.node_id, 'primary_name', 'INSERT', NULL, NEW.primary_name, NEW.created_by);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.node_type != NEW.node_type THEN
            PERFORM log_change('nodes', NEW.node_id, 'node_type', 'UPDATE', OLD.node_type, NEW.node_type, NEW.created_by);
        END IF;
        IF OLD.primary_name != NEW.primary_name THEN
            PERFORM log_change('nodes', NEW.node_id, 'primary_name', 'UPDATE', OLD.primary_name, NEW.primary_name, NEW.created_by);
        END IF;
        IF OLD.status != NEW.status THEN
            PERFORM log_change('nodes', NEW.node_id, 'status', 'UPDATE', OLD.status, NEW.status, NEW.created_by);
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM log_change('nodes', OLD.node_id, 'status', 'DELETE', OLD.status, 'deleted', OLD.created_by);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for relationships table
CREATE OR REPLACE FUNCTION trigger_relationships_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM log_change('relationships', NEW.relationship_id, 'relationship_type', 'INSERT', NULL, NEW.relationship_type, NEW.created_by);
        PERFORM log_change('relationships', NEW.relationship_id, 'strength', 'INSERT', NULL, NEW.strength::TEXT, NEW.created_by);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.relationship_type != NEW.relationship_type THEN
            PERFORM log_change('relationships', NEW.relationship_id, 'relationship_type', 'UPDATE', OLD.relationship_type, NEW.relationship_type, NEW.created_by);
        END IF;
        IF OLD.strength != NEW.strength THEN
            PERFORM log_change('relationships', NEW.relationship_id, 'strength', 'UPDATE', OLD.strength::TEXT, NEW.strength::TEXT, NEW.created_by);
        END IF;
        IF OLD.status != NEW.status THEN
            PERFORM log_change('relationships', NEW.relationship_id, 'status', 'UPDATE', OLD.status, NEW.status, NEW.created_by);
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM log_change('relationships', OLD.relationship_id, 'status', 'DELETE', OLD.status, 'deleted', OLD.created_by);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for attributes table
CREATE OR REPLACE FUNCTION trigger_attributes_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM log_change('attributes', NEW.attribute_id, 'attribute_value', 'INSERT', NULL, NEW.attribute_value, NEW.created_by);
        PERFORM log_change('attributes', NEW.attribute_id, 'confidence', 'INSERT', NULL, NEW.confidence::TEXT, NEW.created_by);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.attribute_value != NEW.attribute_value THEN
            PERFORM log_change('attributes', NEW.attribute_id, 'attribute_value', 'UPDATE', OLD.attribute_value, NEW.attribute_value, NEW.created_by);
        END IF;
        IF OLD.confidence != NEW.confidence THEN
            PERFORM log_change('attributes', NEW.attribute_id, 'confidence', 'UPDATE', OLD.confidence::TEXT, NEW.confidence::TEXT, NEW.created_by);
        END IF;
        IF OLD.status != NEW.status THEN
            PERFORM log_change('attributes', NEW.attribute_id, 'status', 'UPDATE', OLD.status, NEW.status, NEW.created_by);
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM log_change('attributes', OLD.attribute_id, 'status', 'DELETE', OLD.status, 'deleted', OLD.created_by);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE TRIGGER trg_nodes_changes
    AFTER INSERT OR UPDATE OR DELETE ON nodes
    FOR EACH ROW EXECUTE FUNCTION trigger_nodes_changes();

CREATE TRIGGER trg_relationships_changes
    AFTER INSERT OR UPDATE OR DELETE ON relationships
    FOR EACH ROW EXECUTE FUNCTION trigger_relationships_changes();

CREATE TRIGGER trg_attributes_changes
    AFTER INSERT OR UPDATE OR DELETE ON attributes
    FOR EACH ROW EXECUTE FUNCTION trigger_attributes_changes();

-- =============================================
-- Reference Data for Source Types
-- =============================================

INSERT INTO source_types (source_type, description, default_reliability, requires_license) VALUES
-- Legal and Official Sources
('court_records', 'Official court documents and filings', 'high', TRUE),
('bar_association', 'State bar association records', 'high', TRUE),
('sec_filings', 'Securities and Exchange Commission documents', 'high', FALSE),
('incorporation_docs', 'Corporate incorporation and registration documents', 'high', TRUE),
('public_records', 'Government public records and databases', 'high', FALSE),

-- Professional Sources
('law_firm_records', 'Internal law firm client and case records', 'high', FALSE),
('client_intake', 'Information provided during client intake process', 'medium', FALSE),
('business_cards', 'Professional business cards and contact information', 'medium', FALSE),
('letterhead', 'Official letterhead and corporate communications', 'medium', FALSE),
('contracts', 'Legal contracts and agreements', 'high', TRUE),

-- Public Information Sources
('linkedin', 'LinkedIn professional profiles', 'medium', TRUE),
('company_websites', 'Official corporate websites', 'medium', FALSE),
('press_releases', 'Corporate press releases and announcements', 'medium', FALSE),
('news_articles', 'News media reports and articles', 'low', TRUE),
('social_media', 'Social media profiles and posts', 'low', TRUE),

-- Research and Database Sources
('legal_databases', 'Professional legal research databases', 'high', TRUE),
('commercial_databases', 'Commercial data aggregation services', 'medium', TRUE),
('investigative_research', 'Professional investigative research', 'medium', FALSE),
('background_checks', 'Professional background verification services', 'high', TRUE),

-- Internal Sources
('manual_entry', 'Manually entered information', 'medium', FALSE),
('data_migration', 'Migrated from previous systems', 'medium', FALSE),
('generated', 'System-generated or computed data', 'low', FALSE),
('bulk_import', 'Bulk data import operations', 'low', FALSE);

-- =============================================
-- Views for Common Provenance Queries
-- =============================================

-- View to see all sources for an asset
CREATE VIEW asset_provenance AS
SELECT 
    p.asset_type,
    p.asset_id,
    CASE 
        WHEN p.asset_type = 'node' THEN n.primary_name
        WHEN p.asset_type = 'relationship' THEN r.relationship_type
        WHEN p.asset_type = 'attribute' THEN a.attribute_type || ': ' || a.attribute_value
    END as asset_description,
    p.source_name,
    p.source_type,
    st.description as source_description,
    p.confidence_score,
    p.reliability_rating,
    p.data_obtained_at,
    p.status as provenance_status
FROM provenance p
LEFT JOIN source_types st ON p.source_type = st.source_type
LEFT JOIN nodes n ON p.asset_type = 'node' AND p.asset_id = n.node_id
LEFT JOIN relationships r ON p.asset_type = 'relationship' AND p.asset_id = r.relationship_id
LEFT JOIN attributes a ON p.asset_type = 'attribute' AND p.asset_id = a.attribute_id;

-- View to see change history with context
CREATE VIEW asset_changes AS
SELECT 
    ch.change_id,
    ch.table_name,
    ch.record_id,
    ch.field_name,
    ch.operation,
    ch.old_value,
    ch.new_value,
    ch.changed_at,
    ch.changed_by,
    p.source_name,
    p.source_type
FROM change_history ch
LEFT JOIN provenance p ON ch.provenance_id = p.provenance_id
ORDER BY ch.changed_at DESC;

-- =============================================
-- Comments and Documentation
-- =============================================

COMMENT ON TABLE provenance IS 'Tracks sources and provenance for all nodes, relationships, and attributes';
COMMENT ON TABLE change_history IS 'Complete audit trail of all changes to core data tables';
COMMENT ON TABLE source_types IS 'Reference table defining valid source types and their characteristics';

COMMENT ON COLUMN provenance.asset_type IS 'Type of asset: node, relationship, or attribute';
COMMENT ON COLUMN provenance.asset_id IS 'ULID of the specific asset this provenance refers to';
COMMENT ON COLUMN provenance.source_name IS 'Specific name/identifier of the source';
COMMENT ON COLUMN provenance.source_type IS 'Category of source (e.g., court_records, linkedin)';
COMMENT ON COLUMN provenance.confidence_score IS 'Confidence in this source (0.0-1.0)';
COMMENT ON COLUMN provenance.reliability_rating IS 'General reliability of this source type';

COMMENT ON COLUMN change_history.operation IS 'Type of change: INSERT, UPDATE, or DELETE';
COMMENT ON COLUMN change_history.old_value IS 'Previous value before change (NULL for INSERT)';
COMMENT ON COLUMN change_history.new_value IS 'New value after change (NULL for DELETE)';

-- Success message
SELECT 'Provenance system installed successfully' as status;