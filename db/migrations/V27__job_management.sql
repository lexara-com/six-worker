-- =============================================
-- V27: Job Management System for Distributed Loaders
-- =============================================
-- Creates tables for job orchestration, worker registry,
-- and data quality tracking in distributed loader environment
-- =============================================

-- Job Queue Table
-- Written by: Cloudflare Queue Consumer
-- Read by: Cloudflare Coordinator (via Hyperdrive), Python Workers (direct)
CREATE TABLE job_queue (
    job_id VARCHAR(26) PRIMARY KEY,
    job_type VARCHAR(50) NOT NULL,           -- 'iowa_business', 'iowa_asbestos', etc.
    status VARCHAR(20) NOT NULL,             -- 'pending', 'claimed', 'running', 'completed', 'failed'
    worker_id VARCHAR(100),                  -- Which worker claimed this job

    -- Job configuration and state
    config JSONB NOT NULL,                   -- Job configuration (loader config, file path, etc.)
    checkpoint JSONB,                        -- Progress checkpoint (for resume)

    -- Timing
    created_at TIMESTAMP DEFAULT NOW(),
    claimed_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),

    -- Error handling
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    error_message TEXT,

    -- Metadata
    metadata JSONB,                          -- Additional job-specific data

    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('pending', 'claimed', 'running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT valid_retry CHECK (retry_count >= 0 AND retry_count <= max_retries)
);

-- Worker Registry Table
-- Tracks active workers and their capabilities
CREATE TABLE workers (
    worker_id VARCHAR(100) PRIMARY KEY,
    hostname VARCHAR(255),
    ip_address VARCHAR(45),

    -- Worker capabilities
    capabilities JSONB NOT NULL,             -- ['iowa_business', 'iowa_asbestos']

    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'idle',  -- 'active', 'idle', 'offline', 'error'
    last_heartbeat TIMESTAMP,

    -- Worker metadata
    metadata JSONB,                          -- CPU, memory, disk space, etc.
    version VARCHAR(50),                     -- Worker software version

    -- Timing
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT valid_worker_status CHECK (status IN ('active', 'idle', 'offline', 'error'))
);

-- Data Quality Issues Table
-- Tracks validation errors and data anomalies discovered during loading
CREATE TABLE data_quality_issues (
    issue_id VARCHAR(26) PRIMARY KEY,
    job_id VARCHAR(26) REFERENCES job_queue(job_id) ON DELETE CASCADE,

    -- Issue identification
    source_record_id VARCHAR(100),           -- Corp number, license number, etc.
    issue_type VARCHAR(50) NOT NULL,         -- 'invalid_zip', 'missing_field', 'bad_date'
    severity VARCHAR(20) NOT NULL,           -- 'warning', 'error', 'critical'

    -- Issue details
    field_name VARCHAR(100),
    invalid_value TEXT,
    expected_format TEXT,
    message TEXT,
    raw_record JSONB,                        -- Full record for context

    -- Resolution tracking
    resolution_status VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'resolved', 'ignored', 'auto_fixed'
    resolution_action VARCHAR(50),           -- 'manual_fix', 'auto_fix', 'data_correction'
    resolution_notes TEXT,
    resolved_by VARCHAR(100),                -- User or system that resolved
    resolved_at TIMESTAMP,

    -- Timing
    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT valid_severity CHECK (severity IN ('warning', 'error', 'critical')),
    CONSTRAINT valid_resolution_status CHECK (resolution_status IN ('pending', 'resolved', 'ignored', 'auto_fixed'))
);

-- Job Logs Table (Optional - fallback if CloudWatch fails)
-- Stores execution logs for debugging
CREATE TABLE job_logs (
    log_id VARCHAR(26) PRIMARY KEY,
    job_id VARCHAR(26) REFERENCES job_queue(job_id) ON DELETE CASCADE,
    worker_id VARCHAR(100),

    -- Log entry
    timestamp TIMESTAMP DEFAULT NOW(),
    level VARCHAR(10) NOT NULL,              -- 'DEBUG', 'INFO', 'WARNING', 'ERROR'
    message TEXT NOT NULL,
    metadata JSONB,                          -- Structured log data

    CONSTRAINT valid_log_level CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))
);

-- =============================================
-- Indexes for Performance
-- =============================================

-- Job Queue Indexes
CREATE INDEX idx_job_queue_status ON job_queue(status, created_at)
    WHERE status IN ('pending', 'claimed', 'running');
CREATE INDEX idx_job_queue_worker ON job_queue(worker_id)
    WHERE worker_id IS NOT NULL;
CREATE INDEX idx_job_queue_type ON job_queue(job_type);
CREATE INDEX idx_job_queue_created ON job_queue(created_at DESC);

-- Worker Indexes
CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_heartbeat ON workers(last_heartbeat DESC);

-- Data Quality Indexes
CREATE INDEX idx_dq_issues_status ON data_quality_issues(resolution_status);
CREATE INDEX idx_dq_issues_job ON data_quality_issues(job_id);
CREATE INDEX idx_dq_issues_type ON data_quality_issues(issue_type);
CREATE INDEX idx_dq_issues_severity ON data_quality_issues(severity);
CREATE INDEX idx_dq_issues_created ON data_quality_issues(created_at DESC);

-- Job Logs Indexes
CREATE INDEX idx_job_logs_job ON job_logs(job_id, timestamp DESC);
CREATE INDEX idx_job_logs_level ON job_logs(level, timestamp DESC)
    WHERE level IN ('ERROR', 'CRITICAL');

-- =============================================
-- Partitioning for Job Logs (for high volume)
-- =============================================

-- Convert job_logs to partitioned table (by month)
-- Note: This would require recreating the table if it had data
-- Keeping as regular table for now, can partition later if needed

-- =============================================
-- Helper Functions
-- =============================================

-- Function to claim a job (atomic operation)
CREATE OR REPLACE FUNCTION claim_job(
    p_worker_id VARCHAR(100),
    p_capabilities JSONB
) RETURNS TABLE (
    job_id VARCHAR(26),
    job_type VARCHAR(50),
    config JSONB
) AS $$
DECLARE
    v_job_id VARCHAR(26);
BEGIN
    -- Find and claim a pending job that matches worker capabilities
    UPDATE job_queue
    SET
        status = 'claimed',
        worker_id = p_worker_id,
        claimed_at = NOW(),
        updated_at = NOW()
    WHERE job_queue.job_id = (
        SELECT jq.job_id
        FROM job_queue jq
        WHERE jq.status = 'pending'
        AND jq.job_type = ANY(
            SELECT jsonb_array_elements_text(p_capabilities)
        )
        ORDER BY jq.created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING job_queue.job_id INTO v_job_id;

    -- Return the claimed job
    IF v_job_id IS NOT NULL THEN
        RETURN QUERY
        SELECT
            jq.job_id,
            jq.job_type,
            jq.config
        FROM job_queue jq
        WHERE jq.job_id = v_job_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to update worker heartbeat
CREATE OR REPLACE FUNCTION update_worker_heartbeat(
    p_worker_id VARCHAR(100),
    p_metadata JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE workers
    SET
        last_heartbeat = NOW(),
        status = 'active',
        metadata = COALESCE(p_metadata, metadata),
        updated_at = NOW()
    WHERE worker_id = p_worker_id;

    -- If worker doesn't exist, create it
    IF NOT FOUND THEN
        INSERT INTO workers (worker_id, last_heartbeat, status, metadata, capabilities)
        VALUES (p_worker_id, NOW(), 'active', p_metadata, '[]'::jsonb)
        ON CONFLICT (worker_id) DO NOTHING;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to mark stale workers as offline
CREATE OR REPLACE FUNCTION mark_stale_workers_offline() RETURNS INT AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE workers
    SET status = 'offline', updated_at = NOW()
    WHERE status IN ('active', 'idle')
    AND last_heartbeat < NOW() - INTERVAL '5 minutes'
    RETURNING COUNT(*) INTO v_count;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function to fail stale jobs (worker went offline)
CREATE OR REPLACE FUNCTION fail_stale_jobs() RETURNS INT AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE job_queue
    SET
        status = 'failed',
        error_message = 'Worker became unresponsive',
        updated_at = NOW()
    WHERE status IN ('claimed', 'running')
    AND worker_id IN (
        SELECT worker_id FROM workers WHERE status = 'offline'
    )
    RETURNING COUNT(*) INTO v_count;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- Database User for Hyperdrive (Read-Only)
-- =============================================

-- Create read-only user for Cloudflare Hyperdrive
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'hyperdrive_reader') THEN
        CREATE USER hyperdrive_reader WITH PASSWORD 'CHANGE_ME_IN_PRODUCTION';
    END IF;
END $$;

-- Grant read-only access to job management tables
GRANT CONNECT ON DATABASE graph_db TO hyperdrive_reader;
GRANT USAGE ON SCHEMA public TO hyperdrive_reader;
GRANT SELECT ON job_queue, workers, data_quality_issues, job_logs TO hyperdrive_reader;

-- Explicitly deny write operations
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM hyperdrive_reader;

-- =============================================
-- Comments for Documentation
-- =============================================

COMMENT ON TABLE job_queue IS 'Distributed job queue for loader orchestration';
COMMENT ON TABLE workers IS 'Registry of active worker nodes';
COMMENT ON TABLE data_quality_issues IS 'Data validation errors and anomalies';
COMMENT ON TABLE job_logs IS 'Fallback logging when CloudWatch is unavailable';

COMMENT ON FUNCTION claim_job IS 'Atomically claims a pending job for a worker';
COMMENT ON FUNCTION update_worker_heartbeat IS 'Updates worker heartbeat and status';
COMMENT ON FUNCTION mark_stale_workers_offline IS 'Marks workers offline if no heartbeat for 5 minutes';
COMMENT ON FUNCTION fail_stale_jobs IS 'Fails jobs whose workers went offline';

-- =============================================
-- Initial Data / Setup
-- =============================================

-- Example: Create a maintenance cron job (requires pg_cron extension)
-- SELECT cron.schedule('mark-stale-workers', '*/5 * * * *', 'SELECT mark_stale_workers_offline()');
-- SELECT cron.schedule('fail-stale-jobs', '*/5 * * * *', 'SELECT fail_stale_jobs()');

RAISE NOTICE '';
RAISE NOTICE '✅ Job Management System Installed';
RAISE NOTICE '';
RAISE NOTICE '📋 Tables Created:';
RAISE NOTICE '  - job_queue (job orchestration)';
RAISE NOTICE '  - workers (worker registry)';
RAISE NOTICE '  - data_quality_issues (validation tracking)';
RAISE NOTICE '  - job_logs (fallback logging)';
RAISE NOTICE '';
RAISE NOTICE '🔧 Functions Created:';
RAISE NOTICE '  - claim_job() - Atomic job claiming';
RAISE NOTICE '  - update_worker_heartbeat() - Worker health tracking';
RAISE NOTICE '  - mark_stale_workers_offline() - Cleanup offline workers';
RAISE NOTICE '  - fail_stale_jobs() - Fail jobs from dead workers';
RAISE NOTICE '';
RAISE NOTICE '👤 Users Created:';
RAISE NOTICE '  - hyperdrive_reader (read-only for Cloudflare)';
RAISE NOTICE '';
RAISE NOTICE '⚠️  IMPORTANT: Change hyperdrive_reader password in production!';
RAISE NOTICE '';
