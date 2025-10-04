#!/bin/bash
# Start a distributed worker for testing

set -e

# Configuration
COORDINATOR_URL="https://lexara-coordinator-prod.cloudswift.workers.dev"
WORKER_ID="${WORKER_ID:-test-worker-$(hostname)-$$}"
CAPABILITIES="${CAPABILITIES:-iowa_business iowa_asbestos}"

# Database connection (fallback to local dev environment)
export DB_HOST="${DB_HOST:-98.85.51.253}"
export DB_USER="${DB_USER:-graph_admin}"
export DB_PASSWORD="${DB_PASSWORD:-DevPassword123!}"
export DB_NAME="${DB_NAME:-graph_db}"
export DB_PORT="${DB_PORT:-5432}"
export ENVIRONMENT="${ENVIRONMENT:-development}"

# Change to project root
cd "$(dirname "$0")"

echo "🚀 Starting Distributed Worker"
echo "================================"
echo "Worker ID: $WORKER_ID"
echo "Coordinator: $COORDINATOR_URL"
echo "Capabilities: $CAPABILITIES"
echo "Database: $DB_HOST:$DB_PORT/$DB_NAME"
echo "================================"
echo

# Run the worker
python3 src/loaders/distributed_worker.py \
    --coordinator-url "$COORDINATOR_URL" \
    --worker-id "$WORKER_ID" \
    --capabilities $CAPABILITIES
