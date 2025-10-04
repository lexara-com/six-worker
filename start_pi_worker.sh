#!/bin/bash
# Six Worker - Raspberry Pi Startup Script
# This script starts the distributed worker with AWS Secrets Manager integration

set -e

# Configuration
export ENVIRONMENT=production
export AWS_DEFAULT_REGION=us-east-1
export AWS_PROFILE=six-worker-pi
export AWS_ROLE_ARN=arn:aws:iam::561107861343:role/six-worker

# Coordinator URL
COORDINATOR_URL="https://lexara-coordinator-prod.cloudswift.workers.dev"

# Worker ID (defaults to hostname-based if not provided)
WORKER_ID="${WORKER_ID:-pi-worker-$(hostname)}"

# Capabilities - add or remove as needed
CAPABILITIES=(
  iowa_motor_vehicle_service
  iowa_business
  iowa_asbestos
)

# Start the worker
echo "🚀 Starting Six Worker..."
echo "   Worker ID: $WORKER_ID"
echo "   Environment: $ENVIRONMENT"
echo "   Capabilities: ${CAPABILITIES[*]}"
echo ""

python3 src/loaders/distributed_worker.py \
  --coordinator-url "$COORDINATOR_URL" \
  --worker-id "$WORKER_ID" \
  --capabilities ${CAPABILITIES[@]}
