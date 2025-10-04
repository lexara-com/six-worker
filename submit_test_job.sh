#!/bin/bash
# Submit a test job to the coordinator

set -e

COORDINATOR_URL="https://lexara-coordinator-prod.cloudswift.workers.dev"

# Check if data file exists
DATA_FILE="examples/data/Active_Iowa_Business_Entities_20251001.csv"
if [ ! -f "$DATA_FILE" ]; then
    echo "❌ Data file not found: $DATA_FILE"
    echo "Please ensure the data file exists before submitting a job"
    exit 1
fi

# Get absolute path
ABS_PATH=$(cd "$(dirname "$DATA_FILE")" && pwd)/$(basename "$DATA_FILE")

echo "📤 Submitting test job to coordinator"
echo "======================================"
echo "Coordinator: $COORDINATOR_URL"
echo "Job Type: iowa_business"
echo "Data File: $ABS_PATH"
echo "======================================"
echo

# Submit job
curl -X POST "$COORDINATOR_URL/jobs/submit" \
  -H "Content-Type: application/json" \
  -d "{
    \"job_type\": \"iowa_business\",
    \"config\": {
      \"input\": {
        \"file_path\": \"$ABS_PATH\",
        \"format\": \"csv\",
        \"encoding\": \"utf-8\"
      },
      \"processing\": {
        \"batch_size\": 100,
        \"limit\": 10,
        \"checkpoint_interval\": 5
      },
      \"source\": {
        \"name\": \"Iowa Secretary of State - Active Business Entities\",
        \"type\": \"iowa_gov_database\"
      }
    }
  }" | jq '.'

echo
echo "✅ Job submitted successfully"
echo
echo "To check job status:"
echo "  curl $COORDINATOR_URL/jobs?status=pending"
echo
echo "To start a worker:"
echo "  ./start_worker.sh"
