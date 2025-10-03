#!/bin/bash
# =============================================================================
# Iowa Asbestos Licenses - Data Download Script
# =============================================================================
# Downloads the latest Active Iowa Asbestos Licenses dataset from data.iowa.gov
# Dataset: https://data.iowa.gov/Workforce/Active-Iowa-Asbestos-Licenses/c9cg-ivvu
#
# Usage: ./download_data.sh [--format csv|json]
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${JOB_DIR}/data"
DATASET_ID="c9cg-ivvu"
BASE_URL="https://data.iowa.gov/resource/${DATASET_ID}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Default format
FORMAT="${1:-csv}"
if [[ "$1" == "--format" ]]; then
    FORMAT="$2"
fi

# Create data directory if it doesn't exist
mkdir -p "$DATA_DIR"

# Generate filename with timestamp
TIMESTAMP=$(date +%Y%m%d)
OUTPUT_FILE="${DATA_DIR}/Active_Iowa_Asbestos_Licenses_${TIMESTAMP}.${FORMAT}"

echo "================================================================"
echo "Iowa Asbestos Licenses Data Download"
echo "================================================================"
echo "Dataset ID: ${DATASET_ID}"
echo "Format: ${FORMAT}"
echo "Output: ${OUTPUT_FILE}"
echo "================================================================"

# Function to download data
download_data() {
    local format=$1
    local output=$2
    
    echo -e "${GREEN}Downloading data...${NC}"
    
    if [[ "$format" == "csv" ]]; then
        # Download CSV format using export endpoint
        curl -L -o "$output" \
            "https://data.iowa.gov/api/views/${DATASET_ID}/rows.csv?accessType=DOWNLOAD" \
            --progress-bar
    elif [[ "$format" == "json" ]]; then
        # Download JSON format with all records
        # Note: Socrata API has a default limit, so we need to get all records
        echo "Getting record count..."
        COUNT=$(curl -s "${BASE_URL}.json?\$select=count(*)" | python3 -c "import sys, json; print(json.load(sys.stdin)[0]['count'])")
        echo "Total records: $COUNT"
        
        echo "Downloading JSON data..."
        curl -L -o "$output" \
            "${BASE_URL}.json?\$limit=${COUNT}" \
            --progress-bar
    else
        echo -e "${RED}Error: Unknown format '$format'. Use 'csv' or 'json'${NC}"
        exit 1
    fi
}

# Check if file already exists
if [[ -f "$OUTPUT_FILE" ]]; then
    echo -e "${YELLOW}Warning: File already exists: ${OUTPUT_FILE}${NC}"
    read -p "Overwrite? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Download cancelled"
        exit 0
    fi
fi

# Download the data
download_data "$FORMAT" "$OUTPUT_FILE"

# Verify download
if [[ -f "$OUTPUT_FILE" ]]; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    
    if [[ "$FORMAT" == "csv" ]]; then
        # Count lines in CSV (subtract 1 for header)
        LINE_COUNT=$(($(wc -l < "$OUTPUT_FILE") - 1))
        echo -e "${GREEN}✓ Download complete${NC}"
        echo "  File: ${OUTPUT_FILE}"
        echo "  Size: ${FILE_SIZE}"
        echo "  Records: ${LINE_COUNT}"
        
        # Show sample
        echo ""
        echo "Sample data (first 5 records):"
        echo "--------------------------------"
        head -6 "$OUTPUT_FILE" | column -t -s','
        
    elif [[ "$FORMAT" == "json" ]]; then
        # Count records in JSON
        RECORD_COUNT=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_FILE'))))")
        echo -e "${GREEN}✓ Download complete${NC}"
        echo "  File: ${OUTPUT_FILE}"
        echo "  Size: ${FILE_SIZE}"
        echo "  Records: ${RECORD_COUNT}"
        
        # Show sample
        echo ""
        echo "Sample data (first record):"
        echo "--------------------------------"
        python3 -c "import json, pprint; data = json.load(open('$OUTPUT_FILE')); pprint.pprint(data[0] if data else 'No data')"
    fi
    
    # Get metadata
    echo ""
    echo "Dataset Metadata:"
    echo "--------------------------------"
    curl -s "https://data.iowa.gov/api/views/${DATASET_ID}.json" | python3 -c "
import sys, json
from datetime import datetime

data = json.load(sys.stdin)
print(f\"Name: {data.get('name', 'N/A')}\")
print(f\"Description: {data.get('description', 'N/A')}\")
print(f\"Category: {data.get('category', 'N/A')}\")
print(f\"Attribution: {data.get('attribution', 'N/A')}\")

# Convert timestamps
created = data.get('createdAt')
updated = data.get('rowsUpdatedAt')
if created:
    created_dt = datetime.fromtimestamp(created)
    print(f\"Created: {created_dt.strftime('%Y-%m-%d %H:%M:%S')}\")
if updated:
    updated_dt = datetime.fromtimestamp(updated)
    print(f\"Last Updated: {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}\")

# Column info
cols = data.get('columns', [])
if cols:
    print(f\"\nColumns ({len(cols)}):\")
    for c in cols:
        if not c['name'].startswith(':'):  # Skip system columns
            print(f\"  - {c['name']}: {c.get('dataTypeName', 'unknown')}\")
"
    
    echo ""
    echo -e "${GREEN}Ready to load!${NC}"
    echo "Next step: Run the loader with this file"
    echo "  ./scripts/run_asbestos_loader.sh"
    
else
    echo -e "${RED}Error: Download failed${NC}"
    exit 1
fi

echo "================================================================"