#!/usr/bin/env python3
"""
Download Iowa Business Entities data from Iowa.gov
"""

import os
import sys
import logging
import urllib.request
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_iowa_data():
    """Download the Iowa business entities CSV from data.iowa.gov"""
    
    # URL for the Active Iowa Business Entities dataset (CSV export)
    url = "https://data.iowa.gov/api/views/ez5t-3qay/rows.csv?accessType=DOWNLOAD"
    
    # Create examples/data directory if it doesn't exist
    os.makedirs("examples/data", exist_ok=True)
    
    # Output filename with current date
    output_file = f"examples/data/Active_Iowa_Business_Entities_{datetime.now().strftime('%Y%m%d')}.csv"
    
    logger.info(f"Downloading Iowa business data from data.iowa.gov...")
    logger.info(f"URL: {url}")
    logger.info(f"Output: {output_file}")
    
    try:
        # Download with progress
        def download_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(downloaded * 100 / total_size, 100)
            sys.stdout.write(f"\rDownloading: {percent:.1f}% [{downloaded:,}/{total_size:,} bytes]")
            sys.stdout.flush()
        
        urllib.request.urlretrieve(url, output_file, download_progress)
        print()  # New line after progress
        
        # Check file size
        file_size = os.path.getsize(output_file)
        logger.info(f"✅ Download complete! File size: {file_size:,} bytes")
        
        # Count records
        with open(output_file, 'r') as f:
            record_count = sum(1 for _ in f) - 1  # Subtract header
        
        logger.info(f"📊 Total records: {record_count:,}")
        logger.info(f"📁 File saved to: {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    download_iowa_data()