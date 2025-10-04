# Job Loader Template

This is a template for creating new job loaders for the distributed worker system.

## Quick Start

1. **Copy this template folder**:
   ```bash
   cp -r jobs/_TEMPLATE jobs/my_new_job
   ```

2. **Rename and customize**:
   - Rename `loader.py` and update the class name
   - Update `config/job_config.yaml` with your settings
   - Add your data files to `data/` folder
   - Update `scripts/run.sh` with your logic

3. **Submit a job**:
   ```bash
   curl -X POST https://lexara-coordinator-prod.cloudswift.workers.dev/jobs/submit \
     -H "Content-Type: application/json" \
     -d '{
       "job_type": "my_new_job",
       "config": {
         "input": {
           "file_path": "/path/to/data.csv"
         }
       }
     }'
   ```

4. **Workers automatically discover it** - No code changes needed!

## Folder Structure

```
jobs/my_new_job/
├── README.md              # This file
├── loader.py              # Main loader class (REQUIRED)
├── config/
│   └── job_config.yaml    # Job configuration
├── data/                  # Sample or downloaded data
├── scripts/
│   ├── download_data.sh   # Optional: Download source data
│   └── run.sh             # Optional: Manual execution
└── tests/
    └── test_loader.py     # Optional: Unit tests
```

## loader.py Requirements

Your `loader.py` must:

1. **Define a class that ends with "Loader"**
2. **Accept a config dict in `__init__`**
3. **Implement a `run()` method**

### Minimal Example

```python
#!/usr/bin/env python3
"""
My New Job Loader
Loads data from XYZ source into the graph database
"""
import sys
import os

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.graph_types import ProposeAPIClient


class MyNewJobLoader:
    """Loader for my new data source"""

    def __init__(self, config: dict):
        self.config = config
        self.connection = None  # Set by distributed worker

    def run(self, file_path: str = None, limit: int = None, **kwargs):
        """
        Execute the loading process

        Args:
            file_path: Path to data file
            limit: Max records to process
            **kwargs: Additional options from distributed worker
        """
        file_path = file_path or self.config.get('input', {}).get('file_path')

        # Your loading logic here
        print(f"Loading from {file_path}")

        # Example: Read CSV and propose facts
        # with open(file_path, 'r') as f:
        #     reader = csv.DictReader(f)
        #     for i, row in enumerate(reader):
        #         if limit and i >= limit:
        #             break
        #
        #         # Process row...
        #         # Use self.connection for database access

        return {
            'records_processed': 0,
            'success': True
        }
```

### Full Example (With All Features)

```python
#!/usr/bin/env python3
import sys
import os
import csv
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.graph_types import ProposeAPIClient


class MyAdvancedLoader:
    """Advanced loader with checkpoints, validation, and error handling"""

    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0
        }

    def run(
        self,
        file_path: str = None,
        limit: int = None,
        batch_size: int = 100,
        checkpoint_callback = None,
        log_callback = None,
        error_callback = None,
        **kwargs
    ):
        """Execute loading with distributed worker callbacks"""

        file_path = file_path or self.config.get('input', {}).get('file_path')
        limit = limit or self.config.get('processing', {}).get('limit')
        batch_size = batch_size or self.config.get('processing', {}).get('batch_size', 100)

        # Setup API client
        client = ProposeAPIClient(connection=self.connection)

        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break

                try:
                    # Process record
                    self._process_record(row, client)
                    self.stats['successful'] += 1

                except Exception as e:
                    self.stats['failed'] += 1

                    # Report error via callback
                    if error_callback:
                        error_callback({
                            'source_record_id': row.get('id'),
                            'issue_type': 'processing_error',
                            'severity': 'error',
                            'message': str(e),
                            'raw_record': row
                        })

                self.stats['total_processed'] += 1

                # Save checkpoint periodically
                if checkpoint_callback and i % batch_size == 0:
                    checkpoint_callback({
                        'records_processed': self.stats['total_processed'],
                        'last_record_id': row.get('id'),
                        'timestamp': i
                    })

                # Send log update
                if log_callback and i % 100 == 0:
                    log_callback({
                        'level': 'INFO',
                        'message': f'Progress: {self.stats["total_processed"]} records',
                        'metadata': self.stats
                    })

        return self.stats

    def _process_record(self, row: Dict, client: ProposeAPIClient):
        """Process a single record"""
        # Your business logic here
        pass
```

## Configuration

Edit `config/job_config.yaml`:

```yaml
loader:
  name: "My New Job Loader"
  type: "data_source_type"

source:
  name: "Data Source Name"
  type: "data_source_identifier"
  url: "https://data.source.gov/api"

input:
  file_path: "${PROJECT_DIR}/jobs/my_new_job/data/dataset.csv"
  format: "csv"
  encoding: "utf-8"

processing:
  batch_size: 1000
  checkpoint_interval: 5000
  limit: null  # null = no limit

validation:
  required_fields:
    - field1
    - field2
  skip_conditions:
    - missing_required_field
```

## Testing Your Loader

1. **Test locally** before submitting to distributed workers:
   ```bash
   python3 jobs/my_new_job/loader.py
   ```

2. **Submit a limited test job**:
   ```bash
   curl -X POST https://lexara-coordinator-prod.cloudswift.workers.dev/jobs/submit \
     -H "Content-Type: application/json" \
     -d '{
       "job_type": "my_new_job",
       "config": {
         "processing": {"limit": 10}
       }
     }'
   ```

3. **Monitor the job**:
   ```bash
   curl https://lexara-coordinator-prod.cloudswift.workers.dev/jobs?status=pending
   ```

## Advanced: Custom Dependencies

If your loader needs special Python packages:

1. Create `requirements.txt` in your job folder:
   ```
   beautifulsoup4==4.12.0
   requests==2.31.0
   ```

2. Install on worker machines:
   ```bash
   pip3 install -r jobs/my_new_job/requirements.txt
   ```

## See Also

- **Existing Examples**:
  - `jobs/iowa_business_loader/` - Complete business entities loader
  - `jobs/iowa_asbestos_licenses/` - Licensed professional loader

- **Documentation**:
  - `/docs/LOADER_REFACTORING_PLAN.md` - Loader best practices
  - `/cloudflare/README.md` - Distributed system architecture
