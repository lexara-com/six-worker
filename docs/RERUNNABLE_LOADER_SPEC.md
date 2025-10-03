# Rerunnable Data Loader Specification

## Overview

This specification defines the requirements and best practices for building rerunnable data loaders that integrate with the Propose API. All loaders should be idempotent, resumable, and maintain comprehensive audit trails.

## Core Requirements

### 1. Idempotency

**Principle**: Running the loader multiple times with the same input should produce the same result.

**Implementation**:
```python
class DataLoader:
    def __init__(self, source_config):
        self.source_config = source_config
        self.source_id = None
        self.processed_records = set()
    
    def get_or_create_source(self):
        """Get existing source or create new one"""
        # Check if this exact source was already processed
        existing = self.db.query("""
            SELECT source_id, status, records_processed 
            FROM sources 
            WHERE source_type = %s 
              AND source_version = %s
              AND file_hash = %s
        """, (self.source_config['type'], 
              self.source_config['version'],
              self.calculate_file_hash()))
        
        if existing and existing['status'] == 'completed':
            return None  # Already processed
        elif existing:
            self.source_id = existing['source_id']
            self.resume_from = existing['records_processed']
        else:
            self.source_id = self.create_new_source()
            self.resume_from = 0
```

### 2. Resumability

**Checkpoint Management**:
```python
class CheckpointManager:
    def __init__(self, source_id, checkpoint_interval=100):
        self.source_id = source_id
        self.checkpoint_interval = checkpoint_interval
        self.current_position = 0
        self.last_checkpoint = 0
    
    def save_checkpoint(self, position, state=None):
        """Save processing checkpoint"""
        self.db.execute("""
            UPDATE sources 
            SET records_processed = %s,
                last_checkpoint = NOW(),
                processing_state = %s
            WHERE source_id = %s
        """, (position, json.dumps(state), self.source_id))
        self.last_checkpoint = position
    
    def should_checkpoint(self, current):
        return current - self.last_checkpoint >= self.checkpoint_interval
```

### 3. Source Version Management

**Version Tracking**:
```python
def determine_version(file_path, frequency='quarterly'):
    """Determine version based on file metadata and frequency"""
    file_stat = os.stat(file_path)
    file_date = datetime.fromtimestamp(file_stat.st_mtime)
    
    if frequency == 'quarterly':
        quarter = (file_date.month - 1) // 3 + 1
        return f"{file_date.year}-Q{quarter}"
    elif frequency == 'monthly':
        return f"{file_date.year}-{file_date.month:02d}"
    elif frequency == 'daily':
        return file_date.strftime("%Y-%m-%d")
    else:
        # Use file hash for unique version
        return calculate_file_hash(file_path)[:12]
```

## Loader Architecture

### Base Loader Class

```python
from abc import ABC, abstractmethod
import hashlib
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

class BaseDataLoader(ABC):
    """Base class for all data loaders"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self.setup_logging()
        self.stats = self.init_statistics()
        self.source_id = None
        self.propose_client = self.setup_client()
    
    def setup_logging(self) -> logging.Logger:
        """Configure logging with proper formatting"""
        logger = logging.getLogger(self.__class__.__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(self.config.get('log_level', 'INFO'))
        return logger
    
    def init_statistics(self) -> Dict[str, int]:
        """Initialize processing statistics"""
        return {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'entities_created': 0,
            'relationships_created': 0,
            'conflicts_detected': 0
        }
    
    @abstractmethod
    def setup_client(self):
        """Setup propose API client"""
        pass
    
    @abstractmethod
    def parse_record(self, raw_record: Any) -> Optional[Dict[str, Any]]:
        """Parse raw record into structured format"""
        pass
    
    @abstractmethod
    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate record and return list of errors"""
        pass
    
    @abstractmethod
    def process_record(self, record: Dict[str, Any]) -> List[Any]:
        """Process single record through Propose API"""
        pass
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def register_source(self, file_path: str) -> Optional[str]:
        """Register data source and check if already processed"""
        file_hash = self.calculate_file_hash(file_path)
        file_size = os.path.getsize(file_path)
        
        # Determine version
        version = self.determine_version(file_path)
        
        # Check if already processed
        result = self.propose_client.execute_query("""
            SELECT source_id, status, records_processed
            FROM sources
            WHERE source_type = %s
              AND file_hash = %s
        """, (self.config['source_type'], file_hash))
        
        if result and result[0]['status'] == 'completed':
            self.logger.info(f"File already processed: {file_path}")
            return None
        elif result:
            self.logger.info(f"Resuming processing from record {result[0]['records_processed']}")
            self.source_id = result[0]['source_id']
            return result[0]['source_id']
        
        # Create new source
        self.source_id = self.propose_client.execute_query("""
            INSERT INTO sources (
                source_type, source_name, source_version,
                file_name, file_hash, file_size_bytes,
                download_date, import_started_at, status
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), 'processing')
            RETURNING source_id
        """, (
            self.config['source_type'],
            self.config['source_name'],
            version,
            os.path.basename(file_path),
            file_hash,
            file_size
        ))[0]['source_id']
        
        return self.source_id
    
    def run(self, file_path: str, limit: Optional[int] = None, 
            batch_size: int = 100, start_from: int = 0) -> Dict[str, Any]:
        """Main execution method"""
        
        # Register source
        source_id = self.register_source(file_path)
        if not source_id:
            return {'status': 'already_processed'}
        
        try:
            # Process file
            processed_count = 0
            error_records = []
            
            for batch in self.read_in_batches(file_path, batch_size, start_from):
                batch_results = self.process_batch(batch)
                processed_count += len(batch)
                
                # Update statistics
                self.update_stats(batch_results)
                
                # Save checkpoint
                if processed_count % 1000 == 0:
                    self.save_checkpoint(processed_count)
                    self.logger.info(f"Processed {processed_count} records")
                
                # Check limit
                if limit and processed_count >= limit:
                    break
            
            # Mark as complete
            self.mark_complete(processed_count)
            
            return {
                'status': 'completed',
                'statistics': self.stats,
                'error_records': error_records
            }
            
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            self.mark_failed(str(e))
            raise
    
    def process_batch(self, batch: List[Any]) -> List[Dict[str, Any]]:
        """Process a batch of records"""
        results = []
        
        for raw_record in batch:
            try:
                # Parse record
                record = self.parse_record(raw_record)
                if not record:
                    self.stats['skipped'] += 1
                    continue
                
                # Validate record
                errors = self.validate_record(record)
                if errors:
                    self.logger.warning(f"Validation errors: {errors}")
                    self.stats['failed'] += 1
                    results.append({'status': 'failed', 'errors': errors})
                    continue
                
                # Process through Propose API
                propose_results = self.process_record(record)
                
                # Check results
                if all(r.success for r in propose_results):
                    self.stats['successful'] += 1
                    results.append({'status': 'success'})
                else:
                    self.stats['failed'] += 1
                    results.append({'status': 'failed', 'results': propose_results})
                
            except Exception as e:
                self.logger.error(f"Error processing record: {e}")
                self.stats['failed'] += 1
                results.append({'status': 'error', 'exception': str(e)})
        
        return results
    
    @abstractmethod
    def read_in_batches(self, file_path: str, batch_size: int, start_from: int):
        """Read file in batches"""
        pass
    
    def save_checkpoint(self, position: int):
        """Save processing checkpoint"""
        self.propose_client.execute_query("""
            UPDATE sources
            SET records_processed = %s,
                records_imported = %s,
                records_failed = %s,
                records_skipped = %s,
                updated_at = NOW()
            WHERE source_id = %s
        """, (
            position,
            self.stats['successful'],
            self.stats['failed'],
            self.stats['skipped'],
            self.source_id
        ))
    
    def mark_complete(self, total_records: int):
        """Mark source as complete"""
        self.propose_client.execute_query("""
            UPDATE sources
            SET status = 'completed',
                import_completed_at = NOW(),
                records_in_file = %s,
                records_processed = %s,
                records_imported = %s,
                records_failed = %s,
                records_skipped = %s
            WHERE source_id = %s
        """, (
            total_records,
            total_records,
            self.stats['successful'],
            self.stats['failed'],
            self.stats['skipped'],
            self.source_id
        ))
    
    def mark_failed(self, error_message: str):
        """Mark source as failed"""
        self.propose_client.execute_query("""
            UPDATE sources
            SET status = 'failed',
                error_message = %s,
                updated_at = NOW()
            WHERE source_id = %s
        """, (error_message, self.source_id))
```

## Configuration Management

### Loader Configuration Schema

```yaml
# loader_config.yaml
loader:
  name: "Business Registry Loader"
  type: "government_database"
  version: "1.0.0"
  
source:
  name: "State Business Registry"
  type: "government_database"
  update_frequency: "quarterly"
  
database:
  host: "${DB_HOST}"
  port: 5432
  database: "graph_db"
  user: "${DB_USER}"
  password: "${DB_PASSWORD}"
  
processing:
  batch_size: 1000
  checkpoint_interval: 5000
  max_retries: 3
  retry_delay: 60  # seconds
  
validation:
  required_fields:
    - name
    - entity_type
  date_fields:
    - incorporation_date
    - last_update
  
logging:
  level: "INFO"
  file: "loader_${DATE}.log"
  max_size: "100MB"
  retention: 30  # days
```

## Job Scheduling

### Cron Configuration

```bash
# /etc/cron.d/data_loaders

# Business Registry - Quarterly on first Monday
0 2 1-7 1,4,7,10 1 /usr/local/bin/run_loader.sh business_registry

# Court Records - Weekly on Sunday
0 3 * * 0 /usr/local/bin/run_loader.sh court_records

# News Articles - Daily at 4 AM
0 4 * * * /usr/local/bin/run_loader.sh news_articles
```

### Job Runner Script

```bash
#!/bin/bash
# run_loader.sh

set -e

LOADER_NAME=$1
CONFIG_DIR="/etc/loaders"
LOG_DIR="/var/log/loaders"
LOCK_DIR="/var/lock/loaders"

# Create lock file to prevent concurrent runs
LOCK_FILE="${LOCK_DIR}/${LOADER_NAME}.lock"
exec 200>"${LOCK_FILE}"

if ! flock -n 200; then
    echo "Loader ${LOADER_NAME} is already running"
    exit 1
fi

# Set up environment
export $(cat /etc/loaders/${LOADER_NAME}.env | xargs)

# Run loader
python3 /opt/loaders/${LOADER_NAME}/main.py \
    --config ${CONFIG_DIR}/${LOADER_NAME}.yaml \
    --log-dir ${LOG_DIR} \
    2>&1 | tee -a ${LOG_DIR}/${LOADER_NAME}_$(date +%Y%m%d).log

# Check exit status
if [ $? -eq 0 ]; then
    echo "SUCCESS" | mail -s "Loader ${LOADER_NAME} completed" admin@example.com
else
    echo "FAILED" | mail -s "Loader ${LOADER_NAME} failed" admin@example.com
fi

# Release lock
flock -u 200
```

## Monitoring & Alerting

### Health Check Queries

```sql
-- Check loader status
CREATE VIEW loader_status AS
SELECT 
    s.source_type,
    s.source_name,
    s.source_version,
    s.status,
    s.import_started_at,
    s.import_completed_at,
    s.records_processed,
    s.records_imported,
    s.records_failed,
    CASE 
        WHEN s.status = 'processing' AND 
             s.updated_at < NOW() - INTERVAL '1 hour' 
        THEN 'STALLED'
        ELSE s.status
    END as health_status
FROM sources s
WHERE s.created_at > NOW() - INTERVAL '7 days'
ORDER BY s.created_at DESC;

-- Alert on stalled loaders
SELECT * FROM loader_status WHERE health_status = 'STALLED';

-- Check processing rates
SELECT 
    source_type,
    DATE(import_started_at) as date,
    AVG(records_imported::float / 
        EXTRACT(EPOCH FROM (import_completed_at - import_started_at)) * 60
    ) as avg_records_per_minute
FROM sources
WHERE status = 'completed'
  AND import_completed_at IS NOT NULL
GROUP BY source_type, DATE(import_started_at)
ORDER BY date DESC;
```

### Prometheus Metrics

```python
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Define metrics
records_processed = Counter('loader_records_processed_total', 
                           'Total records processed',
                           ['loader_name', 'status'])
                           
processing_time = Histogram('loader_processing_duration_seconds',
                           'Time spent processing records',
                           ['loader_name'])
                           
current_position = Gauge('loader_current_position',
                        'Current processing position',
                        ['loader_name'])

class MonitoredLoader(BaseDataLoader):
    def process_record(self, record):
        with processing_time.labels(loader_name=self.__class__.__name__).time():
            result = super().process_record(record)
        
        status = 'success' if result.success else 'failed'
        records_processed.labels(
            loader_name=self.__class__.__name__,
            status=status
        ).inc()
        
        return result
    
    def save_checkpoint(self, position):
        super().save_checkpoint(position)
        current_position.labels(
            loader_name=self.__class__.__name__
        ).set(position)
```

## Error Recovery

### Retry Strategy

```python
class RetryableLoader(BaseDataLoader):
    def __init__(self, config):
        super().__init__(config)
        self.retry_queue = []
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 60)
    
    def process_with_retry(self, record, attempt=1):
        """Process record with exponential backoff retry"""
        try:
            return self.process_record(record)
        except Exception as e:
            if attempt >= self.max_retries:
                self.logger.error(f"Max retries reached for record: {record}")
                raise
            
            delay = self.retry_delay * (2 ** (attempt - 1))
            self.logger.warning(f"Retry {attempt}/{self.max_retries} in {delay}s")
            time.sleep(delay)
            
            return self.process_with_retry(record, attempt + 1)
```

### Dead Letter Queue

```python
class DeadLetterQueue:
    """Store failed records for manual review"""
    
    def __init__(self, source_id):
        self.source_id = source_id
    
    def add_failed_record(self, record, error):
        """Add failed record to DLQ"""
        self.db.execute("""
            INSERT INTO failed_records (
                source_id,
                record_data,
                error_message,
                error_type,
                created_at
            ) VALUES (%s, %s, %s, %s, NOW())
        """, (
            self.source_id,
            json.dumps(record),
            str(error),
            type(error).__name__
        ))
    
    def reprocess_dlq(self):
        """Attempt to reprocess DLQ records"""
        failed_records = self.db.query("""
            SELECT record_id, record_data
            FROM failed_records
            WHERE source_id = %s
              AND reprocessed = FALSE
            ORDER BY created_at
        """, (self.source_id,))
        
        for record in failed_records:
            try:
                data = json.loads(record['record_data'])
                result = self.process_record(data)
                
                if result.success:
                    self.mark_reprocessed(record['record_id'])
            except Exception as e:
                self.logger.error(f"DLQ reprocess failed: {e}")
```

## Testing

### Unit Tests

```python
import unittest
from unittest.mock import Mock, patch

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.config = {
            'source_type': 'test_source',
            'source_name': 'Test Loader'
        }
        self.loader = TestableLoader(self.config)
    
    def test_parse_record(self):
        """Test record parsing"""
        raw = {'name': 'Test Corp', 'type': 'LLC'}
        parsed = self.loader.parse_record(raw)
        
        self.assertEqual(parsed['entity_type'], 'Company')
        self.assertEqual(parsed['name'], 'TEST CORP')
    
    def test_validate_record(self):
        """Test record validation"""
        # Valid record
        record = {'name': 'Test', 'entity_type': 'Company'}
        errors = self.loader.validate_record(record)
        self.assertEqual(len(errors), 0)
        
        # Invalid record
        record = {'entity_type': 'Company'}  # Missing name
        errors = self.loader.validate_record(record)
        self.assertIn('Missing required field: name', errors)
    
    @patch('propose_api_client.ProposeAPIClient')
    def test_idempotency(self, mock_client):
        """Test idempotent processing"""
        record = {'name': 'Test Corp', 'entity_type': 'Company'}
        
        # Process twice
        result1 = self.loader.process_record(record)
        result2 = self.loader.process_record(record)
        
        # Should only create once
        self.assertEqual(mock_client.propose_fact.call_count, 1)
```

### Integration Tests

```python
class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.test_db = setup_test_database()
        self.loader = create_test_loader(self.test_db)
    
    def test_full_pipeline(self):
        """Test complete loading pipeline"""
        # Create test file
        test_file = create_test_csv([
            {'name': 'Company A', 'city': 'Des Moines'},
            {'name': 'Company B', 'city': 'Iowa City'}
        ])
        
        # Run loader
        result = self.loader.run(test_file)
        
        # Verify results
        self.assertEqual(result['statistics']['successful'], 2)
        
        # Verify database state
        companies = self.test_db.query(
            "SELECT * FROM nodes WHERE node_type = 'Company'"
        )
        self.assertEqual(len(companies), 2)
    
    def test_resume_after_failure(self):
        """Test resuming after partial failure"""
        # Simulate failure after first record
        with patch.object(self.loader, 'process_record') as mock:
            mock.side_effect = [Mock(success=True), Exception('Failed')]
            
            with self.assertRaises(Exception):
                self.loader.run('test.csv')
        
        # Resume processing
        result = self.loader.run('test.csv')
        
        # Should skip first record and process remaining
        self.assertEqual(result['statistics']['successful'], 1)
```

## Performance Optimization

### Batch Operations

```python
def batch_create_entities(entities: List[Dict], batch_size: int = 1000):
    """Create multiple entities in batch"""
    
    for batch in chunks(entities, batch_size):
        values = []
        for entity in batch:
            values.append((
                entity['type'],
                entity['name'],
                normalize_name(entity['name']),
                entity.get('attributes', {})
            ))
        
        # Bulk insert with ON CONFLICT
        query = """
            INSERT INTO nodes (node_type, primary_name, normalized_name, metadata)
            VALUES %s
            ON CONFLICT (normalized_name, node_type) DO NOTHING
            RETURNING node_id, primary_name
        """
        
        results = execute_values_fetch(cursor, query, values)
        yield results
```

### Connection Pooling

```python
from psycopg2 import pool

class PooledLoader(BaseDataLoader):
    def setup_client(self):
        """Setup connection pool"""
        self.connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            **self.config['database']
        )
    
    def get_connection(self):
        """Get connection from pool"""
        return self.connection_pool.getconn()
    
    def release_connection(self, conn):
        """Return connection to pool"""
        self.connection_pool.putconn(conn)
```

## Deployment Checklist

- [ ] Configuration files created and validated
- [ ] Database credentials securely stored
- [ ] Logging configured with appropriate retention
- [ ] Monitoring metrics exposed
- [ ] Alert thresholds configured
- [ ] Cron jobs scheduled
- [ ] Lock files configured to prevent concurrent runs
- [ ] Dead letter queue tables created
- [ ] Test data processed successfully
- [ ] Performance benchmarks met
- [ ] Documentation updated
- [ ] Rollback procedure documented