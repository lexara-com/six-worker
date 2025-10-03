#!/usr/bin/env python3
"""
Iowa Business Entities Rerunnable Loader

This loader implements the BaseDataLoader pattern for idempotent, resumable
loading of Iowa Business Entities data.

Data source: https://data.iowa.gov/Regulation/Active-Iowa-Business-Entities/ez5t-3qay
"""

import csv
import sys
import os
import re
import json
import hashlib
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, Optional, List, Iterator
from abc import ABC, abstractmethod
from psycopg2 import OperationalError, DatabaseError

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'examples'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from propose_api_client import ProposeAPIClient, ProposeResponse
from graph_types import NodeType, RelationshipType, EntityClass

# Import robustness utilities
try:
    from connection_pool import ConnectionPool
    from retry_utils import retry_on_exception, CircuitBreaker
    from data_validator import DataValidator
    HAS_ROBUSTNESS_UTILS = True
except ImportError:
    HAS_ROBUSTNESS_UTILS = False
    logging.warning("Robustness utilities not available - running in basic mode")


class BaseDataLoader(ABC):
    """Base class for all data loaders"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self.setup_logging()
        self.stats = self.init_statistics()
        self.source_id = None
        self.propose_client = self.setup_client()
        self.checkpoint_interval = config.get('checkpoint_interval', 1000)
        self.last_checkpoint = 0

        # Initialize robustness features
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.get('circuit_breaker_threshold', 10),
            timeout=config.get('circuit_breaker_timeout', 60)
        ) if HAS_ROBUSTNESS_UTILS else None

        self.use_connection_pool = config.get('use_connection_pool', False) and HAS_ROBUSTNESS_UTILS
        self.connection_pool = None

        # Progress reporting
        self.progress_interval_seconds = config.get('progress_interval_seconds', 300)  # 5 minutes default
        self.last_progress_report = None
        self.last_progress_count = 0
    
    def setup_logging(self) -> logging.Logger:
        """Configure logging with proper formatting"""
        logger = logging.getLogger(self.__class__.__name__)
        
        # Clear existing handlers
        logger.handlers = []
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        if self.config.get('log_file'):
            file_handler = logging.FileHandler(self.config['log_file'])
            file_handler.setFormatter(console_formatter)
            logger.addHandler(file_handler)
        
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
            'conflicts_detected': 0,
            'checkpoints_saved': 0
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
    
    def determine_version(self, file_path: str) -> str:
        """Determine version based on file metadata"""
        # Extract date from filename if present (e.g., Active_Iowa_Business_Entities_20251001.csv)
        filename = os.path.basename(file_path)
        date_match = re.search(r'(\d{8})', filename)
        
        if date_match:
            date_str = date_match.group(1)
            year = date_str[:4]
            month = int(date_str[4:6])
            quarter = (month - 1) // 3 + 1
            return f"{year}-Q{quarter}"
        
        # Fall back to file modification time
        file_stat = os.stat(file_path)
        file_date = datetime.fromtimestamp(file_stat.st_mtime)
        quarter = (file_date.month - 1) // 3 + 1
        return f"{file_date.year}-Q{quarter}"
    
    def register_source(self, file_path: str) -> Optional[str]:
        """Register data source and check if already processed"""
        file_hash = self.calculate_file_hash(file_path)
        file_size = os.path.getsize(file_path)
        version = self.determine_version(file_path)
        
        self.logger.info(f"Registering source: {os.path.basename(file_path)}")
        self.logger.info(f"Version: {version}, Hash: {file_hash[:12]}..., Size: {file_size:,} bytes")
        
        try:
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if already processed
                    cursor.execute("""
                        SELECT source_id, status, records_processed, records_imported
                        FROM sources
                        WHERE source_type = %s
                          AND file_hash = %s
                    """, (self.config['source_type'], file_hash))
                    
                    result = cursor.fetchone()
                    
                    if result:
                        if result['status'] == 'completed':
                            self.logger.info(f"File already processed: {result['records_imported']} records imported")
                            return None
                        else:
                            self.logger.info(f"Resuming processing from record {result['records_processed']}")
                            self.source_id = result['source_id']
                            self.stats['total_processed'] = result['records_processed'] or 0
                            return result['source_id']
                    
                    # Create new source
                    cursor.execute("""
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
                    ))
                    
                    self.source_id = cursor.fetchone()['source_id']
                    conn.commit()
                    
                    self.logger.info(f"Created new source record: {self.source_id}")
                    return self.source_id
                    
        except Exception as e:
            self.logger.error(f"Error registering source: {e}")
            raise

    def report_progress(self, current_time: datetime, force: bool = False):
        """Report progress at regular intervals"""
        # Initialize on first call
        if self.last_progress_report is None:
            self.last_progress_report = current_time
            self.last_progress_count = self.stats['total_processed']
            return

        # Check if it's time to report
        elapsed = (current_time - self.last_progress_report).total_seconds()

        if not force and elapsed < self.progress_interval_seconds:
            return

        # Calculate metrics
        records_since_last = self.stats['total_processed'] - self.last_progress_count
        velocity = (records_since_last / elapsed) * 60 if elapsed > 0 else 0  # records/minute

        # Calculate overall rate
        total_records = self.stats['total_processed']
        success_rate = (self.stats['successful'] / total_records * 100) if total_records > 0 else 0

        # Report
        self.logger.info(
            f"📊 Progress Report: "
            f"Processed {records_since_last} records in last {elapsed/60:.1f} min "
            f"(velocity: {velocity:.1f} rec/min) | "
            f"Total: {total_records} | "
            f"Success: {self.stats['successful']} ({success_rate:.1f}%) | "
            f"Failed: {self.stats['failed']} | "
            f"Skipped: {self.stats['skipped']}"
        )

        # Update tracking
        self.last_progress_report = current_time
        self.last_progress_count = self.stats['total_processed']

    def save_checkpoint(self, position: int):
        """Save processing checkpoint"""
        if not self.source_id:
            return
        
        try:
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
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
                    conn.commit()
                    
            self.stats['checkpoints_saved'] += 1
            self.last_checkpoint = position
            self.logger.debug(f"Checkpoint saved at position {position}")
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    def mark_complete(self, total_records: int):
        """Mark source as complete"""
        if not self.source_id:
            return
        
        try:
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
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
                    conn.commit()
                    
            self.logger.info(f"Source marked as complete: {total_records} records processed")
            
        except Exception as e:
            self.logger.error(f"Error marking source complete: {e}")
    
    def mark_failed(self, error_message: str):
        """Mark source as failed"""
        if not self.source_id:
            return
        
        try:
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE sources
                        SET status = 'failed',
                            error_message = %s,
                            updated_at = NOW()
                        WHERE source_id = %s
                    """, (error_message[:1000], self.source_id))  # Truncate error message
                    conn.commit()
                    
        except Exception as e:
            self.logger.error(f"Error marking source as failed: {e}")
    
    @abstractmethod
    def read_in_batches(self, file_path: str, batch_size: int, start_from: int):
        """Read file in batches"""
        pass
    
    def process_batch(self, batch: List[Any]) -> List[Dict[str, Any]]:
        """Process a batch of records with error handling"""
        results = []

        for raw_record in batch:
            try:
                # Parse record with error handling
                record = None
                record_id = raw_record.get('Corp Number', 'unknown')
                try:
                    record = self.parse_record(raw_record)
                except Exception as parse_error:
                    self.logger.error(f"Parse error for record {record_id}: {parse_error}")
                    self.stats['failed'] += 1
                    results.append({'status': 'failed', 'errors': [str(parse_error)]})
                    continue

                if not record:
                    self.stats['skipped'] += 1
                    continue

                # Validate record
                errors = self.validate_record(record)
                if errors:
                    company_name = record.get('legal_name', 'unknown')
                    self.logger.warning(f"Validation errors for {record_id} ({company_name}): {errors}")
                    self.stats['failed'] += 1
                    results.append({'status': 'failed', 'errors': errors})
                    continue

                # Process through Propose API with circuit breaker
                propose_results = None
                try:
                    if self.circuit_breaker:
                        propose_results = self.circuit_breaker.call(
                            self.process_record,
                            record
                        )
                    else:
                        propose_results = self.process_record(record)
                except Exception as process_error:
                    company_name = record.get('legal_name', 'unknown')
                    self.logger.error(f"Processing error for {record_id} ({company_name}): {process_error}")
                    self.stats['failed'] += 1
                    results.append({'status': 'failed', 'errors': [str(process_error)]})
                    continue

                # Check results
                if all(r.success for r in propose_results):
                    self.stats['successful'] += 1
                    results.append({'status': 'success'})
                else:
                    self.stats['failed'] += 1
                    failed_results = [r for r in propose_results if not r.success]
                    company_name = record.get('legal_name', 'unknown')
                    # Log first few failures with details
                    if self.stats['failed'] <= 10:
                        self.logger.error(f"Propose API failures for {record_id} ({company_name}):")
                        for fr in failed_results:
                            error_info = fr.error_message or f"Status: {fr.status}"
                            self.logger.error(f"  - {error_info}")
                            if fr.conflicts:
                                self.logger.error(f"    Conflicts: {len(fr.conflicts)}")
                                for conflict in fr.conflicts[:2]:  # Show first 2 conflicts
                                    self.logger.error(f"      {conflict}")
                            if fr.actions:
                                self.logger.error(f"    Actions: {fr.actions}")
                    results.append({'status': 'failed', 'results': failed_results})

            except Exception as e:
                record_id = raw_record.get('Corp Number', 'unknown') if 'raw_record' in locals() else 'unknown'
                self.logger.error(f"Error processing record {record_id}: {e}")
                self.stats['failed'] += 1
                results.append({'status': 'error', 'exception': str(e)})
        
        self.stats['total_processed'] += len(batch)
        return results
    
    def run(self, file_path: str, limit: Optional[int] = None, 
            batch_size: int = 100, start_from: int = 0) -> Dict[str, Any]:
        """Main execution method"""
        
        # Expand environment variables in file path
        file_path = os.path.expandvars(file_path)
        
        # Check if file exists
        if not os.path.exists(file_path):
            self.logger.error(f"Data file not found: {file_path}")
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Register source
        source_id = self.register_source(file_path)
        if not source_id:
            return {'status': 'already_processed'}
        
        start_time = datetime.now()
        
        try:
            # If resuming, adjust start position
            if self.stats['total_processed'] > 0:
                start_from = self.stats['total_processed']
                self.logger.info(f"Resuming from record {start_from}")
            
            # Process file
            processed_count = 0
            
            for batch in self.read_in_batches(file_path, batch_size, start_from):
                batch_results = self.process_batch(batch)
                processed_count += len(batch)

                # Report progress at intervals
                current_time = datetime.now()
                self.report_progress(current_time)

                # Save checkpoint
                if processed_count - self.last_checkpoint >= self.checkpoint_interval:
                    self.save_checkpoint(start_from + processed_count)
                    
                    # Log progress
                    elapsed = datetime.now() - start_time
                    rate = processed_count / elapsed.total_seconds() * 60
                    self.logger.info(
                        f"Processed {start_from + processed_count:,} records "
                        f"({self.stats['successful']:,} successful, "
                        f"{self.stats['failed']:,} failed, "
                        f"{self.stats['skipped']:,} skipped) "
                        f"Rate: {rate:.1f}/min"
                    )
                
                # Check limit
                if limit and processed_count >= limit:
                    self.logger.info(f"Reached limit of {limit} records")
                    break
            
            # Final checkpoint
            self.save_checkpoint(start_from + processed_count)
            
            # Mark as complete
            self.mark_complete(start_from + processed_count)
            
            elapsed = datetime.now() - start_time
            
            return {
                'status': 'completed',
                'statistics': self.stats,
                'elapsed_time': str(elapsed),
                'records_per_minute': processed_count / elapsed.total_seconds() * 60
            }
            
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            self.mark_failed(str(e))
            raise


class IowaBusinessLoader(BaseDataLoader):
    """Rerunnable loader for Iowa Business Entities data"""

    def __init__(self, config: Dict[str, Any]):
        # Initialize geographic cache BEFORE calling super().__init__
        # because parent will call setup_client which needs these attributes
        self.geo_cache = {
            'cities': {},    # {normalized_name: node_id}
            'states': {},    # {normalized_name: node_id}
            'counties': {},  # {normalized_name: node_id}
            'zipcodes': {}   # {code: node_id}
        }
        self.geo_cache_loaded = False

        # Now call parent init which will create the propose_client
        super().__init__(config)

        # Load geographic cache AFTER propose_client is initialized
        self._load_geographic_cache()

    def _load_geographic_cache(self):
        """Load all geographic entities into memory at startup"""
        if self.geo_cache_loaded:
            return

        try:
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Load all geographic nodes
                    cursor.execute("""
                        SELECT node_id, node_type, normalized_name, primary_name
                        FROM nodes
                        WHERE node_type IN ('City', 'State', 'County', 'ZipCode')
                    """)

                    for row in cursor.fetchall():
                        node_id = row['node_id']
                        node_type = row['node_type']
                        normalized_name = row['normalized_name']
                        primary_name = row['primary_name']

                        # Map node types to cache keys
                        type_map = {
                            'City': 'cities',
                            'State': 'states',
                            'County': 'counties',
                            'ZipCode': 'zipcodes'
                        }

                        if node_type in type_map:
                            cache_key = type_map[node_type]
                            if node_type == 'ZipCode':
                                # Use primary name for zip codes
                                self.geo_cache[cache_key][primary_name] = node_id
                            else:
                                self.geo_cache[cache_key][normalized_name] = node_id

                    self.geo_cache_loaded = True
                    self.logger.info(f"📍 Loaded geographic cache: "
                                   f"{len(self.geo_cache['cities'])} cities, "
                                   f"{len(self.geo_cache['states'])} states, "
                                   f"{len(self.geo_cache['counties'])} counties, "
                                   f"{len(self.geo_cache['zipcodes'])} zip codes")
        except Exception as e:
            self.logger.warning(f"Failed to load geographic cache: {e}")

    def setup_client(self) -> ProposeAPIClient:
        """Setup Propose API client"""
        # Expand environment variables with fallback defaults
        conn_params = {
            'host': os.environ.get('DB_HOST', os.path.expandvars(self.config['database'].get('host', 'localhost'))),
            'database': os.environ.get('DB_DATABASE', os.path.expandvars(self.config['database'].get('database', 'graph_db'))),
            'user': os.environ.get('DB_USER', os.path.expandvars(self.config['database'].get('user', 'graph_admin'))),
            'password': os.environ.get('DB_PASSWORD', os.path.expandvars(self.config['database'].get('password', 'your_password'))),
            'port': int(os.environ.get('DB_PORT', self.config['database'].get('port', 5432)))
        }

        # Test connection
        client = ProposeAPIClient(conn_params)
        try:
            with client._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    self.logger.info("✅ Database connection successful")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise

        return client
    
    def parse_record(self, raw_record: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Parse CSV row into structured format"""
        
        def clean_string(value: str) -> Optional[str]:
            if not value or not value.strip():
                return None
            return value.strip().replace('"', '').replace("'", "")
        
        # Extract fields
        corp_number = clean_string(raw_record.get('Corp Number', ''))
        legal_name = clean_string(raw_record.get('Legal Name', ''))
        corp_type = clean_string(raw_record.get('Corporation Type', ''))
        effective_date = clean_string(raw_record.get('Effective Date', ''))
        
        # Skip if missing required fields
        if not legal_name or not corp_type:
            return None
        
        # Registered agent info
        ra_name = clean_string(raw_record.get('Registered Agent', ''))
        ra_address1 = clean_string(raw_record.get('RA Address 1', ''))
        ra_address2 = clean_string(raw_record.get('RA Address 2', ''))
        ra_city = clean_string(raw_record.get('RA City', ''))
        ra_state = clean_string(raw_record.get('RA State', ''))
        ra_zip = clean_string(raw_record.get('RA Zip', ''))
        
        # Home office info
        ho_address1 = clean_string(raw_record.get('HO Address 1', ''))
        ho_address2 = clean_string(raw_record.get('HO Address 2', ''))
        ho_city = clean_string(raw_record.get('HO City', ''))
        ho_state = clean_string(raw_record.get('HO State', ''))
        ho_zip = clean_string(raw_record.get('HO Zip', ''))
        ho_country = clean_string(raw_record.get('HO Country', ''))
        ho_location = clean_string(raw_record.get('HO Location', ''))
        
        # Parse coordinates if available
        coordinates = None
        if ho_location and 'POINT' in ho_location:
            try:
                matches = re.findall(r'-?\d+\.?\d*', ho_location)
                if len(matches) >= 2:
                    lon, lat = float(matches[0]), float(matches[1])
                    coordinates = {"type": "Point", "coordinates": [lon, lat]}
            except:
                pass
        
        return {
            'corp_number': corp_number,
            'legal_name': legal_name,
            'corp_type': corp_type,
            'effective_date': effective_date,
            'registered_agent': {
                'name': ra_name,
                'address1': ra_address1,
                'address2': ra_address2,
                'city': ra_city,
                'state': ra_state,
                'zip': ra_zip
            },
            'home_office': {
                'address1': ho_address1,
                'address2': ho_address2,
                'city': ho_city,
                'state': ho_state,
                'zip': ho_zip,
                'country': ho_country,
                'coordinates': coordinates
            }
        }
    
    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate record and return list of errors"""
        errors = []

        # Use enhanced validator if available
        if HAS_ROBUSTNESS_UTILS:
            errors = DataValidator.validate_iowa_business_record(record)
            if errors:
                return errors

        # Basic validation fallback
        # Required fields
        if not record.get('legal_name'):
            errors.append("Missing legal name")

        if not record.get('corp_type'):
            errors.append("Missing corporation type")

        # Date validation
        if record.get('effective_date'):
            try:
                # Parse date to validate format
                date_str = record['effective_date']
                if '/' in date_str:
                    datetime.strptime(date_str, '%m/%d/%Y')
                else:
                    datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                errors.append(f"Invalid date format: {record['effective_date']}")
        
        return errors
    
    def process_record(self, record: Dict[str, Any]) -> List[ProposeResponse]:
        """Process single record through Propose API"""
        results = []
        
        # Process company with geographic information
        ho = record['home_office']
        if ho['city'] and ho['state']:
            # Build full address
            address_parts = []
            if ho['address1']:
                address_parts.append(ho['address1'])
            if ho['address2']:
                address_parts.append(ho['address2'])
            
            full_address = ', '.join(address_parts) if address_parts else None
            
            # Use geographic propose function
            result = self._propose_geographic_company(
                record['legal_name'],
                ho['city'],
                ho['state'],
                full_address,
                ho['coordinates'],
                record
            )
            results.append(result)
            
            if result.success:
                self.stats['entities_created'] += 1
                self.stats['relationships_created'] += 2  # Company->Address, Address->City
        else:
            # Fallback: Company incorporated in Iowa
            result = self._propose_company_state(record)
            results.append(result)
            
            if result.success:
                self.stats['entities_created'] += 1
                self.stats['relationships_created'] += 1
        
        # Process registered agent
        ra = record['registered_agent']
        if ra['name']:
            agent_result = self._propose_registered_agent(record)
            results.append(agent_result)
            
            if agent_result.success:
                self.stats['entities_created'] += 1
                self.stats['relationships_created'] += 1
        
        # Track conflicts
        conflicts = [r for r in results if r.conflicts]
        if conflicts:
            self.stats['conflicts_detected'] += len(conflicts)
        
        return results
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for matching (same as database function)"""
        if not name:
            return ""
        return name.lower().strip()

    def _get_or_create_city(self, city_name: str) -> Optional[str]:
        """Get city ID from cache, create if not exists"""
        normalized = self._normalize_name(city_name)

        # Check cache first
        if normalized in self.geo_cache['cities']:
            # Cache hit - no database query needed
            return self.geo_cache['cities'][normalized]

        # Not in cache - city might be new, call propose function
        # The propose_geographic_fact will handle creation
        self.logger.debug(f"Cache miss for city: {city_name}")
        return None

    def _propose_geographic_company(self, company_name: str, city: str, state: str,
                                   address: Optional[str], coordinates: Optional[Dict],
                                   record: Dict) -> ProposeResponse:
        """Create company with geographic hierarchy using cached lookups"""
        try:
            # Check if city exists in cache
            city_id = self._get_or_create_city(city)

            # Still use propose_geographic_fact but it will be faster
            # since cities are likely already in the database
            with self.propose_client._get_connection() as conn:
                with conn.cursor() as cursor:
                    sql = """
                    SELECT propose_geographic_fact(
                        %s, %s, %s, %s, %s, %s::JSONB, %s, %s
                    )
                    """

                    cursor.execute(sql, [
                        company_name,
                        'Company',
                        city,
                        'City',
                        address,
                        json.dumps(coordinates) if coordinates else None,
                        self.config['source_name'],  # Use consistent source name, not per-record
                        self.config['source_type']
                    ])

                    result = cursor.fetchone()

                    # If we created a new city, update cache
                    if city_id is None:
                        normalized_city = self._normalize_name(city)
                        # Refresh this specific city in cache
                        cursor.execute("""
                            SELECT node_id FROM nodes
                            WHERE node_type = 'City' AND normalized_name = %s
                            LIMIT 1
                        """, [normalized_city])
                        new_city = cursor.fetchone()
                        if new_city:
                            self.geo_cache['cities'][normalized_city] = new_city['node_id']

                    if result and 'propose_geographic_fact' in result:
                        return ProposeResponse(
                            success=True,
                            status='success',
                            overall_confidence=0.95
                        )
                    else:
                        return ProposeResponse(
                            success=False,
                            status='error',
                            overall_confidence=0.0,
                            error_message="No result from geographic propose"
                        )

        except Exception as e:
            self.logger.error(f"Geographic propose error: {e}")
            return ProposeResponse(
                success=False,
                status='error',
                overall_confidence=0.0,
                error_message=str(e)
            )
    
    def _propose_company_state(self, record: Dict) -> ProposeResponse:
        """Create company incorporated in Iowa"""
        company_attributes = {
            'iowa_business_id': record['corp_number'],  # Primary identifier
            'iowa_corp_number': record['corp_number'],
            'entity_type': record['corp_type'],
            'incorporation_date': record['effective_date']
        }
        
        return self.propose_client.propose_fact(
            source_entity=(NodeType.COMPANY, record['legal_name']),
            target_entity=(NodeType.STATE, 'Iowa'),
            relationship=RelationshipType.INCORPORATED_IN,
            source_info=(self.config['source_name'], self.config['source_type']),
            source_attributes=company_attributes,
            relationship_strength=0.98,
            provenance_confidence=0.92
        )
    
    def _propose_registered_agent(self, record: Dict) -> ProposeResponse:
        """Create registered agent relationship"""
        ra = record['registered_agent']
        
        # Build agent attributes
        agent_attributes = {'role': 'Registered Agent'}
        
        if ra['address1'] or ra['address2']:
            address_parts = []
            if ra['address1']:
                address_parts.append(ra['address1'])
            if ra['address2']:
                address_parts.append(ra['address2'])
            agent_attributes['address'] = ', '.join(address_parts)
        
        if ra['city'] or ra['state'] or ra['zip']:
            location_parts = []
            if ra['city']:
                location_parts.append(ra['city'])
            if ra['state']:
                location_parts.append(ra['state'])
            if ra['zip']:
                location_parts.append(ra['zip'])
            agent_attributes['location'] = ', '.join(location_parts)
        
        # Determine agent type
        business_suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'CORPORATION']
        agent_name_upper = ra['name'].upper()
        is_business = any(suffix in agent_name_upper for suffix in business_suffixes)
        
        agent_type = NodeType.COMPANY if is_business else NodeType.PERSON
        
        return self.propose_client.propose_fact(
            source_entity=(agent_type, ra['name']),
            target_entity=(NodeType.COMPANY, record['legal_name']),
            relationship=RelationshipType.REGISTERED_AGENT,
            source_info=(self.config['source_name'], self.config['source_type']),
            source_attributes=agent_attributes,
            relationship_strength=0.95,
            relationship_metadata={'corp_number': record['corp_number']},
            provenance_confidence=0.92
        )
    
    def read_in_batches(self, file_path: str, batch_size: int, start_from: int) -> Iterator[List[Dict]]:
        """Read CSV file in batches"""
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Skip to start position
            for _ in range(start_from):
                try:
                    next(reader)
                except StopIteration:
                    return
            
            batch = []
            for row in reader:
                batch.append(row)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            
            # Yield remaining records
            if batch:
                yield batch


def main():
    """Main entry point for standalone execution"""
    import argparse
    import yaml
    
    parser = argparse.ArgumentParser(description='Iowa Business Entities Rerunnable Loader')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--limit', type=int, help='Limit number of records (for testing)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--start-from', type=int, default=0, help='Start from record')
    
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override with command line arguments
    if args.batch_size:
        config['processing']['batch_size'] = args.batch_size
    
    # Create loader
    loader = IowaBusinessLoader(config)
    
    # Run loader
    result = loader.run(
        config['input']['file_path'],
        limit=args.limit,
        batch_size=config['processing']['batch_size'],
        start_from=args.start_from
    )
    
    # Print results
    print(f"\nLoader completed with status: {result['status']}")
    if result.get('statistics'):
        print("\nStatistics:")
        for key, value in result['statistics'].items():
            print(f"  {key}: {value:,}")
    if result.get('elapsed_time'):
        print(f"  Elapsed time: {result['elapsed_time']}")
        print(f"  Rate: {result.get('records_per_minute', 0):.1f} records/minute")


if __name__ == "__main__":
    main()