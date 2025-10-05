#!/usr/bin/env python3
"""
Medical Facilities Loader
Loads CMS Provider of Services data (nationwide medical facilities) into the knowledge graph.
"""
import os
import sys
import csv
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'examples'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from propose_api_client import ProposeAPIClient
from graph_types import NodeType, RelationshipType

logger = logging.getLogger(__name__)


class MedicalFacilitiesLoader:
    """Loader for CMS Provider of Services medical facilities data"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.source_id = None
        self.client = self.setup_client()

        # Get field mapping from config
        self.field_mapping = config.get('field_mapping', {
            'business_name': 'FAC_NAME',
            'street_address': 'ST_ADR',
            'city': 'CITY_NAME',
            'state': 'STATE_CD',
            'zip_code': 'ZIP_CD'
        })

        # Progress tracking
        self.start_time = None
        self.last_report_time = None
        self.records_processed = 0
        self.records_imported = 0
        self.log_callback = None
        self.checkpoint_callback = None
        self.error_callback = None

    def setup_client(self) -> ProposeAPIClient:
        """Setup Propose API client"""
        db_config = self.config.get('database', {})

        conn_params = {
            'host': os.environ.get('DB_HOST', os.path.expandvars(db_config.get('host', 'localhost'))),
            'database': os.environ.get('DB_DATABASE', os.environ.get('DB_NAME', os.path.expandvars(db_config.get('database', 'graph_db')))),
            'user': os.environ.get('DB_USER', os.path.expandvars(db_config.get('user', 'graph_admin'))),
            'password': os.environ.get('DB_PASSWORD', os.path.expandvars(db_config.get('password', ''))),
            'port': int(os.environ.get('DB_PORT', db_config.get('port', 5432)))
        }

        return ProposeAPIClient(conn_params)

    def register_source(self, file_path: str) -> Optional[str]:
        """Register data source"""
        try:
            with self.client.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if already processed
                    cursor.execute("""
                        SELECT source_id, status
                        FROM sources
                        WHERE source_type = %s AND file_name = %s
                    """, (self.config['source_type'], os.path.basename(file_path)))

                    result = cursor.fetchone()
                    if result and result[1] == 'completed':
                        self.logger.info(f"Source already processed: {result[0]}")
                        return None

                    # Create new source
                    cursor.execute("""
                        INSERT INTO sources (
                            source_type, source_name, file_name,
                            download_date, import_started_at, status
                        ) VALUES (%s, %s, %s, NOW(), NOW(), 'processing')
                        RETURNING source_id
                    """, (
                        self.config['source_type'],
                        self.config['source_name'],
                        os.path.basename(file_path)
                    ))

                    self.source_id = cursor.fetchone()[0]
                    conn.commit()
                    self.logger.info(f"Registered source: {self.source_id}")
                    return self.source_id

        except Exception as e:
            self.logger.error(f"Error registering source: {e}")
            raise

    def report_progress(self, current_time: datetime, force: bool = False):
        """Report progress every 5 minutes"""
        if not self.log_callback:
            return

        if not self.last_report_time or force or (current_time - self.last_report_time).seconds >= 300:
            elapsed = (current_time - self.start_time).total_seconds()
            velocity = self.records_processed / elapsed if elapsed > 0 else 0

            self.log_callback({
                'level': 'INFO',
                'message': f'Progress: {self.records_processed:,} processed, {self.records_imported:,} imported',
                'metadata': {
                    'records_processed': self.records_processed,
                    'records_imported': self.records_imported,
                    'velocity_per_sec': round(velocity, 2),
                    'elapsed_seconds': int(elapsed)
                }
            })

            self.last_report_time = current_time

    def run(self, file_path: str, limit: Optional[int] = None,
            batch_size: int = 100, start_from: int = 0,
            checkpoint_callback=None, log_callback=None, error_callback=None) -> Dict[str, Any]:
        """Load medical facilities from CSV"""
        # Set callbacks
        self.log_callback = log_callback
        self.checkpoint_callback = checkpoint_callback
        self.error_callback = error_callback

        # Register source
        source_id = self.register_source(file_path)
        if not source_id:
            return {'status': 'already_processed'}

        self.start_time = datetime.now()
        self.last_report_time = self.start_time

        # Use passed-in parameters, falling back to config
        limit = limit or self.config.get('processing', {}).get('limit')
        batch_size = batch_size or self.config.get('processing', {}).get('batch_size', 50)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for record in reader:
                    if limit and self.records_processed >= limit:
                        break

                    self.process_facility(record)
                    self.records_processed += 1

                    # Report progress
                    if self.records_processed % batch_size == 0:
                        self.client.commit()
                        self.report_progress(datetime.now())

            # Final commit and progress report
            self.client.commit()
            self.report_progress(datetime.now(), force=True)

            # Mark source as completed
            with self.client.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE sources
                        SET status = 'completed',
                            records_processed = %s,
                            records_imported = %s,
                            import_completed_at = NOW()
                        WHERE source_id = %s
                    """, (self.records_processed, self.records_imported, self.source_id))
                    conn.commit()

            return {
                'status': 'success',
                'records_processed': self.records_processed,
                'records_imported': self.records_imported
            }

        except Exception as e:
            self.logger.error(f"Error loading facilities: {e}")
            raise

    def process_facility(self, record: Dict[str, str]):
        """Process a single facility record"""
        try:
            # Extract fields using mapping
            facility_name = record.get(self.field_mapping['business_name'], '').strip()
            street_address = record.get(self.field_mapping['street_address'], '').strip()
            city = record.get(self.field_mapping['city'], '').strip()
            state = record.get(self.field_mapping['state'], '').strip()
            zip_code = record.get(self.field_mapping['zip_code'], '').strip()

            if not facility_name or not city or not state:
                return

            # Build address
            address_parts = [p for p in [street_address, city, state, zip_code] if p]
            full_address = ', '.join(address_parts) if address_parts else None

            # Create facility attributes
            facility_attrs = {
                'name': facility_name,
                'address': full_address
            }

            if street_address:
                facility_attrs['street_address'] = street_address
            if zip_code:
                facility_attrs['zip_code'] = zip_code

            # Propose facility entity
            self.client.propose_fact(
                source_entity=(NodeType.MEDICAL_FACILITY, facility_name),
                target_entity=(NodeType.STATE, state),
                relationship=RelationshipType.LOCATED_IN,
                source_info=(self.config['source_name'], self.config['source_type']),
                source_attributes=facility_attrs,
                relationship_strength=0.95,
                provenance_confidence=0.90
            )

            self.records_imported += 1

        except Exception as e:
            self.logger.error(f"Error processing facility: {e}")
            raise
