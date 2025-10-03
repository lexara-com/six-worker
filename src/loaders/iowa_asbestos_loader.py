#!/usr/bin/env python3
"""
Iowa Asbestos Licenses Rerunnable Loader

This loader implements the BaseDataLoader pattern for idempotent, resumable
loading of Iowa Asbestos License holder data.

Data source: https://data.iowa.gov/Workforce/Active-Iowa-Asbestos-Licenses/c9cg-ivvu
"""

import csv
import json
import sys
import os
import re
import hashlib
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Iterator

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'examples'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from propose_api_client import ProposeAPIClient, ProposeResponse
from loaders.iowa_business_loader import BaseDataLoader

# Import from src directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from graph_types import NodeType, RelationshipType, EntityClass


class IowaAsbestosLoader(BaseDataLoader):
    """Rerunnable loader for Iowa Asbestos License data"""
    
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
        """Parse CSV/JSON record into structured format"""
        
        def clean_string(value: str) -> Optional[str]:
            if not value or not value.strip():
                return None
            # Clean and normalize
            cleaned = value.strip().replace('"', '').replace("'", "")
            # Handle 'null' strings
            if cleaned.lower() == 'null':
                return None
            return cleaned
        
        # Handle both CSV and JSON formats
        if 'folderrsn' in raw_record:
            # JSON format (from API)
            folder_rsn = clean_string(raw_record.get('folderrsn', ''))
            registration_number = clean_string(raw_record.get('registration_number', ''))
            license_type = clean_string(raw_record.get('license_type', ''))
            first_name = clean_string(raw_record.get('first_name', ''))
            last_name = clean_string(raw_record.get('last_name', ''))
            county = clean_string(raw_record.get('county', ''))
            issue_date = clean_string(raw_record.get('issue_date', ''))
            expire_date = clean_string(raw_record.get('expire_date', ''))
        else:
            # CSV format (from export)
            folder_rsn = clean_string(raw_record.get('FolderRSN', ''))
            registration_number = clean_string(raw_record.get('Registration Number', ''))
            license_type = clean_string(raw_record.get('License Type', ''))
            first_name = clean_string(raw_record.get('First Name', ''))
            last_name = clean_string(raw_record.get('Last Name', ''))
            county = clean_string(raw_record.get('County', ''))
            issue_date = clean_string(raw_record.get('Issue Date', ''))
            expire_date = clean_string(raw_record.get('Expire Date', ''))
        
        # Skip if missing required fields
        if not first_name or not last_name:
            return None
        
        # Build full name
        full_name = f"{first_name} {last_name}".upper()
        
        # Parse dates - handle multiple formats
        parsed_issue_date = None
        parsed_expire_date = None
        
        if issue_date:
            try:
                # Handle ISO format from JSON
                if 'T' in issue_date:
                    parsed_issue_date = issue_date.split('T')[0]
                elif '/' in issue_date:
                    # Handle MM/DD/YYYY format
                    dt = datetime.strptime(issue_date, '%m/%d/%Y')
                    parsed_issue_date = dt.strftime('%Y-%m-%d')
                else:
                    parsed_issue_date = issue_date
            except:
                parsed_issue_date = issue_date  # Keep original if can't parse
        
        if expire_date:
            try:
                # Handle ISO format from JSON
                if 'T' in expire_date:
                    parsed_expire_date = expire_date.split('T')[0]
                elif '/' in expire_date:
                    # Handle MM/DD/YYYY format
                    dt = datetime.strptime(expire_date, '%m/%d/%Y')
                    parsed_expire_date = dt.strftime('%Y-%m-%d')
                else:
                    parsed_expire_date = expire_date
            except:
                parsed_expire_date = expire_date  # Keep original if can't parse
        
        return {
            'folder_rsn': folder_rsn,
            'registration_number': registration_number,
            'license_type': license_type,
            'first_name': first_name,
            'last_name': last_name,
            'full_name': full_name,
            'county': county,
            'issue_date': parsed_issue_date,
            'expire_date': parsed_expire_date
        }
    
    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate record and return list of errors"""
        errors = []
        
        # Required fields
        if not record.get('full_name'):
            errors.append("Missing person name")
        
        if not record.get('license_type'):
            errors.append("Missing license type")
        
        # Validate license type
        valid_types = ['Worker', 'Inspector', 'Contractor/Supervisor', 
                      'Management Planner', 'Project Designer']
        if record.get('license_type') and record['license_type'] not in valid_types:
            # Don't error, just log warning
            self.logger.warning(f"Unknown license type: {record['license_type']}")
        
        # Date validation - accept multiple formats
        for date_field in ['issue_date', 'expire_date']:
            if record.get(date_field):
                valid = False
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        datetime.strptime(record[date_field], fmt)
                        valid = True
                        break
                    except ValueError:
                        continue
                
                if not valid:
                    errors.append(f"Invalid date format for {date_field}: {record[date_field]}")
        
        return errors
    
    def process_record(self, record: Dict[str, Any]) -> List[ProposeResponse]:
        """Process single record through Propose API"""
        results = []
        
        # Create Person node with license
        person_result = self._create_licensed_person(record)
        results.append(person_result)
        
        if person_result.success:
            self.stats['entities_created'] += 1
            
            # If county is specified, create location relationship
            if record.get('county'):
                location_result = self._create_location_relationship(record)
                if location_result:
                    results.append(location_result)
                    if location_result.success:
                        self.stats['relationships_created'] += 1
        
        # Track any conflicts
        conflicts = [r for r in results if r.conflicts]
        if conflicts:
            self.stats['conflicts_detected'] += len(conflicts)
        
        return results
    
    def _create_licensed_person(self, record: Dict) -> ProposeResponse:
        """Create Person node with asbestos license attributes"""
        
        # Build person attributes
        person_attributes = {
            'asbestos_license_type': record['license_type'],
            'asbestos_registration_number': record['registration_number'],
            'license_status': 'Active',  # Dataset only contains active licenses
            'professional_license': 'Iowa Asbestos License'
        }
        
        # Add dates if available
        if record.get('issue_date'):
            person_attributes['license_issue_date'] = record['issue_date']
        
        if record.get('expire_date'):
            person_attributes['license_expire_date'] = record['expire_date']
        
        # Add folder RSN as unique identifier
        if record.get('folder_rsn'):
            person_attributes['iowa_folder_rsn'] = record['folder_rsn']
        
        # Note: computed_first_name and computed_surname are auto-added by database trigger
        # Don't add them here to avoid duplicate key constraint violation
        
        # Log what we're about to create
        self.logger.debug(f"Creating person: {record['full_name']} with {len(person_attributes)} attributes")
        
        try:
            # Create relationship to State of Iowa as licensing authority
            # Using INCORPORATED_IN as closest match for professional licensing
            result = self.propose_client.propose_fact(
                source_entity=(NodeType.PERSON, record['full_name']),
                target_entity=(NodeType.STATE, 'Iowa'),
                relationship=RelationshipType.INCORPORATED_IN.value,  # Use string value
                source_info=(self.config['source_name'], self.config['source_type']),
                source_attributes=person_attributes,
                relationship_strength=0.95,
                relationship_valid_from=record.get('issue_date'),
                relationship_valid_to=record.get('expire_date'),
                relationship_metadata={
                    'license_type': 'Asbestos',
                    'license_category': record['license_type'],
                    'registration_number': record['registration_number']
                },
                provenance_confidence=0.95  # Official government source
            )
            
            if not result.success:
                self.logger.error(f"Failed to create person {record['full_name']}: {result.error_message}")
            else:
                self.logger.debug(f"Successfully created person {record['full_name']}")
                
            return result
            
        except Exception as e:
            self.logger.error(f"Exception creating person {record['full_name']}: {str(e)}")
            # Create a failed response
            from propose_api_client import ProposeResponse
            return ProposeResponse(
                success=False,
                status='error',
                error_message=str(e)
            )
    
    def _create_location_relationship(self, record: Dict) -> Optional[ProposeResponse]:
        """Create relationship to Iowa county if specified"""
        
        if not record.get('county'):
            return None
        
        # Normalize county name (add "County" if not present)
        county_name = record['county']
        if not county_name.endswith('County'):
            county_name = f"{county_name} County"
        
        # Create Located_In relationship to county
        return self.propose_client.propose_fact(
            source_entity=(NodeType.PERSON, record['full_name']),
            target_entity=(NodeType.COUNTY, county_name),
            relationship=RelationshipType.LOCATED_IN.value,  # Use string value
            source_info=(self.config['source_name'], self.config['source_type']),
            relationship_strength=0.85,  # County info may be business location
            relationship_metadata={
                'location_type': 'business_county',
                'source_field': 'county'
            },
            provenance_confidence=0.95
        )
    
    def read_in_batches(self, file_path: str, batch_size: int, start_from: int) -> Iterator[List[Dict]]:
        """Read CSV or JSON file in batches"""
        
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.json':
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Skip to start position
                data = data[start_from:]
                
                # Yield in batches
                batch = []
                for record in data:
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                
                # Yield remaining records
                if batch:
                    yield batch
                    
        elif file_ext == '.csv':
            # Read CSV file
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
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    
    def get_total_records(self, file_path: str) -> int:
        """Get total number of records in file"""
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return len(data)
        elif file_ext == '.csv':
            with open(file_path, 'r', encoding='utf-8') as f:
                # Count lines minus header
                return sum(1 for _ in f) - 1
        else:
            return 0


def main():
    """Main entry point for standalone execution"""
    import argparse
    import yaml
    
    parser = argparse.ArgumentParser(description='Iowa Asbestos Licenses Rerunnable Loader')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--data-file', help='Path to data file (overrides config)')
    parser.add_argument('--limit', type=int, help='Limit number of records (for testing)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--start-from', type=int, help='Start from record')
    parser.add_argument('--auto-skip', action='store_true', help='Auto skip already loaded records')
    parser.add_argument('--progress', action='store_true', help='Show progress bar')
    
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override with command line arguments
    if args.batch_size:
        config['processing']['batch_size'] = args.batch_size
    
    # Determine data file
    data_file = args.data_file or config['input']['file_path']
    
    # Expand environment variables in path
    data_file = os.path.expandvars(data_file)
    
    if not os.path.exists(data_file):
        print(f"Error: Data file not found: {data_file}")
        print("Run download script first: ./scripts/download_data.sh")
        sys.exit(1)
    
    # Create loader
    loader = IowaAsbestosLoader(config)
    
    # Get total records for progress tracking
    total = loader.get_total_records(data_file)
    print(f"Total records in file: {total:,}")
    
    # Auto-skip existing records if requested
    start_from = args.start_from if args.start_from is not None else 0
    if args.auto_skip:
        # Check for existing asbestos records
        try:
            with loader.propose_client._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(DISTINCT n.node_id) 
                        FROM nodes n 
                        JOIN attributes a ON n.node_id = a.node_id 
                        WHERE a.attribute_type = 'asbestos_license_type'
                    """)
                    existing = cur.fetchone()[0]
                    if existing > 0:
                        print(f"Found {existing} existing records, auto-skipping to position {existing}")
                        start_from = max(start_from, existing)
        except Exception as e:
            print(f"Warning: Could not check existing records: {e}")
    
    # Run loader
    result = loader.run(
        data_file,
        limit=args.limit,
        batch_size=config['processing']['batch_size'],
        start_from=start_from
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