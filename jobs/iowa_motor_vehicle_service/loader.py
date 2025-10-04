#!/usr/bin/env python3
"""
Iowa Motor Vehicle Service Contract Companies Loader
Loads licensed motor vehicle service contract companies from Iowa state data
"""
import sys
import os
import csv
from typing import Dict, List, Optional

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'examples'))

# Import the real ProposeAPIClient
from propose_api_client import ProposeAPIClient


class IowaMotorVehicleServiceLoader:
    """
    Loader for Iowa Motor Vehicle Service Contract Companies

    Data source: https://data.iowa.gov/Regulation/Motor-Vehicle-Service-Contract-Companies-Licensed-/j78q-bdp3
    Expected records: 211

    This loader demonstrates the propose API by creating relationships between:
    - Companies and their contact information (phone, email)
    - Companies and their mailing addresses
    - Companies and their DBA names
    """

    def __init__(self, config: dict):
        """Initialize the loader"""
        self.config = config
        self.connection = None  # Set by distributed worker
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'companies_created': 0,
            'phones_added': 0,
            'emails_added': 0,
            'addresses_added': 0,
            'dbas_added': 0
        }

    def run(
        self,
        file_path: str = None,
        limit: int = None,
        batch_size: int = 100,
        checkpoint_callback=None,
        log_callback=None,
        error_callback=None,
        **kwargs
    ):
        """Execute the loading process"""

        file_path = file_path or self.config.get('input', {}).get('file_path')
        limit = limit or self.config.get('processing', {}).get('limit')

        if not file_path:
            raise ValueError("file_path is required")

        if not self.connection:
            raise ValueError("database connection not set")

        # Setup real ProposeAPIClient with connection parameters
        conn_params = {
            'host': os.environ.get('DB_HOST', '98.85.51.253'),
            'database': os.environ.get('DB_NAME', 'graph_db'),
            'user': os.environ.get('DB_USER', 'graph_admin'),
            'password': os.environ.get('DB_PASSWORD'),
            'port': int(os.environ.get('DB_PORT', 5432))
        }
        client = ProposeAPIClient(conn_params)

        print(f"Loading Motor Vehicle Service Contract Companies from: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break

                try:
                    self._process_company(row, client)
                    self.stats['successful'] += 1

                except Exception as e:
                    self.stats['failed'] += 1
                    print(f"Error processing row {i}: {e}")
                    import traceback
                    traceback.print_exc()

                    if error_callback:
                        error_callback({
                            'source_record_id': row.get('company_number'),
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
                        'last_company_number': row.get('company_number')
                    })

                # Send log update
                if log_callback and i % 50 == 0:
                    log_callback({
                        'level': 'INFO',
                        'message': f'Progress: {self.stats["total_processed"]} companies processed',
                        'metadata': self.stats
                    })

        print(f"\nLoad Complete:")
        print(f"  Total Processed: {self.stats['total_processed']}")
        print(f"  Successful: {self.stats['successful']}")
        print(f"  Failed: {self.stats['failed']}")
        print(f"  Companies Created: {self.stats['companies_created']}")
        print(f"  Phone Numbers Added: {self.stats['phones_added']}")
        print(f"  Email Addresses Added: {self.stats['emails_added']}")
        print(f"  Addresses Added: {self.stats['addresses_added']}")
        print(f"  DBAs Added: {self.stats['dbas_added']}")

        return self.stats

    def _process_company(self, row: Dict, client: ProposeAPIClient):
        """Process a single company record using the propose API"""

        company_name = row.get('company_name', '').strip()
        if not company_name:
            return

        source_info = ('Iowa Motor Vehicle Service Database', 'iowa_mvs')
        company_number = row.get('company_number', '').strip()

        # Prepare company attributes
        company_attrs = {}
        if company_number:
            company_attrs['iowa_company_number'] = company_number

        # The propose API requires a relationship between two entities.
        # We'll propose the company by linking it to its address first,
        # which will create both the company and address nodes.

        # Add mailing address - this creates the company as a side effect
        address_created = self._add_mailing_address(company_name, row, client, source_info, company_attrs)

        if address_created:
            self.stats['companies_created'] += 1

            # Add DBA if present
            dba = row.get('company_d_b_a', '').strip()
            if dba and dba != company_name:
                dba_result = client.propose_fact(
                    source_entity=('Company', company_name),
                    target_entity=('Company', dba),
                    relationship='Partnership',  # DBA is a form of partnership/association
                    source_info=source_info,
                    source_attributes={'alias_type': 'DBA'},
                    provenance_confidence=0.95
                )
                if dba_result.success:
                    self.stats['dbas_added'] += 1

            # Add business phone (NEW FACT TYPE!)
            business_phone = row.get('business_phone', '').strip()
            if business_phone:
                phone_result = client.propose_fact(
                    source_entity=('Company', company_name),
                    target_entity=('Thing', f"Phone: {business_phone}"),  # Using Thing for phone
                    relationship='Located_At',  # Company is contactable at phone
                    source_info=source_info,
                    target_attributes={'phone_number': business_phone, 'contact_type': 'business'},
                    provenance_confidence=0.95
                )
                if phone_result.success:
                    self.stats['phones_added'] += 1

            # Add business email (NEW FACT TYPE!)
            business_email = row.get('business_email', '').strip()
            if business_email:
                email_result = client.propose_fact(
                    source_entity=('Company', company_name),
                    target_entity=('Thing', f"Email: {business_email}"),  # Using Thing for email
                    relationship='Located_At',  # Company is contactable at email
                    source_info=source_info,
                    target_attributes={'email_address': business_email, 'contact_type': 'business'},
                    provenance_confidence=0.95
                )
                if email_result.success:
                    self.stats['emails_added'] += 1

    def _add_mailing_address(self, company_name: str, row: Dict, client: ProposeAPIClient, source_info: tuple, company_attrs: Dict = None):
        """Create and link mailing address for the company"""

        address1 = row.get('mailing_address1', '').strip()
        city = row.get('mailing_city', '').strip()
        state = row.get('mailing_state', '').strip()
        zip_code = row.get('mailing_zip', '').strip()

        if not (address1 and city and state):
            return False

        # Construct full address string
        address_parts = [address1]

        address2 = row.get('mailing_address2', '').strip()
        if address2:
            address_parts.append(address2)

        address_parts.append(f"{city}, {state} {zip_code}")
        full_address = ", ".join(address_parts)

        # Propose the relationship between company and address
        address_attrs = {
            'street': address1,
            'city': city,
            'state': state
        }
        if address2:
            address_attrs['street2'] = address2
        if zip_code:
            address_attrs['zip'] = zip_code

        address_result = client.propose_fact(
            source_entity=('Company', company_name),
            target_entity=('Address', full_address),
            relationship='Located_At',
            source_info=source_info,
            source_attributes=company_attrs or {},  # Include company attributes here
            target_attributes=address_attrs,
            provenance_confidence=0.95
        )

        if address_result.success:
            self.stats['addresses_added'] += 1
            return True

        return False


if __name__ == '__main__':
    # For local testing
    import psycopg2

    loader = IowaMotorVehicleServiceLoader({
        'input': {
            'file_path': './jobs/iowa_motor_vehicle_service/data/motor_vehicle_service_companies.csv'
        },
        'processing': {
            'limit': 5  # Test with first 5 records
        }
    })

    # Connect to database
    loader.connection = psycopg2.connect(
        host=os.environ.get('DB_HOST', '98.85.51.253'),
        database=os.environ.get('DB_NAME', 'graph_db'),
        user=os.environ.get('DB_USER', 'graph_admin'),
        password=os.environ.get('DB_PASSWORD'),
        port=int(os.environ.get('DB_PORT', 5432))
    )

    results = loader.run()
    print(f"\nTest Results: {results}")

    loader.connection.close()
