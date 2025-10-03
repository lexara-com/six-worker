"""
Data validation utilities for robust data loading
"""
import re
from typing import Dict, List, Any, Optional
from datetime import datetime


class DataValidator:
    """Validates data before processing"""

    @staticmethod
    def validate_company_name(name: str) -> List[str]:
        """Validate company name"""
        errors = []

        if not name or not name.strip():
            errors.append("Company name is empty")
            return errors

        if len(name) > 500:
            errors.append(f"Company name too long ({len(name)} chars, max 500)")

        # Check for suspicious patterns
        if re.match(r'^[\s\W]+$', name):
            errors.append("Company name contains only special characters")

        return errors

    @staticmethod
    def validate_date(date_str: str, field_name: str = "date") -> List[str]:
        """Validate date string"""
        errors = []

        if not date_str:
            return errors  # Optional field

        # Try common date formats
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y%m%d'
        ]

        valid = False
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Check reasonable date range
                if parsed_date.year < 1800 or parsed_date.year > 2100:
                    errors.append(f"{field_name} has unreasonable year: {parsed_date.year}")
                valid = True
                break
            except ValueError:
                continue

        if not valid:
            errors.append(f"{field_name} has invalid format: {date_str}")

        return errors

    @staticmethod
    def validate_address(address: Dict[str, Any]) -> List[str]:
        """Validate address components"""
        errors = []

        city = address.get('city', '')
        state = address.get('state', '')

        # City validation
        if city and len(city) > 100:
            errors.append(f"City name too long ({len(city)} chars)")

        # State validation
        if state and len(state) > 2:
            # Should be 2-letter state code
            if len(state) > 50:
                errors.append(f"State value too long ({len(state)} chars)")

        # Zip code validation
        zip_code = address.get('zip', '')
        if zip_code:
            # US zip: 5 digits or 5+4 format
            if not re.match(r'^\d{5}(-\d{4})?$', zip_code):
                errors.append(f"Invalid zip code format: {zip_code}")

        return errors

    @staticmethod
    def validate_coordinates(coords: Optional[Dict]) -> List[str]:
        """Validate geographic coordinates"""
        errors = []

        if not coords:
            return errors

        if 'coordinates' in coords:
            coords_list = coords['coordinates']
            if len(coords_list) != 2:
                errors.append(f"Coordinates must have 2 values, got {len(coords_list)}")
            else:
                lon, lat = coords_list
                if not (-180 <= lon <= 180):
                    errors.append(f"Invalid longitude: {lon}")
                if not (-90 <= lat <= 90):
                    errors.append(f"Invalid latitude: {lat}")

        return errors

    @staticmethod
    def validate_iowa_business_record(record: Dict[str, Any]) -> List[str]:
        """Validate complete Iowa business record"""
        errors = []

        # Required fields
        if not record.get('legal_name'):
            errors.append("Missing required field: legal_name")
        else:
            errors.extend(DataValidator.validate_company_name(record['legal_name']))

        if not record.get('corp_type'):
            errors.append("Missing required field: corp_type")

        # Date validation
        if record.get('effective_date'):
            errors.extend(DataValidator.validate_date(
                record['effective_date'],
                'effective_date'
            ))

        # Address validation
        if record.get('home_office'):
            errors.extend(DataValidator.validate_address(record['home_office']))

        # Coordinates validation
        if record.get('home_office', {}).get('coordinates'):
            errors.extend(DataValidator.validate_coordinates(
                record['home_office']['coordinates']
            ))

        # Registered agent validation
        if record.get('registered_agent', {}).get('address'):
            errors.extend(DataValidator.validate_address(
                record['registered_agent']
            ))

        return errors

    @staticmethod
    def sanitize_string(value: str, max_length: int = 500) -> str:
        """Sanitize string value"""
        if not value:
            return ""

        # Remove null bytes
        value = value.replace('\x00', '')

        # Trim whitespace
        value = value.strip()

        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length]

        return value
