#!/usr/bin/env python3
"""
LLM-Powered Test Data Generator for Law Firm Conflict Checking System

This script uses an LLM to generate realistic test scenarios including:
- Law firms with attorneys and staff
- Client companies with employees and executives
- Family relationships and business connections
- Complex conflict scenarios
- Historical relationships and temporal data

Usage:
    python scripts/generate_test_data.py --scenarios 10 --output db/test-data/generated_test_data.sql
"""

import json
import uuid
import random
import time
import math
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import argparse
import sys
import os

try:
    import anthropic
except ImportError:
    print("Error: anthropic library not installed. Install with: pip install anthropic")
    sys.exit(1)


class TestDataGenerator:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the test data generator with Anthropic API key."""
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("Anthropic API key required. Set ANTHROPIC_API_KEY environment variable or pass api_key parameter.")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.entities = []
        self.relationships = []
        self.attributes = []
        self.conflict_matrix = []
        
    def generate_scenario_prompt(self, scenario_type: str) -> str:
        """Generate prompts for different types of law firm scenarios."""
        
        base_prompt = """You are generating realistic test data for a law firm conflict checking system. 
        Create a detailed scenario with specific names, companies, relationships, and potential conflicts.
        
        Include:
        - Full legal names (not generic names like "John Doe")
        - Company names with proper legal suffixes (Inc, LLC, Corp, etc.)
        - Professional titles and roles
        - Family relationships where relevant
        - Business relationships (employment, ownership, partnerships)
        - Potential conflict situations
        - Time periods for relationships (start/end dates)
        
        Format your response as structured data that can be parsed."""
        
        scenario_prompts = {
            "law_firm": """
            Create a mid-sized law firm scenario with:
            - Law firm name and 5-8 attorneys (partners, associates, counsel)
            - Support staff (paralegals, administrators)
            - Practice areas and specializations
            - Office locations
            - Professional relationships and hierarchies
            """,
            
            "corporate_client": """
            Create a corporate client scenario with:
            - Company name and industry
            - Executive team (CEO, CFO, CTO, etc.)
            - Board of directors
            - Key employees in different departments
            - Corporate structure (subsidiaries, parent companies)
            - Business relationships with other entities
            """,
            
            "family_business": """
            Create a family business scenario with:
            - Family-owned company with multiple generations
            - Family members in various roles
            - Family relationships (spouses, children, siblings, cousins)
            - Business ownership structures
            - Potential succession issues
            - External business partners
            """,
            
            "litigation_matter": """
            Create a complex litigation scenario with:
            - Multiple parties (plaintiffs, defendants)
            - Law firms representing different sides
            - Corporate entities involved
            - Individual executives and employees
            - Insurance companies
            - Expert witnesses
            - Court jurisdiction and case details
            """,
            
            "merger_acquisition": """
            Create a merger & acquisition scenario with:
            - Acquiring company and target company
            - Investment banks and advisors
            - Legal counsel for each side
            - Regulatory bodies
            - Key executives involved
            - Due diligence teams
            - Potential conflicts of interest
            """,
            
            "real_estate": """
            Create a commercial real estate scenario with:
            - Property developers and investors
            - Real estate companies and brokers
            - Construction companies and contractors
            - Financing institutions and lenders
            - Local government entities
            - Environmental consultants
            - Property management companies
            """
        }
        
        return base_prompt + scenario_prompts.get(scenario_type, scenario_prompts["corporate_client"])
    
    def call_llm(self, prompt: str) -> str:
        """Make API call to Anthropic Claude."""
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                temperature=0.8,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            print(f"Error calling LLM: {e}")
            return ""
    
    def parse_llm_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM response and extract structured data."""
        # This is a simplified parser - in production, you'd want more robust parsing
        # For now, we'll use a structured prompt to get JSON-like responses
        
        parsing_prompt = f"""
        Extract the following information from this scenario and format as JSON:
        
        {{
            "law_firms": [
                {{
                    "name": "Firm Name",
                    "attorneys": [
                        {{
                            "name": "Full Name",
                            "title": "Partner/Associate/Counsel",
                            "aliases": ["Nickname", "Initials"],
                            "specialization": "Practice area"
                        }}
                    ]
                }}
            ],
            "companies": [
                {{
                    "name": "Company Name Inc",
                    "industry": "Industry",
                    "employees": [
                        {{
                            "name": "Full Name",
                            "title": "CEO/CFO/etc",
                            "aliases": ["Nickname"]
                        }}
                    ]
                }}
            ],
            "relationships": [
                {{
                    "source": "Person/Company Name",
                    "target": "Person/Company Name", 
                    "type": "Employment/Family/Legal_Counsel/etc",
                    "strength": 0.9,
                    "start_date": "2020-01-01",
                    "end_date": null
                }}
            ],
            "conflicts": [
                {{
                    "description": "Why this is a conflict",
                    "entities": ["Entity 1", "Entity 2"],
                    "conflict_type": "Type of conflict"
                }}
            ]
        }}
        
        Original scenario:
        {response}
        """
        
        parsed_response = self.call_llm(parsing_prompt)
        
        try:
            # Try to extract JSON from the response
            start = parsed_response.find('{')
            end = parsed_response.rfind('}') + 1
            if start >= 0 and end > start:
                json_str = parsed_response[start:end]
                return json.loads(json_str)
        except json.JSONDecodeError:
            print("Warning: Could not parse LLM response as JSON")
            
        return {"law_firms": [], "companies": [], "relationships": [], "conflicts": []}
    
    def generate_ulid(self) -> str:
        """Generate a ULID (Universally Unique Lexicographically Sortable Identifier)."""
        # ULID format: 10 characters timestamp + 16 characters randomness
        # Timestamp: milliseconds since Unix epoch in Crockford Base32
        # Randomness: 80 bits of randomness in Crockford Base32
        
        # Crockford Base32 alphabet (excludes I, L, O, U to avoid confusion)
        ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        
        # Get current timestamp in milliseconds
        timestamp = int(time.time() * 1000)
        
        # Encode timestamp (48 bits)
        time_part = ""
        for _ in range(10):
            time_part = ENCODING[timestamp % 32] + time_part
            timestamp //= 32
        
        # Generate random part (80 bits)
        random_part = ""
        for _ in range(16):
            random_part += ENCODING[random.randint(0, 31)]
        
        return time_part + random_part
    
    def generate_uuid(self) -> str:
        """Generate a UUID string (keeping for backward compatibility)."""
        return str(uuid.uuid4())
    
    def normalize_name(self, name: str) -> str:
        """Normalize name for database storage."""
        return name.lower().strip()
    
    def generate_random_date(self, start_year: int = 2018, end_year: int = 2024) -> str:
        """Generate a random date within the given range."""
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return str(start_date + timedelta(days=random_days))
    
    def create_entity(self, entity_type: str, name: str, created_by: str = "test_generator") -> str:
        """Create a new entity and return its ULID."""
        entity_id = self.generate_ulid()
        
        entity = {
            "node_id": entity_id,
            "node_type": entity_type,
            "primary_name": name,
            "created_by": created_by
        }
        
        self.entities.append(entity)
        return entity_id
    
    def create_attribute(self, node_id: str, attr_type: str, value: str, 
                        confidence: float = 1.0, source: str = "generated"):
        """Create an attribute for an entity."""
        attribute = {
            "node_id": node_id,
            "attribute_type": attr_type,
            "attribute_value": value,
            "confidence": confidence,
            "source": source
        }
        self.attributes.append(attribute)
    
    def create_relationship(self, source_id: str, target_id: str, rel_type: str,
                          strength: float = 1.0, start_date: str = None, end_date: str = None,
                          metadata: Dict = None):
        """Create a relationship between two entities."""
        relationship = {
            "source_node_id": source_id,
            "target_node_id": target_id,
            "relationship_type": rel_type,
            "strength": strength,
            "valid_from": start_date or date.today().isoformat(),
            "valid_to": end_date,
            "metadata": json.dumps(metadata) if metadata else None
        }
        self.relationships.append(relationship)
    
    def process_scenario_data(self, scenario_data: Dict[str, Any]) -> None:
        """Process parsed scenario data and create entities/relationships."""
        entity_map = {}  # Map names to UUIDs
        
        # Create law firms and attorneys
        for firm_data in scenario_data.get("law_firms", []):
            firm_id = self.create_entity("Company", firm_data["name"])
            entity_map[firm_data["name"]] = firm_id
            
            for attorney in firm_data.get("attorneys", []):
                attorney_id = self.create_entity("Person", attorney["name"])
                entity_map[attorney["name"]] = attorney_id
                
                # Add attributes
                self.create_attribute(attorney_id, "title", attorney.get("title", "Attorney"))
                for alias in attorney.get("aliases", []):
                    self.create_attribute(attorney_id, "nameAlias", alias)
                
                if attorney.get("specialization"):
                    self.create_attribute(attorney_id, "category", attorney["specialization"])
                
                # Create employment relationship
                self.create_relationship(
                    attorney_id, firm_id, "Employment", 
                    strength=0.9 if "Partner" in attorney.get("title", "") else 0.8,
                    start_date=self.generate_random_date(2015, 2023)
                )
        
        # Create companies and employees
        for company_data in scenario_data.get("companies", []):
            if company_data["name"] not in entity_map:
                company_id = self.create_entity("Company", company_data["name"])
                entity_map[company_data["name"]] = company_id
                
                if company_data.get("industry"):
                    self.create_attribute(company_id, "category", company_data["industry"])
            else:
                company_id = entity_map[company_data["name"]]
            
            for employee in company_data.get("employees", []):
                if employee["name"] not in entity_map:
                    employee_id = self.create_entity("Person", employee["name"])
                    entity_map[employee["name"]] = employee_id
                else:
                    employee_id = entity_map[employee["name"]]
                
                # Add attributes
                self.create_attribute(employee_id, "title", employee.get("title", "Employee"))
                for alias in employee.get("aliases", []):
                    self.create_attribute(employee_id, "nameAlias", alias)
                
                # Create employment relationship
                strength = 1.0 if any(title in employee.get("title", "") for title in ["CEO", "CFO", "CTO", "President"]) else 0.9
                self.create_relationship(
                    employee_id, company_id, "Employment",
                    strength=strength,
                    start_date=self.generate_random_date(2018, 2023)
                )
        
        # Create explicit relationships
        for rel_data in scenario_data.get("relationships", []):
            source_name = rel_data.get("source")
            target_name = rel_data.get("target")
            
            if source_name in entity_map and target_name in entity_map:
                self.create_relationship(
                    entity_map[source_name],
                    entity_map[target_name],
                    rel_data.get("type", "Related"),
                    strength=rel_data.get("strength", 0.8),
                    start_date=rel_data.get("start_date"),
                    end_date=rel_data.get("end_date")
                )
    
    def generate_test_scenarios(self, num_scenarios: int = 5) -> None:
        """Generate multiple test scenarios using LLM."""
        scenario_types = ["law_firm", "corporate_client", "family_business", 
                         "litigation_matter", "merger_acquisition", "real_estate"]
        
        for i in range(num_scenarios):
            scenario_type = random.choice(scenario_types)
            print(f"Generating scenario {i+1}/{num_scenarios}: {scenario_type}")
            
            prompt = self.generate_scenario_prompt(scenario_type)
            response = self.call_llm(prompt)
            
            if response:
                scenario_data = self.parse_llm_response(response)
                self.process_scenario_data(scenario_data)
                print(f"  - Created {len(scenario_data.get('law_firms', []))} law firms")
                print(f"  - Created {len(scenario_data.get('companies', []))} companies")
                print(f"  - Created {len(scenario_data.get('relationships', []))} relationships")
    
    def generate_sql_output(self) -> str:
        """Generate SQL INSERT statements from the collected data."""
        sql_parts = []
        
        sql_parts.append("-- =============================================")
        sql_parts.append("-- LLM-Generated Test Data for Law Firm Conflict Checking")
        sql_parts.append(f"-- Generated on: {datetime.now().isoformat()}")
        sql_parts.append("-- =============================================")
        sql_parts.append("")
        sql_parts.append("BEGIN;")
        sql_parts.append("")
        
        # Generate nodes
        sql_parts.append("-- Insert nodes (entities)")
        for entity in self.entities:
            escaped_name = entity['primary_name'].replace("'", "''")
            sql_parts.append(
                f"INSERT INTO nodes (node_id, node_type, primary_name, created_by) VALUES "
                f"('{entity['node_id']}', '{entity['node_type']}', '{escaped_name}', '{entity['created_by']}');"
            )
        sql_parts.append("")
        
        # Generate attributes
        sql_parts.append("-- Insert attributes")
        for attr in self.attributes:
            escaped_value = attr['attribute_value'].replace("'", "''")
            sql_parts.append(
                f"INSERT INTO attributes (node_id, attribute_type, attribute_value, confidence, source) VALUES "
                f"('{attr['node_id']}', '{attr['attribute_type']}', '{escaped_value}', {attr['confidence']}, '{attr['source']}');"
            )
        sql_parts.append("")
        
        # Generate relationships
        sql_parts.append("-- Insert relationships")
        for rel in self.relationships:
            valid_to = f"'{rel['valid_to']}'" if rel['valid_to'] else "NULL"
            metadata = f"'{rel['metadata']}'" if rel['metadata'] else "NULL"
            sql_parts.append(
                f"INSERT INTO relationships (source_node_id, target_node_id, relationship_type, strength, valid_from, valid_to, metadata) VALUES "
                f"('{rel['source_node_id']}', '{rel['target_node_id']}', '{rel['relationship_type']}', {rel['strength']}, '{rel['valid_from']}', {valid_to}, {metadata});"
            )
        sql_parts.append("")
        
        sql_parts.append("COMMIT;")
        sql_parts.append("")
        sql_parts.append("-- Verification queries")
        sql_parts.append("SELECT 'Generated entities: ' || COUNT(*) FROM nodes WHERE created_by = 'test_generator';")
        sql_parts.append("SELECT 'Generated attributes: ' || COUNT(*) FROM attributes WHERE source = 'generated';")
        sql_parts.append("SELECT 'Generated relationships: ' || COUNT(*) FROM relationships;")
        
        return "\n".join(sql_parts)


def main():
    parser = argparse.ArgumentParser(description="Generate test data for law firm conflict checking system")
    parser.add_argument("--scenarios", type=int, default=5, help="Number of scenarios to generate")
    parser.add_argument("--output", type=str, default="generated_test_data.sql", help="Output SQL file path")
    parser.add_argument("--api-key", type=str, help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    
    args = parser.parse_args()
    
    try:
        generator = TestDataGenerator(api_key=args.api_key)
        
        print(f"Generating {args.scenarios} test scenarios...")
        generator.generate_test_scenarios(args.scenarios)
        
        print(f"Generated:")
        print(f"  - {len(generator.entities)} entities")
        print(f"  - {len(generator.attributes)} attributes")
        print(f"  - {len(generator.relationships)} relationships")
        
        sql_output = generator.generate_sql_output()
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else '.', exist_ok=True)
        
        with open(args.output, 'w') as f:
            f.write(sql_output)
        
        print(f"SQL output written to: {args.output}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()