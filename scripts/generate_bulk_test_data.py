#!/usr/bin/env python3
"""
Fast Bulk Test Data Generator - No LLM required
Generates substantial realistic test data for law firm conflict checking system.
"""

import random
import time
import math
from datetime import datetime, date, timedelta


class BulkTestDataGenerator:
    def __init__(self):
        self.entities = []
        self.relationships = []
        self.attributes = []
        self.provenance_records = []
        
        # Realistic data pools
        self.law_firm_names = [
            "Morrison & Associates LLP", "Bradley Legal Group", "Carter, Williams & Stone",
            "Thompson Law Partners", "Davis Corporate Counsel", "Mitchell & Hayes LLP",
            "Roberts Legal Services", "Anderson Business Law", "Wilson & Partners",
            "Taylor Employment Law", "Jackson & Associates", "Brown Legal Advisors"
        ]
        
        self.company_names = [
            "TechStart Industries Inc", "Global Manufacturing Corp", "Metro Financial Services LLC",
            "Apex Consulting Group", "Pioneer Energy Solutions", "Summit Healthcare Systems",
            "Quantum Software Technologies", "Atlantic Real Estate Holdings", "Pacific Trade Corporation",
            "Sunrise Logistics LLC", "Vantage Point Investments", "Meridian Construction Group"
        ]
        
        self.first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Nancy", "Daniel", "Lisa",
            "Matthew", "Betty", "Anthony", "Helen", "Mark", "Sandra", "Donald", "Donna"
        ]
        
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
            "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
        ]
        
        self.titles = [
            "Senior Partner", "Partner", "Associate", "Senior Associate", "Counsel", "Of Counsel",
            "CEO", "CFO", "CTO", "COO", "President", "Vice President", "Director", "Manager",
            "Senior Vice President", "General Counsel", "Chief Legal Officer", "Legal Director"
        ]
        
        # Source types for provenance (matching our schema)
        self.source_types = [
            "law_firm_records", "client_intake", "business_cards", "letterhead", "linkedin",
            "company_websites", "incorporation_docs", "public_records", "manual_entry",
            "background_checks", "commercial_databases", "contracts"
        ]
        
    def generate_ulid(self) -> str:
        """Generate a ULID (Universally Unique Lexicographically Sortable Identifier)."""
        ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        
        # Get current timestamp in milliseconds with small random offset
        timestamp = int(time.time() * 1000) + random.randint(0, 1000)
        
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
    
    def generate_random_date(self, start_year: int = 2018, end_year: int = 2024) -> str:
        """Generate a random date within the given range."""
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return str(start_date + timedelta(days=random_days))
    
    def create_provenance_record(self, asset_type: str, asset_id: str, source_name: str, source_type: str, confidence: float = 0.9):
        """Create a provenance record for an asset."""
        provenance = {
            "provenance_id": self.generate_ulid(),
            "asset_type": asset_type,
            "asset_id": asset_id,
            "source_name": source_name,
            "source_type": source_type,
            "confidence_score": confidence,
            "reliability_rating": "medium",
            "created_by": "bulk_generator"
        }
        self.provenance_records.append(provenance)
        return provenance["provenance_id"]
    
    def create_entity(self, entity_type: str, name: str) -> str:
        """Create a new entity and return its ULID."""
        entity_id = self.generate_ulid()
        
        entity = {
            "node_id": entity_id,
            "node_type": entity_type,
            "primary_name": name,
            "created_by": "bulk_generator"
        }
        
        self.entities.append(entity)
        
        # Create provenance record for this entity
        source_type = random.choice(self.source_types)
        source_name = f"{source_type}_source_{len(self.entities)}"
        self.create_provenance_record("node", entity_id, source_name, source_type)
        
        return entity_id
    
    def create_attribute(self, node_id: str, attr_type: str, value: str, confidence: float = 1.0):
        """Create an attribute for an entity."""
        attribute_id = self.generate_ulid()
        attribute = {
            "attribute_id": attribute_id,
            "node_id": node_id,
            "attribute_type": attr_type,
            "attribute_value": value,
            "confidence": confidence,
            "source": "generated"
        }
        self.attributes.append(attribute)
        
        # Create provenance record for this attribute
        source_type = random.choice(self.source_types)
        source_name = f"{source_type}_attr_{len(self.attributes)}"
        attr_confidence = confidence * random.uniform(0.8, 1.0)  # Slight variation
        self.create_provenance_record("attribute", attribute_id, source_name, source_type, attr_confidence)
    
    def create_relationship(self, source_id: str, target_id: str, rel_type: str,
                          strength: float = 1.0, start_date: str = None, end_date: str = None):
        """Create a relationship between two entities."""
        relationship_id = self.generate_ulid()
        relationship = {
            "relationship_id": relationship_id,
            "source_node_id": source_id,
            "target_node_id": target_id,
            "relationship_type": rel_type,
            "strength": strength,
            "valid_from": start_date or date.today().isoformat(),
            "valid_to": end_date,
            "metadata": None
        }
        self.relationships.append(relationship)
        
        # Create provenance record for this relationship
        source_type = random.choice(self.source_types)
        source_name = f"{source_type}_rel_{len(self.relationships)}"
        rel_confidence = strength * random.uniform(0.85, 1.0)  # Base on relationship strength
        self.create_provenance_record("relationship", relationship_id, source_name, source_type, rel_confidence)
    
    def generate_person_aliases(self, first_name: str, last_name: str) -> list:
        """Generate realistic name aliases for a person."""
        aliases = []
        
        # Common variations
        aliases.append(f"{first_name[0]}. {last_name}")  # J. Smith
        aliases.append(f"{last_name}, {first_name}")     # Smith, John
        
        # Nickname variations
        nicknames = {
            "Robert": "Bob", "William": "Bill", "Richard": "Rick", "James": "Jim",
            "Michael": "Mike", "Christopher": "Chris", "Matthew": "Matt",
            "Jennifer": "Jen", "Patricia": "Pat", "Elizabeth": "Liz", "Jessica": "Jess"
        }
        
        if first_name in nicknames:
            aliases.append(f"{nicknames[first_name]} {last_name}")
        
        return aliases
    
    def generate_law_firms(self, count: int = 8) -> dict:
        """Generate law firms with attorneys."""
        firms = {}
        
        for i in range(count):
            firm_name = random.choice(self.law_firm_names)
            if firm_name in [f["primary_name"] for f in self.entities if f["node_type"] == "Company"]:
                continue  # Skip duplicates
                
            firm_id = self.create_entity("Company", firm_name)
            firms[firm_name] = {"id": firm_id, "attorneys": []}
            
            # Add firm attributes
            self.create_attribute(firm_id, "category", "Law Firm")
            
            # Generate 3-8 attorneys per firm
            attorney_count = random.randint(3, 8)
            for j in range(attorney_count):
                first_name = random.choice(self.first_names)
                last_name = random.choice(self.last_names)
                attorney_name = f"{first_name} {last_name}"
                
                attorney_id = self.create_entity("Person", attorney_name)
                firms[firm_name]["attorneys"].append(attorney_id)
                
                # Add attorney attributes
                title = random.choice(["Senior Partner", "Partner", "Associate", "Senior Associate", "Counsel"])
                self.create_attribute(attorney_id, "title", title)
                
                # Add name aliases
                for alias in self.generate_person_aliases(first_name, last_name):
                    self.create_attribute(attorney_id, "nameAlias", alias)
                
                # Create employment relationship
                strength = 1.0 if "Partner" in title else 0.9
                start_date = self.generate_random_date(2015, 2023)
                self.create_relationship(attorney_id, firm_id, "Employment", strength, start_date)
        
        return firms
    
    def generate_companies(self, count: int = 12) -> dict:
        """Generate client companies with employees."""
        companies = {}
        
        for i in range(count):
            company_name = random.choice(self.company_names)
            if company_name in [c["primary_name"] for c in self.entities if c["node_type"] == "Company"]:
                continue  # Skip duplicates
                
            company_id = self.create_entity("Company", company_name)
            companies[company_name] = {"id": company_id, "employees": []}
            
            # Add company attributes
            industries = ["Technology", "Manufacturing", "Financial Services", "Healthcare", "Real Estate", "Energy"]
            self.create_attribute(company_id, "category", random.choice(industries))
            
            # Add company aliases
            aliases = []
            if " Inc" in company_name:
                aliases.append(company_name.replace(" Inc", ""))
            elif " Corp" in company_name:
                aliases.append(company_name.replace(" Corp", ""))
            elif " LLC" in company_name:
                aliases.append(company_name.replace(" LLC", ""))
            
            for alias in aliases:
                self.create_attribute(company_id, "nameAlias", alias)
            
            # Generate 2-6 key employees per company
            employee_count = random.randint(2, 6)
            executive_titles = ["CEO", "CFO", "CTO", "COO", "President", "Vice President", "General Counsel"]
            
            for j in range(employee_count):
                first_name = random.choice(self.first_names)
                last_name = random.choice(self.last_names)
                employee_name = f"{first_name} {last_name}"
                
                employee_id = self.create_entity("Person", employee_name)
                companies[company_name]["employees"].append(employee_id)
                
                # Add employee attributes
                title = executive_titles[j] if j < len(executive_titles) else "Director"
                self.create_attribute(employee_id, "title", title)
                
                # Add name aliases
                for alias in self.generate_person_aliases(first_name, last_name):
                    self.create_attribute(employee_id, "nameAlias", alias)
                
                # Create employment relationship
                strength = 1.0 if title in ["CEO", "CFO", "CTO"] else 0.9
                start_date = self.generate_random_date(2018, 2023)
                self.create_relationship(employee_id, company_id, "Employment", strength, start_date)
        
        return companies
    
    def generate_legal_relationships(self, firms: dict, companies: dict):
        """Create legal counsel relationships between firms and companies."""
        firm_list = list(firms.keys())
        company_list = list(companies.keys())
        
        # Each company gets 1-2 law firms
        for company_name, company_data in companies.items():
            num_firms = random.randint(1, 2)
            chosen_firms = random.sample(firm_list, num_firms)
            
            for firm_name in chosen_firms:
                # Choose 1-2 attorneys from the firm to represent this client
                attorneys = firms[firm_name]["attorneys"]
                chosen_attorneys = random.sample(attorneys, min(2, len(attorneys)))
                
                for attorney_id in chosen_attorneys:
                    self.create_relationship(
                        attorney_id, company_data["id"], "Legal_Counsel", 1.0,
                        self.generate_random_date(2020, 2024)
                    )
    
    def generate_family_relationships(self):
        """Generate some family relationships for conflict scenarios."""
        people = [e for e in self.entities if e["node_type"] == "Person"]
        
        # Create 3-5 family relationships
        for _ in range(random.randint(3, 5)):
            if len(people) >= 2:
                person1, person2 = random.sample(people, 2)
                self.create_relationship(
                    person1["node_id"], person2["node_id"], "Family", 0.9,
                    self.generate_random_date(2000, 2023)
                )
    
    def generate_business_relationships(self, companies: dict):
        """Generate subsidiary and partnership relationships."""
        company_ids = [c["id"] for c in companies.values()]
        
        # Create 2-3 subsidiary relationships
        for _ in range(random.randint(2, 3)):
            if len(company_ids) >= 2:
                parent, subsidiary = random.sample(company_ids, 2)
                self.create_relationship(
                    subsidiary, parent, "Subsidiary", 1.0,
                    self.generate_random_date(2018, 2023)
                )
        
        # Create 1-2 partnership relationships
        for _ in range(random.randint(1, 2)):
            if len(company_ids) >= 2:
                company1, company2 = random.sample(company_ids, 2)
                self.create_relationship(
                    company1, company2, "Partnership", 0.8,
                    self.generate_random_date(2020, 2024)
                )
    
    def generate_sql_output(self) -> str:
        """Generate SQL INSERT statements from the collected data."""
        sql_parts = []
        
        sql_parts.append("-- =============================================")
        sql_parts.append("-- Bulk Generated Test Data for Law Firm Conflict Checking")
        sql_parts.append(f"-- Generated on: {datetime.now().isoformat()}")
        sql_parts.append(f"-- Entities: {len(self.entities)}, Relationships: {len(self.relationships)}")
        sql_parts.append(f"-- Attributes: {len(self.attributes)}, Provenance: {len(self.provenance_records)}")
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
                f"INSERT INTO attributes (attribute_id, node_id, attribute_type, attribute_value, confidence, source) VALUES "
                f"('{attr['attribute_id']}', '{attr['node_id']}', '{attr['attribute_type']}', '{escaped_value}', {attr['confidence']}, '{attr['source']}');"
            )
        sql_parts.append("")
        
        # Generate relationships
        sql_parts.append("-- Insert relationships")
        for rel in self.relationships:
            valid_to = f"'{rel['valid_to']}'" if rel['valid_to'] else "NULL"
            metadata = f"'{rel['metadata']}'" if rel['metadata'] else "NULL"
            sql_parts.append(
                f"INSERT INTO relationships (relationship_id, source_node_id, target_node_id, relationship_type, strength, valid_from, valid_to, metadata) VALUES "
                f"('{rel['relationship_id']}', '{rel['source_node_id']}', '{rel['target_node_id']}', '{rel['relationship_type']}', {rel['strength']}, '{rel['valid_from']}', {valid_to}, {metadata});"
            )
        sql_parts.append("")
        
        # Generate provenance records
        sql_parts.append("-- Insert provenance records")
        for prov in self.provenance_records:
            sql_parts.append(
                f"INSERT INTO provenance (provenance_id, asset_type, asset_id, source_name, source_type, confidence_score, reliability_rating, created_by, data_obtained_at) VALUES "
                f"('{prov['provenance_id']}', '{prov['asset_type']}', '{prov['asset_id']}', '{prov['source_name']}', '{prov['source_type']}', {prov['confidence_score']}, '{prov['reliability_rating']}', '{prov['created_by']}', CURRENT_TIMESTAMP);"
            )
        sql_parts.append("")
        
        sql_parts.append("COMMIT;")
        sql_parts.append("")
        sql_parts.append("-- Verification queries")
        sql_parts.append("SELECT 'Generated entities: ' || COUNT(*) FROM nodes WHERE created_by = 'bulk_generator';")
        sql_parts.append("SELECT 'Generated attributes: ' || COUNT(*) FROM attributes WHERE source = 'generated';")
        sql_parts.append("SELECT 'Generated relationships: ' || COUNT(*) FROM relationships WHERE created_by = 'bulk_generator';")
        sql_parts.append("SELECT 'Generated provenance records: ' || COUNT(*) FROM provenance WHERE created_by = 'bulk_generator';")
        
        return "\n".join(sql_parts)
    
    def generate_comprehensive_dataset(self):
        """Generate a comprehensive dataset with all relationship types."""
        print("🏗️  Generating law firms and attorneys...")
        firms = self.generate_law_firms(8)
        
        print("🏢 Generating companies and employees...")
        companies = self.generate_companies(12)
        
        print("⚖️  Creating legal counsel relationships...")
        self.generate_legal_relationships(firms, companies)
        
        print("👨‍👩‍👧‍👦 Adding family relationships...")
        self.generate_family_relationships()
        
        print("🤝 Creating business relationships...")
        self.generate_business_relationships(companies)
        
        print(f"✅ Dataset complete:")
        print(f"   - {len(self.entities)} entities")
        print(f"   - {len(self.attributes)} attributes")
        print(f"   - {len(self.relationships)} relationships")
        print(f"   - {len(self.provenance_records)} provenance records")


def main():
    generator = BulkTestDataGenerator()
    
    print("🚀 Generating comprehensive bulk test data...")
    generator.generate_comprehensive_dataset()
    
    print("\n📝 Creating SQL output...")
    sql_output = generator.generate_sql_output()
    
    output_file = "db/test-data/generated/bulk_test_data.sql"
    with open(output_file, 'w') as f:
        f.write(sql_output)
    
    print(f"✅ SQL output written to: {output_file}")
    print(f"📊 Ready to load with: ./scripts/run_sql.sh {output_file}")


if __name__ == "__main__":
    main()