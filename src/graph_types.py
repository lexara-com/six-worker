"""
Type definitions and enums for the Six Worker graph database system.

This module provides comprehensive enums and type definitions to ensure consistency
across the codebase and prevent typos in relationship types, node types, and categories.
"""

from enum import Enum
from typing import Set, Dict, Tuple, List
from dataclasses import dataclass


class NodeType(Enum):
    """Valid node types in the graph database"""
    
    # People and Organizations
    PERSON = "Person"
    COMPANY = "Company"
    LAW_FIRM = "LawFirm"  # Special type of company
    
    # Geographic Entities
    COUNTRY = "Country"
    STATE = "State" 
    CITY = "City"
    COUNTY = "County"
    ZIPCODE = "ZipCode"
    ADDRESS = "Address"
    
    # Abstract Entities
    THING = "Thing"
    EVENT = "Event"
    
    @classmethod
    def get_geographic_types(cls) -> Set['NodeType']:
        """Get all geographic node types"""
        return {cls.COUNTRY, cls.STATE, cls.CITY, cls.COUNTY, cls.ZIPCODE, cls.ADDRESS}
    
    @classmethod
    def get_entity_types(cls) -> Set['NodeType']:
        """Get all entity types (non-geographic)"""
        return {cls.PERSON, cls.COMPANY, cls.THING, cls.EVENT}


class EntityClass(Enum):
    """Entity classification for provenance and authority"""
    
    FACT_BASED = "fact_based"      # Discovered through data ingestion
    REFERENCE = "reference"        # Pre-established authoritative entities
    COMPUTED = "computed"          # Derived through analysis


class RelationshipType(Enum):
    """All valid relationship types in the system"""
    
    # ====================
    # Legal Relationships
    # ====================
    LEGAL_COUNSEL = "Legal_Counsel"
    OPPOSING_COUNSEL = "Opposing_Counsel"  
    CLIENT_RELATIONSHIP = "Client_Relationship"
    CONFLICT = "Conflict"
    
    # Legal Conflict Sub-types
    LEGAL_COUNSEL_CONFLICT = "Legal_Counsel_Conflict"
    FAMILY_BUSINESS_CONFLICT = "Family_Business_Conflict"
    DIRECT_REPRESENTATION_CONFLICT = "Direct_Representation_Conflict"
    
    # Client Relationships (aliases/variations)
    CLIENT = "Client"  # Alternative to CLIENT_RELATIONSHIP
    OPPOSING_PARTY = "Opposing_Party"  # Alternative to OPPOSING_COUNSEL
    POTENTIAL_CLIENT = "Potential_Client"
    
    # ====================
    # Geographic Relationships (Bidirectional)
    # ====================
    LOCATED_IN = "Located_In"
    CONTAINS = "Contains"
    LOCATED_AT = "Located_At"
    LOCATION_OF = "Location_Of"
    
    # ====================
    # Corporate/Business Relationships
    # ====================
    INCORPORATED_IN = "Incorporated_In"
    REGISTERED_AGENT = "Registered_Agent"
    SUBSIDIARY = "Subsidiary"
    OWNERSHIP = "Ownership"
    
    # ====================
    # Professional Relationships
    # ====================
    BOARD_MEMBER = "Board_Member"
    EMPLOYMENT = "Employment"
    PARTNERSHIP = "Partnership"
    
    # ====================
    # Personal Relationships
    # ====================
    FAMILY = "Family"
    
    # ====================
    # Activity/Event Relationships
    # ====================
    PARTICIPATION = "Participation"
    ORGANIZER = "Organizer"
    
    # ====================
    # Legacy/System Relationships
    # ====================
    LOCATION = "Location"  # Legacy - use LOCATED_IN/LOCATED_AT instead
    ADVISORY_BOARD = "Advisory_Board"
    TEST_RELATIONSHIP = "Test_Relationship"
    
    @classmethod
    def get_legal_types(cls) -> Set['RelationshipType']:
        """Get all legal relationship types"""
        return {
            cls.LEGAL_COUNSEL, cls.OPPOSING_COUNSEL, cls.CLIENT_RELATIONSHIP,
            cls.CONFLICT, cls.LEGAL_COUNSEL_CONFLICT, cls.FAMILY_BUSINESS_CONFLICT,
            cls.DIRECT_REPRESENTATION_CONFLICT
        }
    
    @classmethod
    def get_geographic_types(cls) -> Set['RelationshipType']:
        """Get all geographic relationship types"""
        return {
            cls.LOCATED_IN, cls.CONTAINS, cls.LOCATED_AT, cls.LOCATION_OF, cls.LOCATION
        }
    
    @classmethod
    def get_corporate_types(cls) -> Set['RelationshipType']:
        """Get all corporate/business relationship types"""
        return {
            cls.INCORPORATED_IN, cls.REGISTERED_AGENT, cls.SUBSIDIARY, cls.OWNERSHIP
        }
    
    @classmethod
    def get_professional_types(cls) -> Set['RelationshipType']:
        """Get all professional relationship types"""
        return {
            cls.BOARD_MEMBER, cls.EMPLOYMENT, cls.PARTNERSHIP, cls.ADVISORY_BOARD
        }
    
    @classmethod
    def get_bidirectional_pairs(cls) -> Set[Tuple['RelationshipType', 'RelationshipType']]:
        """Get pairs of relationship types that are bidirectional"""
        return {
            (cls.LOCATED_IN, cls.CONTAINS),
            (cls.LOCATED_AT, cls.LOCATION_OF),
            (cls.PARTNERSHIP, cls.PARTNERSHIP),  # Self-bidirectional
            (cls.FAMILY, cls.FAMILY),  # Self-bidirectional
            (cls.CONFLICT, cls.CONFLICT),  # Self-bidirectional
        }
    
    @classmethod
    def get_conflict_pairs(cls) -> Set[Tuple['RelationshipType', 'RelationshipType']]:
        """Get pairs of relationship types that create conflicts"""
        return {
            (cls.LEGAL_COUNSEL, cls.OPPOSING_COUNSEL),
            (cls.OPPOSING_COUNSEL, cls.LEGAL_COUNSEL),
        }


class RelationshipCategory(Enum):
    """Categories for grouping relationship types"""
    
    LEGAL = "Legal"
    GEOGRAPHIC = "Geographic" 
    PHYSICAL = "Physical"
    CORPORATE = "Corporate"
    PROFESSIONAL = "Professional"
    PERSONAL = "Personal"
    ACTIVITY = "Activity"
    FINANCIAL = "Financial"


@dataclass
class RelationshipDefinition:
    """Complete definition of a relationship type"""
    
    type_name: RelationshipType
    description: str
    category: RelationshipCategory
    is_bidirectional: bool
    conflicts_with: List[RelationshipType] = None
    
    def __post_init__(self):
        if self.conflicts_with is None:
            self.conflicts_with = []


class RelationshipRegistry:
    """Registry of all relationship type definitions"""
    
    # Define all relationship types with their metadata
    DEFINITIONS = {
        # Legal Relationships
        RelationshipType.LEGAL_COUNSEL: RelationshipDefinition(
            RelationshipType.LEGAL_COUNSEL,
            "Attorney represents Entity",
            RelationshipCategory.LEGAL,
            False,
            [RelationshipType.OPPOSING_COUNSEL]
        ),
        RelationshipType.OPPOSING_COUNSEL: RelationshipDefinition(
            RelationshipType.OPPOSING_COUNSEL,
            "Attorney represents opposing party",
            RelationshipCategory.LEGAL,
            False,
            [RelationshipType.LEGAL_COUNSEL]
        ),
        RelationshipType.CLIENT_RELATIONSHIP: RelationshipDefinition(
            RelationshipType.CLIENT_RELATIONSHIP,
            "Professional service relationship",
            RelationshipCategory.LEGAL,
            False
        ),
        RelationshipType.CONFLICT: RelationshipDefinition(
            RelationshipType.CONFLICT,
            "Adversarial relationship",
            RelationshipCategory.LEGAL,
            True
        ),
        
        # Geographic Relationships
        RelationshipType.LOCATED_IN: RelationshipDefinition(
            RelationshipType.LOCATED_IN,
            "Entity is located within geographic area",
            RelationshipCategory.GEOGRAPHIC,
            True  # Bidirectional with CONTAINS
        ),
        RelationshipType.CONTAINS: RelationshipDefinition(
            RelationshipType.CONTAINS,
            "Geographic area contains entity",
            RelationshipCategory.GEOGRAPHIC,
            True  # Bidirectional with LOCATED_IN
        ),
        RelationshipType.LOCATED_AT: RelationshipDefinition(
            RelationshipType.LOCATED_AT,
            "Entity is located at specific address",
            RelationshipCategory.GEOGRAPHIC,
            True  # Bidirectional with LOCATION_OF
        ),
        RelationshipType.LOCATION_OF: RelationshipDefinition(
            RelationshipType.LOCATION_OF,
            "Address is location of entity",
            RelationshipCategory.GEOGRAPHIC,
            True  # Bidirectional with LOCATED_AT
        ),
        
        # Corporate Relationships
        RelationshipType.INCORPORATED_IN: RelationshipDefinition(
            RelationshipType.INCORPORATED_IN,
            "Company incorporated in jurisdiction",
            RelationshipCategory.CORPORATE,
            False
        ),
        RelationshipType.REGISTERED_AGENT: RelationshipDefinition(
            RelationshipType.REGISTERED_AGENT,
            "Legal representative for corporation",
            RelationshipCategory.CORPORATE,
            False
        ),
        RelationshipType.SUBSIDIARY: RelationshipDefinition(
            RelationshipType.SUBSIDIARY,
            "Company is subsidiary of another",
            RelationshipCategory.CORPORATE,
            False
        ),
        RelationshipType.OWNERSHIP: RelationshipDefinition(
            RelationshipType.OWNERSHIP,
            "Entity owns another entity",
            RelationshipCategory.FINANCIAL,
            False
        ),
        
        # Professional Relationships
        RelationshipType.BOARD_MEMBER: RelationshipDefinition(
            RelationshipType.BOARD_MEMBER,
            "Person serves on Company board",
            RelationshipCategory.PROFESSIONAL,
            False
        ),
        RelationshipType.EMPLOYMENT: RelationshipDefinition(
            RelationshipType.EMPLOYMENT,
            "Person works for Company",
            RelationshipCategory.PROFESSIONAL,
            False
        ),
        RelationshipType.PARTNERSHIP: RelationshipDefinition(
            RelationshipType.PARTNERSHIP,
            "Business partnership relationship",
            RelationshipCategory.PROFESSIONAL,
            True
        ),
        
        # Personal Relationships
        RelationshipType.FAMILY: RelationshipDefinition(
            RelationshipType.FAMILY,
            "Family relationship",
            RelationshipCategory.PERSONAL,
            True
        ),
        
        # Activity Relationships
        RelationshipType.PARTICIPATION: RelationshipDefinition(
            RelationshipType.PARTICIPATION,
            "Entity participates in Event",
            RelationshipCategory.ACTIVITY,
            False
        ),
        RelationshipType.ORGANIZER: RelationshipDefinition(
            RelationshipType.ORGANIZER,
            "Entity organizes Event",
            RelationshipCategory.ACTIVITY,
            False
        ),
        
        # Legacy
        RelationshipType.LOCATION: RelationshipDefinition(
            RelationshipType.LOCATION,
            "Entity is located at Place (legacy)",
            RelationshipCategory.PHYSICAL,
            False
        ),
    }
    
    @classmethod
    def get_definition(cls, relationship_type: RelationshipType) -> RelationshipDefinition:
        """Get the definition for a relationship type"""
        return cls.DEFINITIONS.get(relationship_type)
    
    @classmethod
    def get_by_category(cls, category: RelationshipCategory) -> List[RelationshipDefinition]:
        """Get all relationship definitions in a category"""
        return [
            definition for definition in cls.DEFINITIONS.values()
            if definition.category == category
        ]
    
    @classmethod
    def is_bidirectional(cls, relationship_type: RelationshipType) -> bool:
        """Check if a relationship type is bidirectional"""
        definition = cls.get_definition(relationship_type)
        return definition.is_bidirectional if definition else False
    
    @classmethod
    def get_conflicts(cls, relationship_type: RelationshipType) -> List[RelationshipType]:
        """Get relationship types that conflict with the given type"""
        definition = cls.get_definition(relationship_type)
        return definition.conflicts_with if definition else []
    
    @classmethod
    def validate_relationship_type(cls, relationship_type: str) -> bool:
        """Validate that a relationship type string is valid"""
        try:
            RelationshipType(relationship_type)
            return True
        except ValueError:
            return False


# Utility functions for backward compatibility
def validate_node_type(node_type: str) -> bool:
    """Validate that a node type string is valid"""
    try:
        NodeType(node_type)
        return True
    except ValueError:
        return False


def validate_entity_class(entity_class: str) -> bool:
    """Validate that an entity class string is valid"""
    try:
        EntityClass(entity_class)
        return True
    except ValueError:
        return False


# Export commonly used functions
__all__ = [
    'NodeType',
    'EntityClass', 
    'RelationshipType',
    'RelationshipCategory',
    'RelationshipDefinition',
    'RelationshipRegistry',
    'validate_node_type',
    'validate_entity_class'
]