"""
Propose API Client - Python Integration Example

This module provides a Python interface for the Propose API intelligent fact ingestion system.
It handles database connections, request formatting, response parsing, and error handling.

Usage:
    client = ProposeAPIClient(connection_params)
    
    result = client.propose_fact(
        source_entity=('Person', 'John Smith'),
        target_entity=('Company', 'ACME Corp'),
        relationship='Employment',
        source_info=('HR System', 'hr_database')
    )
    
    if result.success:
        print(f"Fact ingested with confidence: {result.overall_confidence}")
    else:
        print(f"Error: {result.error_message}")
"""

import json
import logging
import sys
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union, Any
import psycopg2
from psycopg2.extras import RealDictCursor

# Add the src directory to the path for type imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from graph_types import NodeType, RelationshipType, EntityClass, RelationshipRegistry


@dataclass
class ProposeResponse:
    """Response from the Propose API"""
    success: bool
    status: str  # 'success', 'conflicts', 'error'
    overall_confidence: float
    actions: List[Dict[str, Any]] = field(default_factory=list)
    conflicts: List[Dict[str, Any]] = field(default_factory=list)
    provenance_ids: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    raw_response: Optional[Dict] = None


class ProposeAPIClient:
    """Client for interacting with the Propose API intelligent fact ingestion system"""
    
    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize the Propose API client
        
        Args:
            connection_params: Database connection parameters
                {
                    'host': 'localhost',
                    'database': 'graph_db', 
                    'user': 'graph_admin',
                    'password': 'your_password',
                    'port': 5432
                }
        """
        self.connection_params = connection_params
        self.logger = logging.getLogger(__name__)
        
    def _get_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(
                cursor_factory=RealDictCursor,
                **self.connection_params
            )
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def _format_attributes(self, attributes: Optional[Dict[str, str]]) -> str:
        """Format attributes dictionary to JSONB string"""
        if not attributes:
            return '[]'
        
        # Convert to propose API format
        formatted = []
        for attr_type, value in attributes.items():
            formatted.append({
                "type": attr_type,
                "value": value
            })
        
        return json.dumps(formatted)
    
    def _parse_response(self, raw_result: Dict) -> ProposeResponse:
        """Parse database response into ProposeResponse object"""
        try:
            status = raw_result.get('status', 'error')
            success = status in ['success', 'conflicts']

            # Parse JSONB fields
            actions = raw_result.get('actions', [])
            if isinstance(actions, str):
                actions = json.loads(actions)

            conflicts = raw_result.get('conflicts', [])
            if isinstance(conflicts, str):
                conflicts = json.loads(conflicts)

            provenance_ids = raw_result.get('provenance_ids', [])
            if isinstance(provenance_ids, str):
                provenance_ids = json.loads(provenance_ids)

            # Extract error message from actions if status is error
            error_message = None
            if status == 'error' and actions:
                if isinstance(actions, list) and len(actions) > 0:
                    error_message = actions[0].get('error') or actions[0].get('message')
                elif isinstance(actions, dict):
                    error_message = actions.get('error') or actions.get('message')

            return ProposeResponse(
                success=success,
                status=status,
                overall_confidence=float(raw_result.get('overall_confidence', 0.0)),
                actions=actions,
                conflicts=conflicts,
                provenance_ids=provenance_ids,
                error_message=error_message,
                raw_response=raw_result
            )
            
        except Exception as e:
            self.logger.error(f"Failed to parse response: {e}")
            return ProposeResponse(
                success=False,
                status='error',
                overall_confidence=0.0,
                error_message=f"Response parsing error: {str(e)}",
                raw_response=raw_result
            )
    
    def propose_fact(
        self,
        source_entity: Tuple[Union[str, NodeType], str],  # (type, name)
        target_entity: Tuple[Union[str, NodeType], str],  # (type, name)
        relationship: Union[str, RelationshipType],
        source_info: Tuple[str, str],   # (source_name, source_type)
        source_attributes: Optional[Dict[str, str]] = None,
        target_attributes: Optional[Dict[str, str]] = None,
        relationship_strength: float = 1.0,
        relationship_valid_from: Optional[date] = None,
        relationship_valid_to: Optional[date] = None,
        relationship_metadata: Optional[Dict] = None,
        provenance_confidence: float = 0.9,
        provenance_metadata: Optional[Dict] = None
    ) -> ProposeResponse:
        """
        Propose a fact to the intelligent ingestion system
        
        Args:
            source_entity: Tuple of (entity_type, entity_name)
            target_entity: Tuple of (entity_type, entity_name) 
            relationship: Relationship type between entities
            source_info: Tuple of (source_name, source_type)
            source_attributes: Optional attributes for source entity
            target_attributes: Optional attributes for target entity
            relationship_strength: Confidence in relationship (0.0-1.0)
            relationship_valid_from: Start date for relationship validity
            relationship_valid_to: End date for relationship validity
            relationship_metadata: Additional relationship metadata
            provenance_confidence: Confidence in source data (0.0-1.0)
            provenance_metadata: Additional provenance metadata
            
        Returns:
            ProposeResponse: Result of the fact ingestion
            
        Example:
            result = client.propose_fact(
                source_entity=('Person', 'John Smith'),
                target_entity=('Company', 'ACME Corporation'),
                relationship='Employment',
                source_info=('LinkedIn Profile', 'linkedin'),
                source_attributes={'title': 'Software Engineer', 'nameAlias': 'J. Smith'},
                relationship_strength=0.9
            )
        """
        
        # Validate and normalize input parameters
        try:
            # Convert enums to string values if needed
            source_type = source_entity[0].value if isinstance(source_entity[0], NodeType) else source_entity[0]
            target_type = target_entity[0].value if isinstance(target_entity[0], NodeType) else target_entity[0]
            relationship_str = relationship.value if isinstance(relationship, RelationshipType) else relationship
            
            # Validate node types
            if not RelationshipRegistry.validate_relationship_type(relationship_str):
                return ProposeResponse(
                    success=False,
                    status='error',
                    overall_confidence=0.0,
                    error_message=f"Invalid relationship type: '{relationship_str}'. Valid types: {[rt.value for rt in RelationshipType]}"
                )
            
            # Validate node types
            from graph_types import validate_node_type
            if not validate_node_type(source_type):
                return ProposeResponse(
                    success=False,
                    status='error', 
                    overall_confidence=0.0,
                    error_message=f"Invalid source node type: '{source_type}'. Valid types: {[nt.value for nt in NodeType]}"
                )
                
            if not validate_node_type(target_type):
                return ProposeResponse(
                    success=False,
                    status='error',
                    overall_confidence=0.0,
                    error_message=f"Invalid target node type: '{target_type}'. Valid types: {[nt.value for nt in NodeType]}"
                )
            
        except Exception as e:
            return ProposeResponse(
                success=False,
                status='error',
                overall_confidence=0.0,
                error_message=f"Validation error: {str(e)}"
            )
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Prepare SQL call
                    sql = """
                    SELECT * FROM propose_fact(
                        %s, %s,  -- source entity
                        %s, %s,  -- target entity  
                        %s,      -- relationship
                        %s, %s,  -- source info
                        %s::JSONB, %s::JSONB,  -- attributes
                        %s,      -- relationship strength
                        %s, %s,  -- relationship validity
                        %s::JSONB,  -- relationship metadata
                        %s,      -- provenance confidence
                        %s::JSONB   -- provenance metadata
                    )
                    """
                    
                    # Prepare parameters
                    params = [
                        source_type, source_entity[1],       # source (normalized)
                        target_type, target_entity[1],       # target (normalized)
                        relationship_str,                     # relationship (normalized)
                        source_info[0], source_info[1],      # source info
                        self._format_attributes(source_attributes),  # source attrs
                        self._format_attributes(target_attributes),  # target attrs
                        relationship_strength,                # strength
                        relationship_valid_from,              # valid from
                        relationship_valid_to,                # valid to
                        json.dumps(relationship_metadata) if relationship_metadata else None,
                        provenance_confidence,                # prov confidence
                        json.dumps(provenance_metadata) if provenance_metadata else None
                    ]
                    
                    # Execute query
                    cursor.execute(sql, params)
                    result = cursor.fetchone()
                    
                    if not result:
                        return ProposeResponse(
                            success=False,
                            status='error',
                            overall_confidence=0.0,
                            error_message="No response from database"
                        )
                    
                    return self._parse_response(dict(result))
                    
        except psycopg2.Error as e:
            self.logger.error(f"Database error in propose_fact: {e}")
            return ProposeResponse(
                success=False,
                status='error', 
                overall_confidence=0.0,
                error_message=f"Database error: {str(e)}"
            )
        except Exception as e:
            self.logger.error(f"Unexpected error in propose_fact: {e}")
            return ProposeResponse(
                success=False,
                status='error',
                overall_confidence=0.0,
                error_message=f"Unexpected error: {str(e)}"
            )
    
    def batch_propose_facts(
        self, 
        facts: List[Dict[str, Any]]
    ) -> List[ProposeResponse]:
        """
        Process multiple facts in sequence
        
        Args:
            facts: List of fact dictionaries with same parameters as propose_fact
            
        Returns:
            List of ProposeResponse objects
            
        Example:
            facts = [
                {
                    'source_entity': ('Person', 'John Smith'),
                    'target_entity': ('Company', 'ACME Corp'),
                    'relationship': 'Employment',
                    'source_info': ('HR System', 'hr_database')
                },
                {
                    'source_entity': ('Person', 'Jane Doe'), 
                    'target_entity': ('Company', 'ACME Corp'),
                    'relationship': 'Employment',
                    'source_info': ('HR System', 'hr_database')
                }
            ]
            
            results = client.batch_propose_facts(facts)
        """
        results = []
        
        for i, fact in enumerate(facts):
            try:
                result = self.propose_fact(**fact)
                results.append(result)
                
                # Log progress for large batches
                if (i + 1) % 100 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(facts)} facts")
                    
            except Exception as e:
                self.logger.error(f"Error processing fact {i}: {e}")
                results.append(ProposeResponse(
                    success=False,
                    status='error',
                    overall_confidence=0.0,
                    error_message=f"Processing error: {str(e)}"
                ))
        
        return results
    
    def get_entity_provenance(self, entity_id: str) -> List[Dict]:
        """
        Get provenance records for a specific entity
        
        Args:
            entity_id: ULID of the entity
            
        Returns:
            List of provenance records
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    sql = """
                    SELECT p.*, st.description as source_description
                    FROM provenance p
                    LEFT JOIN source_types st ON p.source_type = st.source_type
                    WHERE p.asset_id = %s AND p.asset_type = 'node'
                    ORDER BY p.created_at DESC
                    """
                    cursor.execute(sql, (entity_id,))
                    return [dict(row) for row in cursor.fetchall()]
                    
        except Exception as e:
            self.logger.error(f"Error getting provenance: {e}")
            return []
    
    def get_relationship_conflicts(self, entity1_id: str, entity2_id: str) -> List[Dict]:
        """
        Check for relationship conflicts between two entities
        
        Args:
            entity1_id: ULID of first entity
            entity2_id: ULID of second entity
            
        Returns:
            List of conflicting relationships
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    sql = """
                    SELECT r1.relationship_type as rel1_type, r2.relationship_type as rel2_type,
                           r1.strength as rel1_strength, r2.strength as rel2_strength,
                           r1.created_at as rel1_created, r2.created_at as rel2_created
                    FROM relationships r1
                    JOIN relationships r2 ON (
                        (r1.source_node_id = r2.source_node_id AND r1.target_node_id = r2.target_node_id) OR
                        (r1.source_node_id = r2.target_node_id AND r1.target_node_id = r2.source_node_id)
                    )
                    WHERE r1.relationship_id != r2.relationship_id
                      AND ((r1.source_node_id = %s AND r1.target_node_id = %s) OR
                           (r1.source_node_id = %s AND r1.target_node_id = %s))
                      AND r1.status = 'active' AND r2.status = 'active'
                      AND (
                          (r1.relationship_type = 'Legal_Counsel' AND r2.relationship_type = 'Opposing_Counsel') OR
                          (r1.relationship_type = 'Opposing_Counsel' AND r2.relationship_type = 'Legal_Counsel')
                      )
                    """
                    cursor.execute(sql, (entity1_id, entity2_id, entity2_id, entity1_id))
                    return [dict(row) for row in cursor.fetchall()]
                    
        except Exception as e:
            self.logger.error(f"Error checking conflicts: {e}")
            return []


# Example usage and test cases
if __name__ == "__main__":
    import os
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'graph_db'),
        'user': os.getenv('DB_USER', 'graph_admin'),
        'password': os.getenv('DB_PASS', 'your_password_here'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    # Initialize client
    client = ProposeAPIClient(conn_params)
    
    print("=== Propose API Python Client Demo ===\n")
    
    # Test Case 1: Basic fact ingestion
    print("1. Basic Fact Ingestion:")
    result = client.propose_fact(
        source_entity=('Person', 'Alice Johnson'),
        target_entity=('Company', 'TechStart LLC'),
        relationship='Employment',
        source_info=('Employee Directory', 'hr_system'),
        source_attributes={'title': 'Senior Developer', 'nameAlias': 'Alice J.'},
        relationship_strength=0.95
    )
    
    print(f"   Status: {result.status}")
    print(f"   Confidence: {result.overall_confidence:.2f}")
    print(f"   Actions: {len(result.actions)}")
    print(f"   Provenance IDs: {len(result.provenance_ids)}")
    print()
    
    # Test Case 2: Potential conflict scenario
    print("2. Conflict Detection Test:")
    result = client.propose_fact(
        source_entity=('Person', 'Alice Johnson'),  # Same person
        target_entity=('Company', 'TechStart LLC'), # Same company
        relationship='Opposing_Counsel',              # Conflicting relationship
        source_info=('Court Filing', 'legal_records'),
        relationship_strength=0.9
    )
    
    print(f"   Status: {result.status}")
    print(f"   Confidence: {result.overall_confidence:.2f}")
    print(f"   Conflicts detected: {len(result.conflicts)}")
    if result.conflicts:
        print(f"   Conflict type: {result.conflicts[0].get('type', 'Unknown')}")
    print()
    
    # Test Case 3: Batch processing
    print("3. Batch Processing Test:")
    facts = [
        {
            'source_entity': ('Person', 'Bob Wilson'),
            'target_entity': ('Company', 'Legal Partners Inc'),
            'relationship': 'Employment',
            'source_info': ('Business Card', 'business_cards'),
            'source_attributes': {'title': 'Partner'}
        },
        {
            'source_entity': ('Person', 'Carol Davis'),
            'target_entity': ('Company', 'Legal Partners Inc'), 
            'relationship': 'Employment',
            'source_info': ('LinkedIn Import', 'linkedin'),
            'source_attributes': {'title': 'Associate', 'nameAlias': 'C. Davis'}
        }
    ]
    
    batch_results = client.batch_propose_facts(facts)
    successful = sum(1 for r in batch_results if r.success)
    print(f"   Processed: {len(batch_results)} facts")
    print(f"   Successful: {successful}")
    print(f"   Average confidence: {sum(r.overall_confidence for r in batch_results) / len(batch_results):.2f}")
    print()
    
    print("=== Demo Complete ===")