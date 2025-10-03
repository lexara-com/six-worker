#!/usr/bin/env python3
"""
Dead Letter Queue Handler for Failed Records

This module provides functionality to manage and reprocess failed records
from data loaders. It implements retry logic with exponential backoff and
maintains a comprehensive audit trail.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import psycopg2
from psycopg2.extras import DictCursor


@dataclass
class FailedRecord:
    """Represents a failed record in the DLQ"""
    record_id: str
    source_id: str
    source_type: str
    record_data: Dict[str, Any]
    error_message: str
    error_type: str
    attempt_count: int
    created_at: datetime
    last_attempt_at: Optional[datetime] = None
    reprocessed: bool = False
    reprocessed_at: Optional[datetime] = None


class DeadLetterQueue:
    """Manages failed records for data loaders"""
    
    def __init__(self, connection_params: Dict[str, Any], source_id: Optional[str] = None):
        self.conn_params = connection_params
        self.source_id = source_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self._ensure_dlq_table()
    
    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.conn_params)
    
    def _ensure_dlq_table(self):
        """Ensure the DLQ table exists"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS failed_records (
                        record_id VARCHAR(26) PRIMARY KEY DEFAULT generate_ulid(),
                        source_id VARCHAR(26) NOT NULL,
                        source_type VARCHAR(100) NOT NULL,
                        record_data JSONB NOT NULL,
                        error_message TEXT,
                        error_type VARCHAR(100),
                        error_details JSONB,
                        attempt_count INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_attempt_at TIMESTAMP,
                        reprocessed BOOLEAN DEFAULT FALSE,
                        reprocessed_at TIMESTAMP,
                        reprocess_result JSONB,
                        
                        -- Indexes for efficient querying
                        CONSTRAINT fk_failed_records_source 
                            FOREIGN KEY (source_id) REFERENCES sources(source_id)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_failed_records_source 
                        ON failed_records(source_id);
                    CREATE INDEX IF NOT EXISTS idx_failed_records_reprocessed 
                        ON failed_records(reprocessed);
                    CREATE INDEX IF NOT EXISTS idx_failed_records_created 
                        ON failed_records(created_at);
                    CREATE INDEX IF NOT EXISTS idx_failed_records_type 
                        ON failed_records(error_type);
                """)
                conn.commit()
                
                self.logger.info("DLQ table verified/created")
    
    def add_failed_record(self, record: Dict[str, Any], error: Exception, 
                         source_type: str, error_details: Optional[Dict] = None) -> str:
        """Add a failed record to the DLQ"""
        
        if not self.source_id:
            raise ValueError("Source ID must be set to add failed records")
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO failed_records (
                        source_id, source_type, record_data,
                        error_message, error_type, error_details,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    RETURNING record_id
                """, (
                    self.source_id,
                    source_type,
                    json.dumps(record),
                    str(error)[:5000],  # Truncate very long error messages
                    type(error).__name__,
                    json.dumps(error_details) if error_details else None
                ))
                
                record_id = cursor.fetchone()[0]
                conn.commit()
                
                self.logger.debug(f"Added failed record to DLQ: {record_id}")
                return record_id
    
    def get_failed_records(self, limit: int = 100, 
                          max_attempts: int = 3,
                          older_than_minutes: int = 60) -> List[FailedRecord]:
        """Retrieve failed records ready for reprocessing"""
        
        cutoff_time = datetime.now() - timedelta(minutes=older_than_minutes)
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                query = """
                    SELECT record_id, source_id, source_type, record_data,
                           error_message, error_type, attempt_count,
                           created_at, last_attempt_at, reprocessed, reprocessed_at
                    FROM failed_records
                    WHERE reprocessed = FALSE
                      AND attempt_count < %s
                      AND (last_attempt_at IS NULL OR last_attempt_at < %s)
                """
                
                params = [max_attempts, cutoff_time]
                
                if self.source_id:
                    query += " AND source_id = %s"
                    params.append(self.source_id)
                
                query += " ORDER BY created_at LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                
                records = []
                for row in cursor:
                    records.append(FailedRecord(
                        record_id=row['record_id'],
                        source_id=row['source_id'],
                        source_type=row['source_type'],
                        record_data=row['record_data'],
                        error_message=row['error_message'],
                        error_type=row['error_type'],
                        attempt_count=row['attempt_count'],
                        created_at=row['created_at'],
                        last_attempt_at=row['last_attempt_at'],
                        reprocessed=row['reprocessed'],
                        reprocessed_at=row['reprocessed_at']
                    ))
                
                return records
    
    def mark_reprocessing(self, record_id: str):
        """Mark a record as being reprocessed"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE failed_records
                    SET last_attempt_at = NOW(),
                        attempt_count = attempt_count + 1
                    WHERE record_id = %s
                """, (record_id,))
                conn.commit()
    
    def mark_reprocessed(self, record_id: str, success: bool, result: Optional[Dict] = None):
        """Mark a record as successfully reprocessed"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                if success:
                    cursor.execute("""
                        UPDATE failed_records
                        SET reprocessed = TRUE,
                            reprocessed_at = NOW(),
                            reprocess_result = %s
                        WHERE record_id = %s
                    """, (json.dumps(result) if result else None, record_id))
                else:
                    # Just update the attempt count and last attempt
                    cursor.execute("""
                        UPDATE failed_records
                        SET reprocess_result = %s
                        WHERE record_id = %s
                    """, (json.dumps(result) if result else None, record_id))
                
                conn.commit()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get DLQ statistics"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                
                # Overall statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(*) FILTER (WHERE reprocessed = TRUE) as reprocessed,
                        COUNT(*) FILTER (WHERE reprocessed = FALSE) as pending,
                        COUNT(*) FILTER (WHERE attempt_count >= 3) as max_attempts_reached,
                        MIN(created_at) as oldest_record,
                        MAX(created_at) as newest_record
                    FROM failed_records
                    WHERE source_id = %s OR %s IS NULL
                """, (self.source_id, self.source_id))
                
                overall = dict(cursor.fetchone())
                
                # Error type breakdown
                cursor.execute("""
                    SELECT error_type, COUNT(*) as count
                    FROM failed_records
                    WHERE (source_id = %s OR %s IS NULL)
                      AND reprocessed = FALSE
                    GROUP BY error_type
                    ORDER BY count DESC
                """, (self.source_id, self.source_id))
                
                error_breakdown = {row['error_type']: row['count'] for row in cursor}
                
                # Source breakdown
                cursor.execute("""
                    SELECT source_type, COUNT(*) as count
                    FROM failed_records
                    WHERE reprocessed = FALSE
                    GROUP BY source_type
                    ORDER BY count DESC
                """)
                
                source_breakdown = {row['source_type']: row['count'] for row in cursor}
                
                return {
                    'overall': overall,
                    'error_breakdown': error_breakdown,
                    'source_breakdown': source_breakdown
                }
    
    def cleanup_old_records(self, days: int = 30) -> int:
        """Remove successfully reprocessed records older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM failed_records
                    WHERE reprocessed = TRUE
                      AND reprocessed_at < %s
                    RETURNING record_id
                """, (cutoff_date,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Cleaned up {deleted_count} old reprocessed records")
                return deleted_count


class DLQReprocessor:
    """Handles reprocessing of failed records with retry logic"""
    
    def __init__(self, dlq: DeadLetterQueue, loader):
        self.dlq = dlq
        self.loader = loader
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_retries = 3
        self.base_delay = 60  # seconds
    
    def calculate_backoff(self, attempt: int) -> int:
        """Calculate exponential backoff delay"""
        return self.base_delay * (2 ** (attempt - 1))
    
    def reprocess_record(self, failed_record: FailedRecord) -> bool:
        """Reprocess a single failed record"""
        
        self.logger.info(f"Reprocessing record {failed_record.record_id} "
                        f"(attempt {failed_record.attempt_count + 1})")
        
        # Mark as being reprocessed
        self.dlq.mark_reprocessing(failed_record.record_id)
        
        try:
            # Parse the record using the loader's parse method
            parsed = self.loader.parse_record(failed_record.record_data)
            if not parsed:
                raise ValueError("Record could not be parsed")
            
            # Validate the record
            errors = self.loader.validate_record(parsed)
            if errors:
                raise ValueError(f"Validation errors: {errors}")
            
            # Process the record
            results = self.loader.process_record(parsed)
            
            # Check if all results were successful
            if all(r.success for r in results):
                self.dlq.mark_reprocessed(
                    failed_record.record_id, 
                    success=True,
                    result={'status': 'success', 'results': len(results)}
                )
                self.logger.info(f"Successfully reprocessed record {failed_record.record_id}")
                return True
            else:
                # Some results failed
                failed_results = [r for r in results if not r.success]
                self.dlq.mark_reprocessed(
                    failed_record.record_id,
                    success=False,
                    result={'status': 'partial_failure', 'failed': len(failed_results)}
                )
                self.logger.warning(f"Partial failure reprocessing {failed_record.record_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to reprocess {failed_record.record_id}: {e}")
            
            # Update with new error information
            self.dlq.mark_reprocessed(
                failed_record.record_id,
                success=False,
                result={'status': 'error', 'error': str(e)}
            )
            
            # Apply backoff if we haven't exceeded max attempts
            if failed_record.attempt_count < self.max_retries:
                delay = self.calculate_backoff(failed_record.attempt_count)
                self.logger.info(f"Will retry in {delay} seconds")
                time.sleep(delay)
            
            return False
    
    def reprocess_batch(self, limit: int = 100) -> Dict[str, Any]:
        """Reprocess a batch of failed records"""
        
        self.logger.info(f"Starting DLQ batch reprocessing (limit: {limit})")
        
        # Get failed records ready for reprocessing
        records = self.dlq.get_failed_records(
            limit=limit,
            max_attempts=self.max_retries,
            older_than_minutes=5  # Don't retry too quickly
        )
        
        if not records:
            self.logger.info("No records ready for reprocessing")
            return {'processed': 0, 'successful': 0, 'failed': 0}
        
        self.logger.info(f"Found {len(records)} records to reprocess")
        
        stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        for record in records:
            success = self.reprocess_record(record)
            stats['processed'] += 1
            
            if success:
                stats['successful'] += 1
            else:
                stats['failed'] += 1
                stats['errors'].append({
                    'record_id': record.record_id,
                    'error_type': record.error_type,
                    'attempts': record.attempt_count + 1
                })
        
        self.logger.info(f"DLQ reprocessing complete: "
                        f"{stats['successful']}/{stats['processed']} successful")
        
        return stats


def main():
    """Main entry point for DLQ management"""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='Dead Letter Queue Manager')
    parser.add_argument('--action', choices=['stats', 'reprocess', 'cleanup'], 
                       default='stats', help='Action to perform')
    parser.add_argument('--source-id', help='Filter by source ID')
    parser.add_argument('--limit', type=int, default=100, 
                       help='Limit for reprocessing')
    parser.add_argument('--cleanup-days', type=int, default=30,
                       help='Days to keep reprocessed records')
    
    args = parser.parse_args()
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'graph_db'),
        'user': os.getenv('DB_USER', 'graph_admin'),
        'password': os.getenv('DB_PASS', 'your_password'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    # Create DLQ instance
    dlq = DeadLetterQueue(conn_params, source_id=args.source_id)
    
    if args.action == 'stats':
        # Show statistics
        stats = dlq.get_statistics()
        
        print("\n=== Dead Letter Queue Statistics ===")
        print(f"Total Records: {stats['overall']['total_records']:,}")
        print(f"Reprocessed: {stats['overall']['reprocessed']:,}")
        print(f"Pending: {stats['overall']['pending']:,}")
        print(f"Max Attempts Reached: {stats['overall']['max_attempts_reached']:,}")
        
        if stats['overall']['oldest_record']:
            print(f"Oldest Record: {stats['overall']['oldest_record']}")
            print(f"Newest Record: {stats['overall']['newest_record']}")
        
        if stats['error_breakdown']:
            print("\n=== Error Type Breakdown ===")
            for error_type, count in stats['error_breakdown'].items():
                print(f"  {error_type}: {count:,}")
        
        if stats['source_breakdown']:
            print("\n=== Source Type Breakdown ===")
            for source_type, count in stats['source_breakdown'].items():
                print(f"  {source_type}: {count:,}")
    
    elif args.action == 'reprocess':
        # Reprocess failed records
        print(f"\nReprocessing up to {args.limit} failed records...")
        
        # Note: This would need the appropriate loader instance
        # For demonstration, we'll just show what would be done
        records = dlq.get_failed_records(limit=args.limit)
        
        print(f"Found {len(records)} records ready for reprocessing")
        for record in records[:5]:  # Show first 5
            print(f"  - {record.record_id}: {record.error_type} "
                  f"(attempts: {record.attempt_count})")
        
        if len(records) > 5:
            print(f"  ... and {len(records) - 5} more")
    
    elif args.action == 'cleanup':
        # Cleanup old records
        print(f"\nCleaning up reprocessed records older than {args.cleanup_days} days...")
        deleted = dlq.cleanup_old_records(days=args.cleanup_days)
        print(f"Deleted {deleted:,} old records")


if __name__ == "__main__":
    main()