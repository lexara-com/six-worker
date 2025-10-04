#!/usr/bin/env python3
"""
CloudWatch Logger Utility
Batched logging to AWS CloudWatch Logs for distributed workers
"""
import os
import json
import time
import boto3
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import deque
from botocore.exceptions import ClientError


class CloudWatchLogger:
    """
    Batched CloudWatch logger with automatic flushing

    Batches log events and sends them to CloudWatch in groups to reduce API calls.
    Supports both IAM role (EC2) and access key authentication.
    """

    def __init__(
        self,
        log_group: str = '/lexara/distributed-loaders',
        log_stream: Optional[str] = None,
        aws_region: str = 'us-east-1',
        batch_size: int = 25,
        flush_interval: int = 5,
        worker_id: Optional[str] = None
    ):
        """
        Initialize CloudWatch logger

        Args:
            log_group: CloudWatch log group name
            log_stream: CloudWatch log stream name (auto-generated if not provided)
            aws_region: AWS region
            batch_size: Number of log events to batch before sending (max 10,000)
            flush_interval: Seconds between automatic flushes
            worker_id: Worker ID for log stream naming
        """
        self.log_group = log_group
        self.log_stream = log_stream or self._generate_log_stream(worker_id)
        self.aws_region = aws_region
        self.batch_size = min(batch_size, 10000)  # AWS limit
        self.flush_interval = flush_interval

        # Initialize boto3 client
        self.client = self._get_cloudwatch_client()

        # Create log stream
        self._create_log_stream()

        # Batch buffer
        self.buffer = deque()
        self.buffer_lock = threading.Lock()
        self.sequence_token = None

        # Auto-flush thread
        self.should_stop = False
        self.flush_thread = threading.Thread(target=self._auto_flush_loop, daemon=True)
        self.flush_thread.start()

    def _generate_log_stream(self, worker_id: Optional[str]) -> str:
        """Generate unique log stream name"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d')
        worker_suffix = f"-{worker_id}" if worker_id else ""
        return f"worker-{timestamp}{worker_suffix}"

    def _get_cloudwatch_client(self):
        """Get CloudWatch Logs client with appropriate credentials"""
        try:
            # Try IAM role first (EC2)
            session = boto3.session.Session()
            return session.client(
                service_name='logs',
                region_name=self.aws_region
            )
        except Exception:
            # Fallback to access keys (environment variables or credentials file)
            return boto3.client(
                service_name='logs',
                region_name=self.aws_region,
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
            )

    def _create_log_stream(self):
        """Create log stream if it doesn't exist"""
        try:
            self.client.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=self.log_stream
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            # Log stream already exists, that's fine
            pass
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Log group doesn't exist
                raise RuntimeError(
                    f"Log group '{self.log_group}' does not exist. "
                    f"Please create it via Terraform or AWS Console."
                )
            raise

    def log(
        self,
        level: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        worker_id: Optional[str] = None
    ):
        """
        Log a message (will be batched)

        Args:
            level: Log level (INFO, WARNING, ERROR, DEBUG)
            message: Log message
            metadata: Additional structured data
            job_id: Job ID (if applicable)
            worker_id: Worker ID
        """
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level.upper(),
            'message': message,
            'log_stream': self.log_stream
        }

        if metadata:
            log_entry['metadata'] = metadata

        if job_id:
            log_entry['job_id'] = job_id

        if worker_id:
            log_entry['worker_id'] = worker_id

        # Add to buffer
        with self.buffer_lock:
            self.buffer.append({
                'timestamp': int(time.time() * 1000),  # Milliseconds since epoch
                'message': json.dumps(log_entry)
            })

            # Flush if batch size reached
            if len(self.buffer) >= self.batch_size:
                self._flush()

    def info(self, message: str, **kwargs):
        """Log INFO level message"""
        self.log('INFO', message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log WARNING level message"""
        self.log('WARNING', message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log ERROR level message"""
        self.log('ERROR', message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log DEBUG level message"""
        self.log('DEBUG', message, **kwargs)

    def _auto_flush_loop(self):
        """Automatically flush buffer every N seconds"""
        while not self.should_stop:
            time.sleep(self.flush_interval)
            self._flush()

    def _flush(self):
        """Send buffered logs to CloudWatch"""
        with self.buffer_lock:
            if not self.buffer:
                return

            # Take up to batch_size events
            events = []
            while self.buffer and len(events) < self.batch_size:
                events.append(self.buffer.popleft())

            if not events:
                return

            # Send to CloudWatch
            try:
                kwargs = {
                    'logGroupName': self.log_group,
                    'logStreamName': self.log_stream,
                    'logEvents': events
                }

                if self.sequence_token:
                    kwargs['sequenceToken'] = self.sequence_token

                response = self.client.put_log_events(**kwargs)
                self.sequence_token = response.get('nextSequenceToken')

            except self.client.exceptions.InvalidSequenceTokenException as e:
                # Sequence token out of sync, extract correct token and retry
                self.sequence_token = e.response['Error']['Message'].split('is: ')[-1]

                kwargs['sequenceToken'] = self.sequence_token
                response = self.client.put_log_events(**kwargs)
                self.sequence_token = response.get('nextSequenceToken')

            except ClientError as e:
                # Failed to send logs, put them back in buffer
                print(f"Failed to send logs to CloudWatch: {e}")
                with self.buffer_lock:
                    self.buffer.extendleft(reversed(events))

    def flush(self):
        """Force flush of all buffered logs"""
        self._flush()

    def close(self):
        """Close logger and flush remaining logs"""
        self.should_stop = True
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=10)
        self._flush()


# Singleton instance for convenient access
_default_logger: Optional[CloudWatchLogger] = None


def get_logger(
    log_group: str = '/lexara/distributed-loaders',
    worker_id: Optional[str] = None,
    **kwargs
) -> CloudWatchLogger:
    """
    Get or create default CloudWatch logger instance

    Args:
        log_group: CloudWatch log group name
        worker_id: Worker ID for log stream
        **kwargs: Additional CloudWatchLogger constructor arguments

    Returns:
        CloudWatchLogger instance
    """
    global _default_logger

    if _default_logger is None:
        _default_logger = CloudWatchLogger(
            log_group=log_group,
            worker_id=worker_id,
            **kwargs
        )

    return _default_logger


def close_logger():
    """Close default logger"""
    global _default_logger

    if _default_logger:
        _default_logger.close()
        _default_logger = None
