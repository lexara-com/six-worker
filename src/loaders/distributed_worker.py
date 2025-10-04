#!/usr/bin/env python3
"""
Distributed Worker Client
Connects to Cloudflare Coordinator and executes loader jobs

Uses plugin-based loader discovery - automatically finds and loads
the appropriate loader class based on job type.
"""
import os
import sys
import time
import json
import socket
import psycopg2
import requests
import threading
import logging
import importlib
import importlib.util
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributedWorker:
    """Distributed worker that claims and executes jobs from Cloudflare Coordinator"""

    def __init__(
        self,
        coordinator_url: str,
        worker_id: Optional[str] = None,
        capabilities: Optional[List[str]] = None,
        aws_region: str = 'us-east-1'
    ):
        self.coordinator_url = coordinator_url.rstrip('/')
        self.worker_id = worker_id or self._generate_worker_id()
        self.capabilities = capabilities or ['iowa_business', 'iowa_asbestos']
        self.aws_region = aws_region

        # Aurora connection (for direct writes)
        self.db_conn = None
        self._setup_database()

        # Heartbeat thread
        self.current_job_id = None
        self.heartbeat_thread = None
        self.should_stop = False

        logger.info(f"Worker initialized: {self.worker_id}")
        logger.info(f"Capabilities: {self.capabilities}")
        logger.info(f"Coordinator: {self.coordinator_url}")

    def _generate_worker_id(self) -> str:
        """Generate unique worker ID"""
        hostname = socket.gethostname()
        return f"worker-{hostname}-{int(time.time())}"

    def _setup_database(self):
        """Set up direct Aurora connection for writes"""
        try:
            # Get credentials from AWS Secrets Manager
            db_creds = self._get_aurora_credentials()

            # Connect to Aurora
            self.db_conn = psycopg2.connect(
                host=db_creds['host'],
                database=db_creds['database'],
                user=db_creds['user'],
                password=db_creds['password'],
                port=db_creds['port'],
                application_name=self.worker_id
            )

            logger.info("✅ Connected to Aurora PostgreSQL")

        except Exception as e:
            logger.error(f"❌ Failed to connect to Aurora: {e}")
            raise

    def _get_aurora_credentials(self) -> Dict[str, Any]:
        """Fetch Aurora credentials from AWS Secrets Manager"""
        try:
            # Try IAM role first (for EC2)
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=self.aws_region
            )

            secret_name = f"{os.environ.get('ENVIRONMENT', 'production')}/database-write"
            response = client.get_secret_value(SecretId=secret_name)

            return json.loads(response['SecretString'])

        except ClientError as e:
            # Fallback to environment variables (for Raspberry Pi)
            logger.warning(f"Secrets Manager failed: {e}. Using environment variables.")
            return {
                'host': os.environ.get('DB_HOST'),
                'database': os.environ.get('DB_NAME', 'graph_db'),
                'user': os.environ.get('DB_USER', 'graph_admin'),
                'password': os.environ.get('DB_PASSWORD'),
                'port': int(os.environ.get('DB_PORT', 5432))
            }

    def run(self):
        """Main worker loop"""
        logger.info("🚀 Worker started, polling for jobs...")

        try:
            while not self.should_stop:
                # Claim a job from coordinator
                job = self._claim_job()

                if not job:
                    # No jobs available, sleep and retry
                    logger.debug("No jobs available, sleeping...")
                    time.sleep(30)
                    continue

                logger.info(f"📋 Claimed job: {job['job_id']} ({job['job_type']})")

                # Execute job
                try:
                    self._execute_job(job)
                except Exception as e:
                    logger.error(f"❌ Job execution failed: {e}")
                    self._fail_job(job['job_id'], str(e))

        except KeyboardInterrupt:
            logger.info("🛑 Worker stopped by user")
        finally:
            self.cleanup()

    def _claim_job(self) -> Optional[Dict[str, Any]]:
        """Claim a job from Cloudflare Coordinator"""
        try:
            response = requests.post(
                f"{self.coordinator_url}/jobs/claim",
                json={
                    'worker_id': self.worker_id,
                    'capabilities': self.capabilities
                },
                timeout=10
            )

            if response.status_code == 204:
                # No jobs available
                return None

            if response.status_code == 200:
                job_data = response.json()

                # Execute claim instruction in Aurora
                self._execute_claim(job_data)

                return job_data

            logger.error(f"Unexpected response: {response.status_code}")
            return None

        except Exception as e:
            logger.error(f"Failed to claim job: {e}")
            return None

    def _execute_claim(self, job_data: Dict[str, Any]):
        """Execute the claim instruction to update Aurora"""
        claim_sql = job_data['claim_instruction']['sql']
        claim_params = job_data['claim_instruction']['params']

        with self.db_conn.cursor() as cur:
            cur.execute(claim_sql, claim_params)
            self.db_conn.commit()

        logger.info(f"✅ Job claimed in Aurora: {job_data['job_id']}")

    def _load_loader_class(self, job_type: str):
        """
        Dynamically load the appropriate loader class for the job type.

        Tries multiple strategies:
        1. Load from jobs/{job_type}/loader.py
        2. Load from src/loaders/{job_type}_loader.py (legacy)
        3. Raise error if not found
        """
        # Get project root
        project_root = Path(__file__).parent.parent.parent

        # Strategy 1: Check jobs folder (new plugin approach)
        job_dir = project_root / 'jobs' / job_type
        loader_file = job_dir / 'loader.py'

        if loader_file.exists():
            logger.info(f"📦 Loading plugin from: {loader_file}")
            spec = importlib.util.spec_from_file_location(f"job_{job_type}", loader_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Try to find a class that ends with 'Loader'
            for name in dir(module):
                obj = getattr(module, name)
                if isinstance(obj, type) and name.endswith('Loader') and name != 'Loader':
                    logger.info(f"✅ Loaded loader class: {name}")
                    return obj

            raise ImportError(f"No Loader class found in {loader_file}")

        # Strategy 2: Legacy loader in src/loaders (backward compatibility)
        legacy_module_name = f"{job_type}_loader"
        legacy_class_name = ''.join(word.capitalize() for word in job_type.split('_')) + 'Loader'

        try:
            logger.info(f"📦 Trying legacy loader: loaders.{legacy_module_name}")
            module = importlib.import_module(f"loaders.{legacy_module_name}")
            loader_class = getattr(module, legacy_class_name)
            logger.info(f"✅ Loaded legacy loader: {legacy_class_name}")
            return loader_class
        except (ImportError, AttributeError) as e:
            logger.error(f"❌ Failed to load legacy loader: {e}")

        # Neither strategy worked
        raise ImportError(
            f"Could not find loader for job type '{job_type}'.\n"
            f"Tried:\n"
            f"  1. {loader_file}\n"
            f"  2. src/loaders/{legacy_module_name}.py\n"
            f"Create a loader in one of these locations."
        )

    def _execute_job(self, job: Dict[str, Any]):
        """Execute the loader job"""
        job_id = job['job_id']
        job_type = job['job_type']
        config = job['config']

        # Mark job as running
        self._update_job_status(job_id, 'running')

        # Start heartbeat thread
        self.current_job_id = job_id
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

        # Dynamically load the appropriate loader class
        LoaderClass = self._load_loader_class(job_type)
        loader = LoaderClass(config)

        # Set up loader with distributed callbacks
        loader.connection = self.db_conn

        # Process file with callbacks
        loader.run(
            file_path=config.get('input', {}).get('file_path'),
            limit=config.get('processing', {}).get('limit'),
            batch_size=config.get('processing', {}).get('batch_size', 100),
            checkpoint_callback=lambda cp: self._save_checkpoint(job_id, cp),
            log_callback=lambda log: self._send_log(job_id, log),
            error_callback=lambda err: self._report_data_quality_issue(job_id, err)
        )

        # Mark job as completed
        self._complete_job(job_id)

        # Stop heartbeat
        self.current_job_id = None

    def _update_job_status(self, job_id: str, status: str, error_message: str = None):
        """Update job status in Aurora"""
        with self.db_conn.cursor() as cur:
            if status == 'running':
                cur.execute("""
                    UPDATE job_queue
                    SET status = %s, started_at = NOW(), updated_at = NOW()
                    WHERE job_id = %s
                """, (status, job_id))
            elif status == 'completed':
                cur.execute("""
                    UPDATE job_queue
                    SET status = %s, completed_at = NOW(), updated_at = NOW()
                    WHERE job_id = %s
                """, (status, job_id))
            elif status == 'failed':
                cur.execute("""
                    UPDATE job_queue
                    SET status = %s, error_message = %s, updated_at = NOW()
                    WHERE job_id = %s
                """, (status, error_message, job_id))

            self.db_conn.commit()

    def _heartbeat_loop(self):
        """Send periodic heartbeats to coordinator"""
        while self.current_job_id:
            try:
                # Update worker heartbeat in Aurora
                with self.db_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO workers (worker_id, hostname, last_heartbeat, status, capabilities)
                        VALUES (%s, %s, NOW(), 'active', %s)
                        ON CONFLICT (worker_id)
                        DO UPDATE SET last_heartbeat = NOW(), status = 'active', updated_at = NOW()
                    """, (self.worker_id, socket.gethostname(), json.dumps(self.capabilities)))
                    self.db_conn.commit()

                # Also notify coordinator (optional, for monitoring)
                try:
                    requests.post(
                        f"{self.coordinator_url}/jobs/{self.current_job_id}/heartbeat",
                        json={'worker_id': self.worker_id},
                        timeout=5
                    )
                except:
                    pass  # Not critical if this fails

            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")

            time.sleep(60)  # Heartbeat every 60 seconds

    def _save_checkpoint(self, job_id: str, checkpoint: Dict[str, Any]):
        """Save job checkpoint to Aurora"""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                UPDATE job_queue
                SET checkpoint = %s, updated_at = NOW()
                WHERE job_id = %s
            """, (json.dumps(checkpoint), job_id))
            self.db_conn.commit()

        logger.info(f"💾 Checkpoint saved: {checkpoint.get('records_processed', 0)} records")

    def _send_log(self, job_id: str, log_entry: Dict[str, Any]):
        """Send log entry to CloudWatch (via shared logger)"""
        # This will be handled by CloudWatch logger utility
        # For now, just log locally
        level = log_entry.get('level', 'INFO')
        message = log_entry.get('message', '')
        metadata = log_entry.get('metadata', {})

        log_method = getattr(logger, level.lower(), logger.info)
        log_method(f"{message} | {json.dumps(metadata)}")

    def _report_data_quality_issue(self, job_id: str, issue: Dict[str, Any]):
        """Report data quality issue to Aurora"""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_quality_issues (
                    issue_id, job_id, source_record_id, issue_type, severity,
                    field_name, invalid_value, expected_format, message, raw_record,
                    resolution_status, created_at
                ) VALUES (
                    gen_ulid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW()
                )
            """, (
                job_id,
                issue.get('source_record_id'),
                issue.get('issue_type'),
                issue.get('severity', 'warning'),
                issue.get('field_name'),
                issue.get('invalid_value'),
                issue.get('expected_format'),
                issue.get('message'),
                json.dumps(issue.get('raw_record', {}))
            ))
            self.db_conn.commit()

    def _complete_job(self, job_id: str):
        """Mark job as completed"""
        self._update_job_status(job_id, 'completed')
        logger.info(f"✅ Job completed: {job_id}")

    def _fail_job(self, job_id: str, error_message: str):
        """Mark job as failed"""
        self._update_job_status(job_id, 'failed', error_message)
        logger.error(f"❌ Job failed: {job_id} - {error_message}")

    def cleanup(self):
        """Cleanup resources"""
        if self.db_conn:
            self.db_conn.close()
        logger.info("Worker cleanup complete")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Distributed Loader Worker')
    parser.add_argument('--coordinator-url', required=True, help='Cloudflare Coordinator URL')
    parser.add_argument('--worker-id', help='Worker ID (auto-generated if not provided)')
    parser.add_argument('--capabilities', nargs='+', default=['iowa_business', 'iowa_asbestos'],
                       help='Job types this worker can handle')
    parser.add_argument('--aws-region', default='us-east-1', help='AWS region')

    args = parser.parse_args()

    # Create and run worker
    worker = DistributedWorker(
        coordinator_url=args.coordinator_url,
        worker_id=args.worker_id,
        capabilities=args.capabilities,
        aws_region=args.aws_region
    )

    worker.run()


if __name__ == '__main__':
    main()
