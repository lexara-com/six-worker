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
        aws_region: str = None
    ):
        self.coordinator_url = coordinator_url.rstrip('/')
        self.worker_id = worker_id or self._generate_worker_id()
        self.capabilities = capabilities or ['iowa_business', 'iowa_asbestos']
        # Get AWS region from environment or parameter, default to us-east-1
        self.aws_region = aws_region or os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'us-east-1'

        # Aurora connection (for direct writes)
        self.db_conn = None
        self._setup_database()

        # Heartbeat thread
        self.current_job_id = None
        self.heartbeat_thread = None
        self.should_stop = False

        # AWS credentials (will be set during credential fetch)
        self.aws_credentials = None

        logger.info(f"Worker initialized: {self.worker_id}")
        logger.info(f"AWS Region: {self.aws_region}")
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

            # Set environment variables so loaders can access them
            os.environ['DB_HOST'] = db_creds['host']
            os.environ['DB_NAME'] = db_creds['database']
            os.environ['DB_USER'] = db_creds['user']
            os.environ['DB_PASSWORD'] = db_creds['password']
            os.environ['DB_PORT'] = str(db_creds['port'])

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
        """
        Fetch Aurora credentials from AWS Secrets Manager.

        Supports multiple authentication methods:
        1. AWS profile (AWS_PROFILE env var) - for Raspberry Pi workers
        2. IAM role - for EC2 instances
        3. Environment variables - fallback for local development
        """
        try:
            # Determine environment (dev, test, production)
            environment = os.environ.get('ENVIRONMENT', 'production')

            # Check if AWS profile is specified (for Raspberry Pi)
            aws_profile = os.environ.get('AWS_PROFILE')
            aws_role_arn = os.environ.get('AWS_ROLE_ARN', 'arn:aws:iam::561107861343:role/six-worker')

            if aws_profile:
                logger.info(f"Using AWS profile: {aws_profile}")
                session = boto3.session.Session(profile_name=aws_profile)
            else:
                logger.info("Using default AWS credentials (IAM role or default profile)")
                session = boto3.session.Session()

            # Assume role if configured (for Raspberry Pi workers with IAM user)
            if aws_role_arn and aws_profile:
                logger.info(f"Assuming role: {aws_role_arn}")
                sts_client = session.client('sts', region_name=self.aws_region)
                assumed_role = sts_client.assume_role(
                    RoleArn=aws_role_arn,
                    RoleSessionName=f"six-worker-{self.worker_id}"
                )

                # Create new session with temporary credentials
                session = boto3.session.Session(
                    aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                    aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                    aws_session_token=assumed_role['Credentials']['SessionToken']
                )

                # Store credentials for later use (S3 downloads, etc.)
                self.aws_credentials = {
                    'aws_access_key_id': assumed_role['Credentials']['AccessKeyId'],
                    'aws_secret_access_key': assumed_role['Credentials']['SecretAccessKey'],
                    'aws_session_token': assumed_role['Credentials']['SessionToken']
                }
                logger.info("✅ Successfully assumed role and stored credentials")

            client = session.client(
                service_name='secretsmanager',
                region_name=self.aws_region
            )

            # Secret naming convention: {environment}/six-worker/database
            secret_name = f"{environment}/six-worker/database"

            logger.info(f"Fetching credentials from Secrets Manager: {secret_name} (region: {self.aws_region})")
            response = client.get_secret_value(SecretId=secret_name)

            credentials = json.loads(response['SecretString'])
            logger.info(f"✅ Retrieved credentials from Secrets Manager for host: {credentials.get('host', 'unknown')}")

            return credentials

        except ClientError as e:
            logger.warning(f"Secrets Manager failed: {e}. Falling back to environment variables.")

            # Fallback to environment variables
            credentials = {
                'host': os.environ.get('DB_HOST'),
                'database': os.environ.get('DB_NAME', 'graph_db'),
                'user': os.environ.get('DB_USER', 'graph_admin'),
                'password': os.environ.get('DB_PASSWORD'),
                'port': int(os.environ.get('DB_PORT', 5432))
            }

            # Validate that we have credentials
            if not credentials['host'] or not credentials['password']:
                raise ValueError(
                    "No database credentials available. Please either:\n"
                    "  1. Set AWS_PROFILE and create secret: {env}/six-worker/database in Secrets Manager\n"
                    "  2. Set environment variables: DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT"
                )

            logger.info(f"Using environment variables for host: {credentials['host']}")
            return credentials

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
        try:
            claim_sql = job_data['claim_instruction']['sql']
            claim_params = job_data['claim_instruction']['params']

            # Convert $1, $2 style placeholders to %s for psycopg2
            import re
            claim_sql = re.sub(r'\$\d+', '%s', claim_sql)

            # Convert list to tuple for psycopg2
            if isinstance(claim_params, list):
                claim_params = tuple(claim_params)

            with self.db_conn.cursor() as cur:
                cur.execute(claim_sql, claim_params)
                self.db_conn.commit()

            logger.info(f"✅ Job claimed in Aurora: {job_data['job_id']}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Failed to execute claim: {e}")
            raise

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

    def _download_input_file(self, input_config: Dict[str, Any]) -> str:
        """
        Download input file from URL or S3, or return local file path.

        Supports three input methods:
        1. file_path: Local file path (no download needed)
        2. url: HTTP/HTTPS URL to download
        3. s3_bucket + s3_key: S3 object to download

        Returns: Path to local file (downloaded or original)
        """
        import tempfile
        import urllib.request

        # Method 1: Local file path
        if 'file_path' in input_config:
            file_path = input_config['file_path']
            logger.info(f"Using local file: {file_path}")
            return file_path

        # Method 2: Download from URL
        if 'url' in input_config:
            url = input_config['url']
            logger.info(f"📥 Downloading from URL: {url}")

            # Create temp file with appropriate extension
            file_ext = os.path.splitext(url.split('?')[0])[1] or '.csv'
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_ext)
            temp_path = temp_file.name
            temp_file.close()

            # Download file
            urllib.request.urlretrieve(url, temp_path)
            logger.info(f"✅ Downloaded to: {temp_path}")
            return temp_path

        # Method 3: Download from S3
        if 's3_bucket' in input_config and 's3_key' in input_config:
            bucket = input_config['s3_bucket']
            key = input_config['s3_key']
            logger.info(f"📥 Downloading from S3: s3://{bucket}/{key}")

            # Create temp file with appropriate extension
            file_ext = os.path.splitext(key)[1] or '.csv'
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_ext)
            temp_path = temp_file.name
            temp_file.close()

            # Download from S3 using assumed role credentials
            if self.aws_credentials:
                logger.info("Using assumed role credentials for S3 access")
                s3_client = boto3.client('s3', region_name=self.aws_region, **self.aws_credentials)
            else:
                logger.warning("No AWS credentials available, using default boto3 client")
                s3_client = boto3.client('s3', region_name=self.aws_region)
            s3_client.download_file(bucket, key, temp_path)
            logger.info(f"✅ Downloaded to: {temp_path}")
            return temp_path

        raise ValueError(
            "No valid input source specified. Provide one of:\n"
            "  - file_path: Local file path\n"
            "  - url: HTTP/HTTPS URL\n"
            "  - s3_bucket + s3_key: S3 object"
        )

    def _execute_job(self, job: Dict[str, Any]):
        """Execute the loader job"""
        job_id = job['job_id']
        job_type = job['job_type']
        config = job['config']
        temp_file_path = None

        try:
            # Mark job as running
            self._update_job_status(job_id, 'running')

            # Start heartbeat thread
            self.current_job_id = job_id
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.heartbeat_thread.start()

            # Download or get input file path
            input_config = config.get('input', {})
            file_path = self._download_input_file(input_config)

            # Track if we need to clean up temp file
            if 'url' in input_config or 's3_bucket' in input_config:
                temp_file_path = file_path

            # Dynamically load the appropriate loader class
            LoaderClass = self._load_loader_class(job_type)
            loader = LoaderClass(config)

            # Set up loader with distributed callbacks
            loader.connection = self.db_conn

            # Process file with callbacks
            loader.run(
                file_path=file_path,
                limit=config.get('processing', {}).get('limit'),
                batch_size=config.get('processing', {}).get('batch_size', 100),
                checkpoint_callback=lambda cp: self._save_checkpoint(job_id, cp),
                log_callback=lambda log: self._send_log(job_id, log),
                error_callback=lambda err: self._report_data_quality_issue(job_id, err)
            )

            # Mark job as completed
            self._complete_job(job_id)

        finally:
            # Stop heartbeat
            self.current_job_id = None

            # Clean up temp file if we downloaded one
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                logger.info(f"🗑️  Cleaned up temp file: {temp_file_path}")

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
        """Send log entry to job_logs table"""
        level = log_entry.get('level', 'INFO')
        message = log_entry.get('message', '')
        metadata = log_entry.get('metadata', {})

        # Log locally
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(f"{message} | {json.dumps(metadata)}")

        # Store in database
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO job_logs (log_id, job_id, timestamp, level, message, metadata)
                    VALUES (gen_ulid(), %s, NOW(), %s, %s, %s)
                """, (job_id, level, message, json.dumps(metadata)))
                self.db_conn.commit()
        except Exception as e:
            logger.warning(f"Failed to write log to database: {e}")

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
    parser.add_argument('--aws-region', default=None, help='AWS region (default: read from AWS_REGION or AWS_DEFAULT_REGION env var)')

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
