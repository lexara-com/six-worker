"""
Cloudflare Queue Consumer Worker
Receives job submissions and writes them to Aurora PostgreSQL
"""
import json
from datetime import datetime

# This worker consumes messages from Cloudflare Queue
# and writes them directly to Aurora PostgreSQL

async def on_queue(batch, env):
    """
    Queue consumer handler

    Args:
        batch: MessageBatch from Cloudflare Queue
        env: Worker environment with bindings
    """
    # Get database credentials from Secrets Manager
    db_creds = await get_aurora_credentials(env)

    # Import psycopg (PostgreSQL driver for Python)
    # Note: Availability depends on Cloudflare Python Workers support
    import psycopg

    # Connect to Aurora (direct write connection)
    async with psycopg.AsyncConnection.connect(
        host=db_creds['host'],
        dbname=db_creds['database'],
        user=db_creds['user'],
        password=db_creds['password'],
        port=db_creds['port']
    ) as conn:
        async with conn.cursor() as cur:
            # Process each message in batch
            for message in batch.messages:
                try:
                    job_data = message.body

                    # Insert job into Aurora
                    await cur.execute("""
                        INSERT INTO job_queue (
                            job_id,
                            job_type,
                            config,
                            status,
                            created_at,
                            updated_at
                        ) VALUES (
                            %(job_id)s,
                            %(job_type)s,
                            %(config)s,
                            'pending',
                            NOW(),
                            NOW()
                        )
                        ON CONFLICT (job_id) DO NOTHING
                    """, {
                        'job_id': job_data['job_id'],
                        'job_type': job_data['job_type'],
                        'config': json.dumps(job_data['config'])
                    })

                    # Commit transaction
                    await conn.commit()

                    # Acknowledge message
                    message.ack()

                    # Log success
                    await log_to_cloudwatch(env, {
                        'level': 'INFO',
                        'message': 'Job created in queue',
                        'job_id': job_data['job_id'],
                        'job_type': job_data['job_type']
                    })

                except Exception as e:
                    # Log error
                    await log_to_cloudwatch(env, {
                        'level': 'ERROR',
                        'message': f'Failed to process queue message: {str(e)}',
                        'job_id': job_data.get('job_id', 'unknown'),
                        'error': str(e)
                    })

                    # Retry message (don't ack)
                    message.retry()


async def get_aurora_credentials(env):
    """Fetch Aurora credentials from AWS Secrets Manager"""
    from js import fetch, Headers

    # Use AWS Secrets Manager API
    # Note: This requires AWS SDK or direct API calls

    # Alternative: Use environment variables if secrets are pre-loaded
    # For MVP, we can store credentials in Cloudflare secrets

    # Return credentials dict
    # In production, fetch from Secrets Manager using AWS SDK

    # For now, use environment variables (set via wrangler secret)
    return {
        'host': env.DB_HOST,
        'database': env.DB_NAME,
        'user': env.DB_USER,
        'password': env.DB_PASSWORD,
        'port': int(env.DB_PORT or 5432)
    }


async def log_to_cloudwatch(env, log_entry):
    """Send log to CloudWatch Logs"""
    # Import AWS SDK for CloudWatch
    # Note: This may require custom implementation for Cloudflare Workers

    try:
        # For MVP: Log to console (visible in Cloudflare dashboard)
        print(json.dumps({
            **log_entry,
            'timestamp': datetime.utcnow().isoformat(),
            'worker': 'queue-consumer',
            'environment': env.ENVIRONMENT
        }))

        # Future: Send to CloudWatch using AWS SDK
        # from js import fetch
        # await send_to_cloudwatch(env, log_entry)

    except Exception as e:
        print(f"Error logging to CloudWatch: {e}")


# Export handler
export_default = {
    'queue': on_queue
}
