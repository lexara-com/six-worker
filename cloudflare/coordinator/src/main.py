"""
Cloudflare Coordinator Worker
Manages distributed job queue for Six Worker loaders
"""
from js import Response, Headers, Date
import json

# Worker environment will provide these bindings
# - env.HYPERDRIVE: Database connection (read-only)
# - env.JOB_QUEUE: Cloudflare Queue for job submissions
# - env.AWS_REGION: AWS region
# - env.AWS_ACCESS_KEY_ID: CloudWatch access key (secret)
# - env.AWS_SECRET_ACCESS_KEY: CloudWatch secret key (secret)

async def on_fetch(request, env):
    """Main request handler"""
    url = request.url
    method = request.method

    # Parse URL
    from urllib.parse import urlparse
    parsed = urlparse(url)
    path = parsed.path

    # CORS headers for browser access
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json'
    }

    # Handle OPTIONS (preflight)
    if method == 'OPTIONS':
        return Response.new(None, status=204, headers=Headers.new(cors_headers))

    try:
        # Route handling
        if path == '/health':
            return handle_health(env, cors_headers)

        elif path == '/jobs/claim' and method == 'POST':
            body = await request.json()
            return await handle_claim_job(body, env, cors_headers)

        elif path == '/jobs/submit' and method == 'POST':
            body = await request.json()
            return await handle_submit_job(body, env, cors_headers)

        elif path.startswith('/jobs/') and path.endswith('/status') and method == 'GET':
            job_id = path.split('/')[2]
            return await handle_job_status(job_id, env, cors_headers)

        elif path.startswith('/jobs/') and path.endswith('/heartbeat') and method == 'POST':
            job_id = path.split('/')[2]
            body = await request.json()
            return await handle_heartbeat(job_id, body, env, cors_headers)

        elif path == '/jobs' and method == 'GET':
            return await handle_list_jobs(request, env, cors_headers)

        elif path == '/workers' and method == 'GET':
            return await handle_list_workers(env, cors_headers)

        elif path == '/data-quality/issues' and method == 'GET':
            return await handle_list_dq_issues(request, env, cors_headers)

        else:
            return Response.new(
                json.dumps({'error': 'Not found'}),
                status=404,
                headers=Headers.new(cors_headers)
            )

    except Exception as e:
        return Response.new(
            json.dumps({'error': str(e)}),
            status=500,
            headers=Headers.new(cors_headers)
        )


def handle_health(env, headers_dict):
    """Health check endpoint"""
    response_body = json.dumps({
        'status': 'healthy',
        'service': 'lexara-coordinator',
        'timestamp': 'ok'
    })

    return Response.new(
        response_body,
        status=200,
        headers=headers_dict
    )


async def handle_claim_job(body, env, headers):
    """
    Worker claims a job from the queue

    Request body:
    {
        "worker_id": "rpi-001",
        "capabilities": ["iowa_business", "iowa_asbestos"]
    }

    Response:
    {
        "job_id": "01K6...",
        "job_type": "iowa_business",
        "config": {...},
        "claim_instruction": {
            "sql": "UPDATE job_queue SET...",
            "params": [...]
        }
    }
    """
    worker_id = body.get('worker_id')
    capabilities = body.get('capabilities', [])

    if not worker_id:
        return Response.new(
            json.dumps({'error': 'worker_id required'}),
            status=400,
            headers=Headers.new(headers)
        )

    # Query Hyperdrive for pending jobs
    # Note: Hyperdrive binding provides a D1-like API
    result = await env.HYPERDRIVE.prepare("""
        SELECT job_id, job_type, config, created_at
        FROM job_queue
        WHERE status = 'pending'
        AND job_type = ANY($1)
        ORDER BY created_at ASC
        LIMIT 1
    """).bind([capabilities]).first()

    if not result:
        # No jobs available
        return Response.new(None, status=204, headers=Headers.new(headers))

    # Return job with claim instruction
    response_data = {
        'job_id': result['job_id'],
        'job_type': result['job_type'],
        'config': json.loads(result['config']) if isinstance(result['config'], str) else result['config'],
        'created_at': result['created_at'].isoformat() if hasattr(result['created_at'], 'isoformat') else str(result['created_at']),

        # Tell worker how to claim in Aurora
        'claim_instruction': {
            'sql': """
                UPDATE job_queue
                SET status = 'claimed', worker_id = $1, claimed_at = NOW(), updated_at = NOW()
                WHERE job_id = $2 AND status = 'pending'
                RETURNING *
            """,
            'params': [worker_id, result['job_id']]
        }
    }

    return Response.new(
        json.dumps(response_data),
        status=200,
        headers=Headers.new(headers)
    )


async def handle_submit_job(body, env, headers):
    """
    Submit a new job to the queue

    Request body:
    {
        "job_type": "iowa_business",
        "config": {...}
    }
    """
    job_type = body.get('job_type')
    config = body.get('config', {})

    if not job_type:
        return Response.new(
            json.dumps({'error': 'job_type required'}),
            status=400,
            headers=Headers.new(headers)
        )

    # Generate ULID for job
    job_id = generate_ulid()

    # Send to Cloudflare Queue (which will write to Aurora)
    await env.JOB_QUEUE.send({
        'job_id': job_id,
        'job_type': job_type,
        'config': config,
        'created_at': Date.new().toISOString()
    })

    return Response.new(
        json.dumps({
            'job_id': job_id,
            'status': 'queued',
            'message': 'Job submitted successfully'
        }),
        status=202,
        headers=Headers.new(headers)
    )


async def handle_job_status(job_id, env, headers):
    """Get job status from Hyperdrive"""
    result = await env.HYPERDRIVE.prepare("""
        SELECT
            j.job_id,
            j.job_type,
            j.status,
            j.worker_id,
            j.checkpoint,
            j.created_at,
            j.claimed_at,
            j.started_at,
            j.completed_at,
            j.error_message,
            w.hostname,
            w.last_heartbeat
        FROM job_queue j
        LEFT JOIN workers w ON j.worker_id = w.worker_id
        WHERE j.job_id = $1
    """).bind([job_id]).first()

    if not result:
        return Response.new(
            json.dumps({'error': 'Job not found'}),
            status=404,
            headers=Headers.new(headers)
        )

    # Convert to JSON-serializable format
    job_data = {
        'job_id': result['job_id'],
        'job_type': result['job_type'],
        'status': result['status'],
        'worker_id': result['worker_id'],
        'checkpoint': json.loads(result['checkpoint']) if result['checkpoint'] else None,
        'created_at': str(result['created_at']),
        'claimed_at': str(result['claimed_at']) if result['claimed_at'] else None,
        'started_at': str(result['started_at']) if result['started_at'] else None,
        'completed_at': str(result['completed_at']) if result['completed_at'] else None,
        'error_message': result['error_message'],
        'worker': {
            'hostname': result['hostname'],
            'last_heartbeat': str(result['last_heartbeat']) if result['last_heartbeat'] else None
        } if result['hostname'] else None
    }

    return Response.new(
        json.dumps(job_data),
        status=200,
        headers=Headers.new(headers)
    )


async def handle_heartbeat(job_id, body, env, headers):
    """
    Receive heartbeat from worker
    Note: Worker updates Aurora directly, this is just for logging
    """
    worker_id = body.get('worker_id')
    metadata = body.get('metadata', {})

    # Log heartbeat (could send to CloudWatch here)
    # For now, just acknowledge

    return Response.new(
        json.dumps({'status': 'acknowledged'}),
        status=200,
        headers=Headers.new(headers)
    )


async def handle_list_jobs(request, env, headers):
    """List jobs with filtering"""
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(request.url)
    params = parse_qs(parsed.query)

    status_filter = params.get('status', ['all'])[0]
    limit = int(params.get('limit', ['50'])[0])

    # Build query
    if status_filter == 'all':
        query = "SELECT * FROM job_queue ORDER BY created_at DESC LIMIT $1"
        bind_params = [limit]
    else:
        query = "SELECT * FROM job_queue WHERE status = $1 ORDER BY created_at DESC LIMIT $2"
        bind_params = [status_filter, limit]

    results = await env.HYPERDRIVE.prepare(query).bind(bind_params).all()

    jobs = [
        {
            'job_id': r['job_id'],
            'job_type': r['job_type'],
            'status': r['status'],
            'worker_id': r['worker_id'],
            'created_at': str(r['created_at'])
        }
        for r in results['results']
    ]

    return Response.new(
        json.dumps({'jobs': jobs, 'count': len(jobs)}),
        status=200,
        headers=Headers.new(headers)
    )


async def handle_list_workers(env, headers):
    """List active workers"""
    results = await env.HYPERDRIVE.prepare("""
        SELECT worker_id, hostname, status, last_heartbeat, capabilities
        FROM workers
        WHERE status IN ('active', 'idle')
        ORDER BY last_heartbeat DESC
    """).all()

    workers = [
        {
            'worker_id': r['worker_id'],
            'hostname': r['hostname'],
            'status': r['status'],
            'last_heartbeat': str(r['last_heartbeat']),
            'capabilities': json.loads(r['capabilities']) if isinstance(r['capabilities'], str) else r['capabilities']
        }
        for r in results['results']
    ]

    return Response.new(
        json.dumps({'workers': workers, 'count': len(workers)}),
        status=200,
        headers=Headers.new(headers)
    )


async def handle_list_dq_issues(request, env, headers):
    """List data quality issues"""
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(request.url)
    params = parse_qs(parsed.query)

    status_filter = params.get('status', ['pending'])[0]
    limit = int(params.get('limit', ['100'])[0])

    results = await env.HYPERDRIVE.prepare("""
        SELECT
            issue_id, job_id, source_record_id, issue_type, severity,
            field_name, invalid_value, expected_format, message,
            resolution_status, created_at
        FROM data_quality_issues
        WHERE resolution_status = $1
        ORDER BY created_at DESC
        LIMIT $2
    """).bind([status_filter, limit]).all()

    issues = [
        {
            'issue_id': r['issue_id'],
            'job_id': r['job_id'],
            'source_record_id': r['source_record_id'],
            'issue_type': r['issue_type'],
            'severity': r['severity'],
            'field_name': r['field_name'],
            'invalid_value': r['invalid_value'],
            'expected_format': r['expected_format'],
            'message': r['message'],
            'resolution_status': r['resolution_status'],
            'created_at': str(r['created_at'])
        }
        for r in results['results']
    ]

    return Response.new(
        json.dumps({'issues': issues, 'count': len(issues)}),
        status=200,
        headers=Headers.new(headers)
    )


def generate_ulid():
    """Generate ULID (Universally Unique Lexicographically Sortable Identifier)"""
    import time
    import random

    # ULID format: 01ARYZ6S41 (10 chars timestamp) + TST3QV (16 chars randomness)
    # Simplified version for Cloudflare Workers
    timestamp = int(time.time() * 1000)

    # Crockford's Base32
    alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    # Encode timestamp
    ts_str = ''
    ts = timestamp
    for _ in range(10):
        ts_str = alphabet[ts % 32] + ts_str
        ts //= 32

    # Add randomness
    random_str = ''.join(random.choice(alphabet) for _ in range(16))

    return ts_str + random_str


# Export handler
export_default = {
    'fetch': on_fetch
}
