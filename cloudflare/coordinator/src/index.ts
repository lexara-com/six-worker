/**
 * Cloudflare Coordinator Worker (TypeScript)
 * Manages distributed job queue for Six Worker loaders
 */
import postgres from 'postgres';

interface Env {
	HYPERDRIVE: Hyperdrive;
	JOB_QUEUE: Queue;
	ENVIRONMENT: string;
	AWS_REGION: string;
	AWS_ACCESS_KEY_ID?: string;
	AWS_SECRET_ACCESS_KEY?: string;
}

interface ClaimJobRequest {
	worker_id: string;
	capabilities: string[];
}

interface SubmitJobRequest {
	job_type: string;
	config: Record<string, any>;
}

const CORS_HEADERS = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
	'Access-Control-Allow-Headers': 'Content-Type',
	'Content-Type': 'application/json',
};

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;
		const method = request.method;

		// Handle CORS preflight
		if (method === 'OPTIONS') {
			return new Response(null, { status: 204, headers: CORS_HEADERS });
		}

		try {
			// Route handling
			if (path === '/health') {
				return handleHealth(env);
			}

			if (path === '/jobs/claim' && method === 'POST') {
				const body = await request.json<ClaimJobRequest>();
				return await handleClaimJob(body, env);
			}

			if (path === '/jobs/submit' && method === 'POST') {
				const body = await request.json<SubmitJobRequest>();
				return await handleSubmitJob(body, env);
			}

			if (path.match(/^\/jobs\/[^/]+\/status$/) && method === 'GET') {
				const jobId = path.split('/')[2];
				return await handleJobStatus(jobId, env);
			}

			if (path.match(/^\/jobs\/[^/]+\/heartbeat$/) && method === 'POST') {
				const jobId = path.split('/')[2];
				const body = await request.json();
				return await handleHeartbeat(jobId, body, env);
			}

			if (path === '/jobs' && method === 'GET') {
				return await handleListJobs(url, env);
			}

			if (path === '/workers' && method === 'GET') {
				return await handleListWorkers(env);
			}

			if (path === '/data-quality/issues' && method === 'GET') {
				return await handleListDQIssues(url, env);
			}

			return jsonResponse({ error: 'Not found' }, 404);

		} catch (error: any) {
			console.error('Error:', error);
			return jsonResponse({ error: error.message || 'Internal server error' }, 500);
		}
	},
};

function jsonResponse(data: any, status: number = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: CORS_HEADERS,
	});
}

function handleHealth(env: Env): Response {
	return jsonResponse({
		status: 'healthy',
		service: 'lexara-coordinator',
		timestamp: new Date().toISOString(),
		environment: env.ENVIRONMENT,
	});
}

async function handleClaimJob(body: ClaimJobRequest, env: Env): Promise<Response> {
	const { worker_id, capabilities } = body;

	if (!worker_id) {
		return jsonResponse({ error: 'worker_id required' }, 400);
	}

	const sql = postgres(env.HYPERDRIVE.connectionString);

	try {
		// Query for pending jobs matching capabilities
		const results = await sql`
			SELECT job_id, job_type, config, created_at
			FROM job_queue
			WHERE status = 'pending'
			AND job_type = ANY(${capabilities})
			ORDER BY created_at ASC
			LIMIT 1
		`;

		if (results.length === 0) {
			await sql.end();
			return new Response(null, { status: 204, headers: CORS_HEADERS });
		}

		const job = results[0];

		// Parse config if it's a string
		let config = job.config;
		if (typeof config === 'string') {
			try {
				config = JSON.parse(config);
			} catch (e) {
				console.error('Failed to parse config:', e);
			}
		}

		// Return job with claim instruction for worker to execute
		const responseData = {
			job_id: job.job_id,
			job_type: job.job_type,
			config: config,
			created_at: job.created_at,
			claim_instruction: {
				sql: `
					UPDATE job_queue
					SET status = 'claimed', worker_id = $1, claimed_at = NOW(), updated_at = NOW()
					WHERE job_id = $2 AND status = 'pending'
					RETURNING *
				`,
				params: [worker_id, job.job_id],
			},
		};

		await sql.end();
		return jsonResponse(responseData);
	} catch (error: any) {
		await sql.end();
		console.error('Error claiming job:', error);
		return jsonResponse({ error: error.message }, 500);
	}
}

async function handleSubmitJob(body: SubmitJobRequest, env: Env): Promise<Response> {
	const { job_type, config } = body;

	if (!job_type) {
		return jsonResponse({ error: 'job_type required' }, 400);
	}

	// Generate ULID for job
	const job_id = generateULID();

	// Send to Cloudflare Queue (which will write to Aurora)
	await env.JOB_QUEUE.send({
		job_id,
		job_type,
		config,
		created_at: new Date().toISOString(),
	});

	return jsonResponse(
		{
			job_id,
			status: 'queued',
			message: 'Job submitted successfully',
		},
		202
	);
}

async function handleJobStatus(jobId: string, env: Env): Promise<Response> {
	const sql = postgres(env.HYPERDRIVE.connectionString);

	try {
		const results = await sql`
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
			WHERE j.job_id = ${jobId}
		`;

		if (results.length === 0) {
			await sql.end();
			return jsonResponse({ error: 'Job not found' }, 404);
		}

		const result = results[0];

		// Parse checkpoint if it's a string
		let checkpoint = result.checkpoint;
		if (typeof checkpoint === 'string' && checkpoint) {
			try {
				checkpoint = JSON.parse(checkpoint);
			} catch (e) {
				console.error('Failed to parse checkpoint:', e);
			}
		}

		const jobData = {
			job_id: result.job_id,
			job_type: result.job_type,
			status: result.status,
			worker_id: result.worker_id,
			checkpoint: checkpoint,
			created_at: result.created_at,
			claimed_at: result.claimed_at,
			started_at: result.started_at,
			completed_at: result.completed_at,
			error_message: result.error_message,
			worker: result.hostname
				? {
						hostname: result.hostname,
						last_heartbeat: result.last_heartbeat,
				  }
				: null,
		};

		await sql.end();
		return jsonResponse(jobData);
	} catch (error: any) {
		await sql.end();
		console.error('Error fetching job status:', error);
		return jsonResponse({ error: error.message }, 500);
	}
}

async function handleHeartbeat(jobId: string, body: any, env: Env): Promise<Response> {
	// Worker updates Aurora directly, this is just for acknowledgment
	return jsonResponse({ status: 'acknowledged' });
}

async function handleListJobs(url: URL, env: Env): Promise<Response> {
	const statusFilter = url.searchParams.get('status') || 'all';
	const limit = parseInt(url.searchParams.get('limit') || '50');

	const sql = postgres(env.HYPERDRIVE.connectionString);

	try {
		let results;
		if (statusFilter === 'all') {
			results = await sql`
				SELECT job_id, job_type, status, worker_id, created_at
				FROM job_queue
				ORDER BY created_at DESC
				LIMIT ${limit}
			`;
		} else {
			results = await sql`
				SELECT job_id, job_type, status, worker_id, created_at
				FROM job_queue
				WHERE status = ${statusFilter}
				ORDER BY created_at DESC
				LIMIT ${limit}
			`;
		}

		const jobs = results.map((r: any) => ({
			job_id: r.job_id,
			job_type: r.job_type,
			status: r.status,
			worker_id: r.worker_id,
			created_at: r.created_at,
		}));

		await sql.end();
		return jsonResponse({ jobs, count: jobs.length });
	} catch (error: any) {
		await sql.end();
		console.error('Error querying jobs:', error);
		return jsonResponse({ error: error.message }, 500);
	}
}

async function handleListWorkers(env: Env): Promise<Response> {
	const sql = postgres(env.HYPERDRIVE.connectionString);

	try {
		const results = await sql`
			SELECT worker_id, hostname, status, last_heartbeat, capabilities
			FROM workers
			WHERE status IN ('active', 'idle')
			ORDER BY last_heartbeat DESC
		`;

		const workers = results.map((r: any) => {
			let capabilities = r.capabilities;
			if (typeof capabilities === 'string') {
				try {
					capabilities = JSON.parse(capabilities);
				} catch (e) {
					capabilities = [];
				}
			}

			return {
				worker_id: r.worker_id,
				hostname: r.hostname,
				status: r.status,
				last_heartbeat: r.last_heartbeat,
				capabilities: capabilities,
			};
		});

		await sql.end();
		return jsonResponse({ workers, count: workers.length });
	} catch (error: any) {
		await sql.end();
		console.error('Error listing workers:', error);
		return jsonResponse({ error: error.message }, 500);
	}
}

async function handleListDQIssues(url: URL, env: Env): Promise<Response> {
	const statusFilter = url.searchParams.get('status') || 'pending';
	const limit = parseInt(url.searchParams.get('limit') || '100');

	const sql = postgres(env.HYPERDRIVE.connectionString);

	try {
		const results = await sql`
			SELECT
				issue_id, job_id, source_record_id, issue_type, severity,
				field_name, invalid_value, expected_format, message,
				resolution_status, created_at
			FROM data_quality_issues
			WHERE resolution_status = ${statusFilter}
			ORDER BY created_at DESC
			LIMIT ${limit}
		`;

		const issues = results.map((r: any) => ({
			issue_id: r.issue_id,
			job_id: r.job_id,
			source_record_id: r.source_record_id,
			issue_type: r.issue_type,
			severity: r.severity,
			field_name: r.field_name,
			invalid_value: r.invalid_value,
			expected_format: r.expected_format,
			message: r.message,
			resolution_status: r.resolution_status,
			created_at: r.created_at,
		}));

		await sql.end();
		return jsonResponse({ issues, count: issues.length });
	} catch (error: any) {
		await sql.end();
		console.error('Error listing data quality issues:', error);
		return jsonResponse({ error: error.message }, 500);
	}
}

function generateULID(): string {
	// ULID format: 10 chars timestamp + 16 chars randomness (Crockford's Base32)
	const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
	const timestamp = Date.now();

	// Encode timestamp (10 characters)
	let ts = timestamp;
	let tsStr = '';
	for (let i = 0; i < 10; i++) {
		tsStr = ALPHABET[ts % 32] + tsStr;
		ts = Math.floor(ts / 32);
	}

	// Add randomness (16 characters)
	let randomStr = '';
	for (let i = 0; i < 16; i++) {
		randomStr += ALPHABET[Math.floor(Math.random() * 32)];
	}

	return tsStr + randomStr;
}
