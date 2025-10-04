/**
 * Cloudflare Queue Consumer Worker (TypeScript)
 * Consumes job submissions from queue and writes them to Aurora PostgreSQL
 */
import postgres from 'postgres';

interface Env {
	HYPERDRIVE: Hyperdrive;
	ENVIRONMENT: string;
	AWS_REGION: string;
	AWS_ACCESS_KEY_ID?: string;
	AWS_SECRET_ACCESS_KEY?: string;
}

interface JobMessage {
	job_id: string;
	job_type: string;
	config: Record<string, any>;
	created_at: string;
}

export default {
	// Add a fetch handler for testing database connectivity
	async fetch(request: Request, env: Env): Promise<Response> {
		console.log('🧪 Test endpoint called');

		try {
			console.log(`🔌 Testing Hyperdrive connection`);

			const sql = postgres(env.HYPERDRIVE.connectionString);

			// Try a simple query
			const result = await sql`SELECT COUNT(*) as count FROM job_queue`;
			console.log('✅ Query result:', result);

			await sql.end();

			return new Response(JSON.stringify({
				success: true,
				count: result[0].count,
				message: 'Hyperdrive connection successful'
			}), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error: any) {
			console.error('❌ Database test failed:', error);
			return new Response(JSON.stringify({
				success: false,
				error: error.message,
				stack: error.stack
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	},

	async queue(batch: MessageBatch<JobMessage>, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`🔔 Queue consumer received ${batch.messages.length} messages`);

		// Connect to Aurora PostgreSQL via Hyperdrive (write access)
		let sql;
		try {
			console.log(`🔌 Connecting via Hyperdrive`);

			sql = postgres(env.HYPERDRIVE.connectionString);

			console.log(`✅ Hyperdrive connection created`);

			// Process each message in the batch
			for (const message of batch.messages) {
				try {
					const job = message.body;

					console.log(`📋 Processing job: ${job.job_id} (${job.job_type})`);

					// Insert job into Aurora
					const result = await sql`
						INSERT INTO job_queue (
							job_id,
							job_type,
							config,
							status,
							created_at,
							updated_at
						) VALUES (
							${job.job_id},
							${job.job_type},
							${JSON.stringify(job.config)},
							'pending',
							NOW(),
							NOW()
						)
						ON CONFLICT (job_id) DO NOTHING
						RETURNING job_id
					`;

					console.log(`✅ Job ${job.job_id} written to database`, result);

					// Acknowledge successful processing
					message.ack();

				} catch (error) {
					console.error(`❌ Error processing job ${message.body.job_id}:`, error);
					console.error('Error details:', error instanceof Error ? error.stack : String(error));

					// Retry the message (don't ack)
					message.retry();
				}
			}
		} catch (error) {
			console.error('❌ Database connection or batch processing error:', error);
			console.error('Error details:', error instanceof Error ? error.stack : String(error));

			// Retry all messages in batch
			for (const message of batch.messages) {
				message.retry();
			}
		} finally {
			// Clean up database connection
			if (sql) {
				try {
					await sql.end();
					console.log('🔌 Database connection closed');
				} catch (e) {
					console.error('Error closing DB connection:', e);
				}
			}
		}
	},
};

async function logToCloudWatch(env: Env, logEntry: Record<string, any>): Promise<void> {
	// TODO: Implement CloudWatch logging using AWS SDK
	// For now, just log to console (visible in Cloudflare dashboard)
	console.log(
		JSON.stringify({
			...logEntry,
			timestamp: new Date().toISOString(),
			worker: 'queue-consumer',
			environment: env.ENVIRONMENT,
		})
	);

	// Future implementation:
	// import { CloudWatchLogsClient, PutLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
	// const client = new CloudWatchLogsClient({
	//   region: env.AWS_REGION,
	//   credentials: {
	//     accessKeyId: env.AWS_ACCESS_KEY_ID,
	//     secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
	//   },
	// });
	// await client.send(new PutLogEventsCommand({ ... }));
}
