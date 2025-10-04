# Cloudflare Workers for Distributed Loaders

This directory contains Cloudflare Python Workers for the distributed loader system.

## Workers

### 1. **Coordinator** (`coordinator/`)
- Manages job queue
- Provides API for job claiming, status, submission
- Reads from Aurora via Hyperdrive (read-only)
- Routes: `/jobs/claim`, `/jobs/submit`, `/jobs/status/:id`, `/health`

### 2. **Queue Consumer** (`queue_consumer/`)
- Consumes Cloudflare Queue messages
- Writes jobs to Aurora PostgreSQL
- Handles job creation from coordinator submissions

## Prerequisites

1. **Cloudflare Account** with Workers Paid plan (for Python Workers)
2. **Wrangler CLI** installed:
   ```bash
   npm install -g wrangler
   ```
3. **AWS Infrastructure** deployed (Secrets Manager, CloudWatch)
4. **Aurora PostgreSQL** running with Hyperdrive user created
5. **Terraform outputs** from `infrastructure/aws/`

## Setup Instructions

### Step 1: Create Cloudflare Queues

```bash
# Production queue
wrangler queues create job-queue-prod

# Dead letter queue
wrangler queues create job-queue-prod-dlq

# Staging/dev queues
wrangler queues create job-queue-staging
wrangler queues create job-queue-dev
```

### Step 2: Create Hyperdrive Configuration

#### Option A: Via Cloudflare Dashboard
1. Go to Workers & Pages → Hyperdrive
2. Click "Create Hyperdrive"
3. Fill in details:
   - **Name**: `lexara-aurora-prod`
   - **Protocol**: PostgreSQL
   - **Host**: (from terraform output `db_host`)
   - **Port**: 5432
   - **Database**: graph_db
   - **Username**: hyperdrive_reader
   - **Password**: (from terraform.tfvars `db_password_read`)

#### Option B: Via Wrangler CLI
```bash
# Get Hyperdrive ID after creation
npx wrangler hyperdrive create lexara-aurora-prod \
  --connection-string="postgres://hyperdrive_reader:<password>@<host>:5432/graph_db"

# Note the Hyperdrive ID from output
```

### Step 3: Update wrangler.toml Files

In `coordinator/wrangler.toml`:
```toml
[[env.production.hyperdrive]]
binding = "HYPERDRIVE"
id = "<HYPERDRIVE_ID_FROM_STEP_2>"
```

### Step 4: Set Secrets

#### Coordinator Secrets
```bash
cd coordinator

# CloudWatch credentials (from terraform outputs)
wrangler secret put AWS_ACCESS_KEY_ID --env production
# Paste: <cloudwatch_access_key_id from terraform>

wrangler secret put AWS_SECRET_ACCESS_KEY --env production
# Paste: <cloudwatch_secret_access_key from terraform>
```

#### Queue Consumer Secrets
```bash
cd queue_consumer

# Aurora write credentials
wrangler secret put DB_HOST --env production
# Enter: six-worker-cluster.cluster-xxx.us-east-1.rds.amazonaws.com

wrangler secret put DB_USER --env production
# Enter: graph_admin

wrangler secret put DB_PASSWORD --env production
# Enter: <db_password_write from terraform.tfvars>

# CloudWatch credentials
wrangler secret put AWS_ACCESS_KEY_ID --env production
wrangler secret put AWS_SECRET_ACCESS_KEY --env production
```

### Step 5: Deploy Workers

```bash
# Deploy Coordinator
cd coordinator
wrangler deploy --env production

# Deploy Queue Consumer
cd ../queue_consumer
wrangler deploy --env production
```

### Step 6: Verify Deployment

```bash
# Test coordinator health
curl https://lexara-coordinator-prod.workers.dev/health

# Should return:
# {"status":"healthy","service":"lexara-coordinator","timestamp":"..."}
```

## API Reference

### Coordinator Endpoints

#### `GET /health`
Health check

**Response:**
```json
{
  "status": "healthy",
  "service": "lexara-coordinator",
  "timestamp": "2025-10-03T10:30:00Z"
}
```

#### `POST /jobs/claim`
Worker claims a job

**Request:**
```json
{
  "worker_id": "rpi-001",
  "capabilities": ["iowa_business", "iowa_asbestos"]
}
```

**Response (200):**
```json
{
  "job_id": "01K6N3TM...",
  "job_type": "iowa_business",
  "config": {...},
  "created_at": "2025-10-03T10:00:00Z",
  "claim_instruction": {
    "sql": "UPDATE job_queue SET...",
    "params": ["rpi-001", "01K6N3TM..."]
  }
}
```

**Response (204):** No jobs available

#### `POST /jobs/submit`
Submit new job

**Request:**
```json
{
  "job_type": "iowa_business",
  "config": {
    "file_path": "s3://bucket/data.csv",
    "batch_size": 1000
  }
}
```

**Response:**
```json
{
  "job_id": "01K6N3TM...",
  "status": "queued",
  "message": "Job submitted successfully"
}
```

#### `GET /jobs/:job_id/status`
Get job status

**Response:**
```json
{
  "job_id": "01K6N3TM...",
  "job_type": "iowa_business",
  "status": "running",
  "worker_id": "rpi-001",
  "checkpoint": {"records_processed": 5000},
  "created_at": "...",
  "claimed_at": "...",
  "worker": {
    "hostname": "raspberrypi",
    "last_heartbeat": "..."
  }
}
```

#### `GET /jobs?status=<status>&limit=<n>`
List jobs

**Response:**
```json
{
  "jobs": [
    {
      "job_id": "...",
      "job_type": "iowa_business",
      "status": "completed",
      "worker_id": "rpi-001",
      "created_at": "..."
    }
  ],
  "count": 10
}
```

#### `GET /workers`
List active workers

**Response:**
```json
{
  "workers": [
    {
      "worker_id": "rpi-001",
      "hostname": "raspberrypi",
      "status": "active",
      "last_heartbeat": "...",
      "capabilities": ["iowa_business"]
    }
  ],
  "count": 3
}
```

#### `GET /data-quality/issues?status=pending&limit=100`
List data quality issues

**Response:**
```json
{
  "issues": [
    {
      "issue_id": "...",
      "job_id": "...",
      "source_record_id": "080686",
      "issue_type": "invalid_zip",
      "severity": "warning",
      "field_name": "home_office.zip",
      "invalid_value": "1478",
      "expected_format": "5 digits",
      "message": "Invalid zip code format",
      "resolution_status": "pending",
      "created_at": "..."
    }
  ],
  "count": 127
}
```

## Local Development

### Test Coordinator Locally
```bash
cd coordinator
wrangler dev --env development

# In another terminal:
curl http://localhost:8787/health
```

### Test Queue Consumer Locally
```bash
cd queue_consumer
wrangler dev --env development

# Send test message to queue (via dashboard or API)
```

## Monitoring

### View Logs
```bash
# Coordinator logs
wrangler tail lexara-coordinator-prod

# Queue consumer logs
wrangler tail lexara-queue-consumer-prod
```

### CloudWatch Dashboard
View metrics in AWS CloudWatch:
```bash
cd ../../infrastructure/aws
terraform output dashboard_url
```

## Troubleshooting

### Error: "Hyperdrive binding not found"
- Ensure Hyperdrive ID is set in wrangler.toml
- Verify Hyperdrive configuration exists in Cloudflare dashboard

### Error: "Queue not found"
- Create queue using `wrangler queues create`
- Ensure queue name matches wrangler.toml

### Error: "Database connection failed"
- Verify DB_HOST, DB_USER, DB_PASSWORD secrets are set
- Check Aurora security group allows Cloudflare IPs
- Test connection from Hyperdrive dashboard

### Error: "AWS credentials invalid"
- Verify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
- Check IAM user has CloudWatch PutLogEvents permission

## Cost Estimate

**Cloudflare Workers:**
- Workers Paid plan: $5/month
- Requests: $0.50 per million requests
- Queue operations: $0.40 per million operations

**Estimated monthly cost:**
- Base: $5
- API calls (1M/month): $0.50
- Queue operations (1M/month): $0.40
- **Total**: ~$6-10/month

## Next Steps

After deploying Cloudflare Workers:
1. Set up Python distributed worker client
2. Configure Raspberry Pi with worker client
3. Submit test job via coordinator
4. Monitor execution in CloudWatch dashboard
