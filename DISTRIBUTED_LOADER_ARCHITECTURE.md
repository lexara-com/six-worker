# Distributed Loader Architecture Options

## Current Situation Analysis

**What We Have:**
- Single-node loaders (laptop → Raspberry Pi migration in progress)
- Checkpoint/resume capability (every 5,000 records)
- Centralized PostgreSQL database
- Local logging only
- Environment variable credentials
- Manual job execution

**What We Need:**
- Multi-worker job distribution
- Secure credential management
- Centralized logging/monitoring
- Error tracking and data quality workflows
- Job scheduling and orchestration

---

## Option 1: Lightweight Queue-Based (Minimal Infrastructure)

### Architecture
```
┌─────────────┐
│   PostgreSQL │ (Central DB + Job Queue)
└──────┬──────┘
       │
   ┌───┴────────────────┬─────────────┐
   │                    │             │
┌──▼───┐           ┌───▼──┐      ┌───▼──┐
│Worker│           │Worker│      │Worker│
│(RPi) │           │(Mac) │      │(RPi) │
└──────┘           └──────┘      └──────┘
```

### Components

**1. Job Queue Table** (in existing PostgreSQL)
```sql
CREATE TABLE job_queue (
    job_id VARCHAR(26) PRIMARY KEY,
    job_type VARCHAR(50),           -- 'iowa_business', 'asbestos', etc.
    status VARCHAR(20),              -- 'pending', 'claimed', 'running', 'completed', 'failed'
    worker_id VARCHAR(100),          -- Which worker claimed this
    claimed_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    config JSONB,                    -- Job configuration
    checkpoint JSONB,                -- Current progress
    error_message TEXT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    heartbeat_at TIMESTAMP           -- Worker health check
);
```

**2. Worker Registration**
```sql
CREATE TABLE workers (
    worker_id VARCHAR(100) PRIMARY KEY,
    hostname VARCHAR(255),
    ip_address VARCHAR(45),
    capabilities JSONB,              -- What job types it can run
    status VARCHAR(20),              -- 'active', 'idle', 'offline'
    last_heartbeat TIMESTAMP,
    created_at TIMESTAMP
);
```

**3. Centralized Logging**
```sql
CREATE TABLE job_logs (
    log_id VARCHAR(26) PRIMARY KEY,
    job_id VARCHAR(26) REFERENCES job_queue(job_id),
    worker_id VARCHAR(100),
    timestamp TIMESTAMP,
    level VARCHAR(10),               -- INFO, WARNING, ERROR
    message TEXT,
    metadata JSONB                   -- Extra context
);
```

**4. Data Quality Issues**
```sql
CREATE TABLE data_quality_issues (
    issue_id VARCHAR(26) PRIMARY KEY,
    job_id VARCHAR(26),
    source_record_id VARCHAR(100),   -- e.g., '080686'
    issue_type VARCHAR(50),          -- 'invalid_zip', 'missing_field', etc.
    severity VARCHAR(20),            -- 'warning', 'error', 'critical'
    field_name VARCHAR(100),
    invalid_value TEXT,
    expected_format TEXT,
    resolution_status VARCHAR(20),   -- 'pending', 'resolved', 'ignored'
    resolution_notes TEXT,
    created_at TIMESTAMP
);
```

### Implementation

**Worker Process:**
1. Poll `job_queue` for pending jobs matching capabilities
2. Claim job with UPDATE/CAS (check-and-set)
3. Update heartbeat every 60 seconds
4. Stream logs to `job_logs` table
5. Log validation errors to `data_quality_issues`
6. Update checkpoint in `job_queue.checkpoint` every 5,000 records
7. Mark complete/failed on finish

**Credential Management:**
- Store in AWS Secrets Manager
- Workers fetch on startup using IAM role (EC2/RPi with credentials)
- Rotate secrets don't break workers (re-fetch on auth failure)

### Pros
- Minimal infrastructure (just PostgreSQL)
- Easy to implement (Python + psycopg2)
- Workers can be heterogeneous (RPi, Mac, EC2)
- Fault tolerant (heartbeat + checkpoint recovery)

### Cons
- PostgreSQL as job queue (not ideal for high-throughput)
- No built-in retry/backoff logic
- Logs in DB (can get large, need partitioning)
- Manual worker deployment

### Cost
**~$0/month** (uses existing PostgreSQL)

---

## Option 2: AWS-Native (Serverless + Managed Services)

### Architecture
```
┌──────────────┐
│AWS Step      │  (Job Orchestration)
│Functions     │
└──────┬───────┘
       │
   ┌───┴────────────────────────┐
   │                            │
┌──▼──────┐              ┌──────▼─────┐
│AWS SQS  │              │AWS Lambda  │
│(Queue)  │◄─────────────┤(Workers)   │
└─────────┘              └──────┬─────┘
                                │
                         ┌──────▼─────┐
                         │PostgreSQL  │
                         │RDS         │
                         └────────────┘
```

### Components

**1. AWS Step Functions** (Workflow orchestration)
- Define job workflow as state machine
- Handle retries, timeouts, error handling
- Fan-out parallel workers
- Checkpoint management

**2. AWS SQS** (Message queue)
- FIFO queue for job ordering
- Dead-letter queue for failures
- Visibility timeout for in-progress jobs
- Auto-scaling trigger for Lambda

**3. AWS Lambda** (Workers)
- Python loader code
- Auto-scale based on queue depth
- 15-minute timeout (need chunking for long jobs)
- Environment variables from Secrets Manager

**4. AWS Secrets Manager** (Credentials)
- Database credentials
- API keys
- Automatic rotation
- IAM-based access control

**5. AWS CloudWatch** (Logging/Monitoring)
- Centralized log aggregation
- Custom metrics (records/min, errors)
- Alarms for failures
- Log Insights for querying

**6. AWS S3** (Data storage)
- Input CSV files
- Error reports
- Checkpoint snapshots

### Implementation

**Job Submission:**
```python
# Submit job to Step Functions
sfn_client.start_execution(
    stateMachineArn='arn:aws:states:...',
    input=json.dumps({
        'job_type': 'iowa_business',
        'file': 's3://bucket/data.csv',
        'config': {...}
    })
)
```

**Worker Lambda:**
```python
def lambda_handler(event, context):
    # Get credentials from Secrets Manager
    db_creds = get_secret('prod/db')

    # Process batch from SQS
    for record in event['Records']:
        job_data = json.loads(record['body'])

        # Run loader (with timeout awareness)
        loader = IowaBusinessLoader(job_data['config'])
        loader.process_batch(...)

        # Log to CloudWatch
        print(json.dumps({'metric': 'records_processed', 'count': 100}))
```

### Pros
- Fully managed (no server maintenance)
- Auto-scaling (0 → 1000 workers)
- Built-in retry/error handling
- CloudWatch logging/monitoring out-of-box
- Step Functions visual workflow

### Cons
- Vendor lock-in (AWS only)
- Lambda 15-min limit (need job chunking)
- More complex setup
- Learning curve for Step Functions

### Cost
- **Lambda:** ~$0.20 per 1M requests + compute
- **SQS:** ~$0.40 per 1M requests
- **Step Functions:** ~$25 per 1M transitions
- **CloudWatch Logs:** ~$0.50/GB ingested
- **Secrets Manager:** ~$0.40/secret/month

**Estimated:** ~$20-50/month for moderate workload

---

## Option 3: Hybrid (Kubernetes + PostgreSQL)

### Architecture
```
┌────────────────┐
│   Kubernetes   │
│   Cluster      │
│                │
│  ┌──────────┐  │
│  │ Job Queue│  │  (CronJob creates Job pods)
│  │Controller│  │
│  └─────┬────┘  │
│        │       │
│  ┌─────▼────┐  │
│  │Worker    │  │  (Pods scale 0-N)
│  │Pods      │  │
│  └─────┬────┘  │
└────────┼───────┘
         │
    ┌────▼─────┐
    │PostgreSQL│
    └──────────┘
```

### Components

**1. Kubernetes CronJob** (Scheduler)
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: iowa-business-loader
spec:
  schedule: "0 2 * * 1"  # Weekly Monday 2am
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: loader
            image: lexara/iowa-business-loader:latest
            env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: db-secret
                  key: password
```

**2. Kubernetes Job** (One-time execution)
- Create job programmatically
- Parallelism for multi-pod execution
- Auto-restart on failure
- TTL for cleanup

**3. Kubernetes Secrets** (Credentials)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: db-secret
type: Opaque
data:
  password: <base64-encoded>
```

**4. Centralized Logging**
- **Option A:** Fluent Bit → CloudWatch/Elasticsearch
- **Option B:** PostgreSQL table (like Option 1)
- **Option C:** Loki (lightweight log aggregation)

**5. Monitoring**
- Prometheus (metrics)
- Grafana (dashboards)
- AlertManager (notifications)

### Implementation

**Job Controller:**
```python
from kubernetes import client, config

def create_loader_job(job_config):
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=f"iowa-loader-{job_id}"),
        spec=client.V1JobSpec(
            parallelism=3,  # 3 worker pods
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[client.V1Container(
                        name="loader",
                        image="lexara/iowa-loader:latest",
                        env=[...env_from_secret...]
                    )]
                )
            )
        )
    )
    batch_api.create_namespaced_job(namespace="default", body=job)
```

**Worker Pod:**
- Same Python loader code
- Logs go to stdout (captured by K8s)
- Checkpoints to PostgreSQL
- Exit code 0 = success, non-zero = retry

### Pros
- Cloud-agnostic (runs anywhere)
- Built-in scheduling (CronJob)
- Auto-scaling (HPA on queue depth)
- Rich ecosystem (Helm charts, operators)
- Can run on-prem or cloud

### Cons
- Infrastructure complexity (K8s cluster)
- Learning curve (K8s concepts)
- Cluster management overhead
- Overkill for small workloads

### Cost
- **Managed K8s (EKS/GKE):** ~$75/month cluster + worker nodes
- **Self-managed (EC2):** ~$20-40/month (t3.small nodes)
- **Local (Raspberry Pi cluster):** ~$0 (just electricity)

---

## Option 4: Simple Coordinator + Workers (Recommended for MVP)

### Architecture
```
┌─────────────────┐
│  Coordinator    │  (Single Python process)
│  (Laptop/EC2)   │
└────────┬────────┘
         │ (HTTP/gRPC)
    ┌────┴────────────────┐
    │                     │
┌───▼────┐           ┌────▼───┐
│Worker 1│           │Worker 2│
│(RPi)   │           │(Mac)   │
└────┬───┘           └────┬───┘
     │                    │
     └────────┬───────────┘
          ┌───▼────┐
          │PostgreSQL
          └────────┘
```

### Components

**1. Coordinator Service** (Simple Python HTTP server)
```python
# coordinator.py
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Job queue in PostgreSQL (same as Option 1)
# Workers poll /jobs/claim to get work

@app.post("/jobs/claim")
async def claim_job(worker: WorkerInfo):
    """Worker requests a job"""
    job = find_pending_job(worker.capabilities)
    if job:
        mark_claimed(job.id, worker.id)
        return job
    return None

@app.post("/jobs/{job_id}/heartbeat")
async def heartbeat(job_id: str, worker_id: str):
    """Worker reports it's alive"""
    update_heartbeat(job_id, worker_id)

@app.post("/jobs/{job_id}/checkpoint")
async def save_checkpoint(job_id: str, checkpoint: dict):
    """Worker saves progress"""
    save_job_checkpoint(job_id, checkpoint)

@app.post("/jobs/{job_id}/log")
async def log_message(job_id: str, log: LogEntry):
    """Worker sends log"""
    insert_log(log)

@app.post("/data-quality/issue")
async def report_issue(issue: DataQualityIssue):
    """Worker reports validation error"""
    insert_data_quality_issue(issue)
```

**2. Worker Client** (Added to loader)
```python
class DistributedLoader:
    def __init__(self, coordinator_url, worker_id):
        self.coordinator = CoordinatorClient(coordinator_url)
        self.worker_id = worker_id

    def run(self):
        while True:
            # Claim job
            job = self.coordinator.claim_job(self.worker_id)
            if not job:
                time.sleep(30)
                continue

            # Start heartbeat thread
            heartbeat_thread = start_heartbeat(job.id)

            try:
                # Run loader
                loader = IowaBusinessLoader(job.config)
                loader.process_file(
                    checkpoint_callback=lambda cp:
                        self.coordinator.save_checkpoint(job.id, cp),
                    log_callback=lambda log:
                        self.coordinator.log(job.id, log),
                    error_callback=lambda err:
                        self.coordinator.report_issue(job.id, err)
                )

                # Mark complete
                self.coordinator.complete_job(job.id)
            except Exception as e:
                self.coordinator.fail_job(job.id, str(e))
            finally:
                heartbeat_thread.stop()
```

**3. Credential Management**
- Coordinator fetches from AWS Secrets Manager
- Distributes to workers via secure env vars or config
- Alternative: Workers fetch directly (if they have IAM roles)

**4. Logging**
- Workers stream to coordinator
- Coordinator writes to PostgreSQL
- Web UI to view logs (FastAPI serves HTML)

**5. Data Quality Workflow**
```python
# Coordinator exposes data quality dashboard
@app.get("/data-quality/issues")
async def list_issues(status: str = "pending"):
    """View validation errors needing attention"""
    return get_issues(status)

@app.post("/data-quality/issues/{issue_id}/resolve")
async def resolve_issue(issue_id: str, resolution: Resolution):
    """Mark issue as resolved/ignored"""
    update_issue(issue_id, resolution)
```

### Implementation Steps

1. **Week 1:** Set up coordinator
   - FastAPI service
   - Job queue tables
   - Basic job claim/heartbeat

2. **Week 2:** Update workers
   - Add coordinator client
   - Stream logs to coordinator
   - Report data quality issues

3. **Week 3:** Add monitoring
   - Simple web UI for job status
   - Data quality dashboard
   - Basic metrics

4. **Week 4:** Production hardening
   - Secrets management
   - Error handling
   - Recovery procedures

### Pros
- Simple to understand and implement
- Minimal dependencies (Python + PostgreSQL)
- Easy debugging (single coordinator process)
- Flexible deployment (laptop, EC2, Docker)
- Incremental migration (works with existing loaders)

### Cons
- Coordinator is single point of failure
- Not auto-scaling (manual worker management)
- No built-in workflow orchestration
- Limited to HTTP polling (not push-based)

### Cost
**~$5-10/month** (Small EC2 for coordinator, or $0 if run on laptop)

---

## Comparison Matrix

| Feature | Option 1<br/>Queue-Based | Option 2<br/>AWS-Native | Option 3<br/>Kubernetes | Option 4<br/>Coordinator |
|---------|----------|------------|-------------|-------------|
| **Setup Complexity** | Low | Medium | High | Low |
| **Operational Complexity** | Low | Low | High | Medium |
| **Infrastructure Cost** | $0 | $20-50/mo | $20-75/mo | $0-10/mo |
| **Auto-scaling** | No | Yes | Yes | No |
| **Multi-cloud** | Yes | No | Yes | Yes |
| **Learning Curve** | Low | Medium | High | Low |
| **Production Ready** | Medium | High | High | Medium |
| **Fault Tolerance** | Medium | High | High | Low |
| **Monitoring** | Basic | Excellent | Excellent | Basic |

---

## Recommendations

### Immediate (Next 2 weeks): **Option 4 - Coordinator + Workers**

**Why:**
- Quickest path to distributed execution
- Works with existing loaders (minimal changes)
- Raspberry Pi can start immediately
- Easy to understand and debug
- Low cost ($0 if run coordinator on laptop)

**What to build:**
1. Simple FastAPI coordinator (200 lines of code)
2. Job queue tables in PostgreSQL
3. Worker client library
4. Basic web UI for monitoring

### Medium-term (1-3 months): **Option 1 - Queue-Based**

**Why:**
- Remove coordinator single point of failure
- Workers poll PostgreSQL directly
- More robust heartbeat/recovery
- Still uses existing infrastructure

**Migration path:**
- Keep coordinator for UI/API
- Move job claiming logic to workers
- PostgreSQL becomes authoritative job store

### Long-term (3-6 months): **Option 2 - AWS-Native** or **Option 3 - Kubernetes**

**Choose AWS if:**
- Want fully managed solution
- Okay with vendor lock-in
- Need auto-scaling
- Small team (don't want ops burden)

**Choose Kubernetes if:**
- Want cloud-agnostic solution
- Have K8s expertise
- Need on-prem option
- Want ecosystem integrations

---

## Data Quality Workflow Design

Regardless of architecture choice, implement this workflow:

### 1. Capture Issues During Load
```python
# In loader validation
if not is_valid_zip(zip_code):
    issue = {
        'job_id': self.job_id,
        'source_record_id': record['corp_number'],
        'issue_type': 'invalid_zip_code',
        'field_name': 'home_office.zip',
        'invalid_value': zip_code,
        'expected_format': '5 digits or 5+4',
        'raw_record': record  # For context
    }
    self.report_data_quality_issue(issue)
```

### 2. Triage Dashboard
```
┌─────────────────────────────────────────┐
│  Data Quality Issues (127 pending)      │
├─────────────────────────────────────────┤
│                                         │
│  ❌ Invalid Zip Code (43 issues)        │
│     • 080686: "1478" → Expected 5 digits │
│     • 091234: "ABC12" → Invalid format  │
│     [Bulk Fix] [Ignore All] [Review]    │
│                                         │
│  ⚠️  Missing Required Field (31 issues) │
│     • 082301: Missing legal_name        │
│     [Manual Review]                     │
│                                         │
│  ⚠️  Date Format Issue (53 issues)      │
│     • 077889: "02/30/2020" → Invalid    │
│     [Auto-fix with fuzzy date parsing]  │
└─────────────────────────────────────────┘
```

### 3. Resolution Actions
- **Auto-fix:** Apply transformation rule (e.g., prepend "0" to 4-digit zips)
- **Manual fix:** Show record, let user correct
- **Ignore:** Mark as acceptable variance
- **Create task:** Send to data cleanup queue for batch processing

### 4. Feedback Loop
```python
# Apply learned corrections automatically
if issue.resolution_status == 'auto_fix':
    add_transformation_rule(issue.issue_type, issue.fix_applied)

# Next loader run applies the rule
for rule in get_transformation_rules():
    if rule.applies_to(record):
        record = rule.transform(record)
```

---

## Next Steps

1. **Decide on architecture** (Recommend: Start with Option 4)
2. **Set up coordinator** (1-2 days)
3. **Update loader** to use coordinator (1-2 days)
4. **Test with Raspberry Pi** (1 day)
5. **Build data quality dashboard** (2-3 days)
6. **Production deployment** (1 day)

**Total: ~1 week to distributed + monitored loaders**
