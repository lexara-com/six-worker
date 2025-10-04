# AWS Infrastructure for Distributed Loaders

This directory contains Terraform configuration for AWS resources needed by the distributed loader system.

## Resources Created

### Secrets Manager
- `{env}/database-write` - Aurora credentials for write access (Python workers)
- `{env}/database-read` - Aurora credentials for read-only access (Hyperdrive)
- `{env}/cloudwatch` - CloudWatch logging credentials

### IAM
- `cloudwatch-logger` IAM user - For Cloudflare Workers to write logs
- `loader-worker` IAM role - For EC2/container workers
- Instance profile for EC2 workers

### CloudWatch
- Log group: `/lexara/distributed-loaders`
- Metric filters for errors, velocity, completions
- Alarms for high error rate, stalled jobs
- Dashboard for monitoring
- Log Insights saved queries

## Prerequisites

1. **Terraform installed** (>= 1.0)
2. **AWS CLI configured** with appropriate credentials
3. **Aurora PostgreSQL cluster** already created
4. **Database users created**:
   - `graph_admin` (write access)
   - `hyperdrive_reader` (read-only)

## Setup Instructions

### 1. Configure Variables

```bash
cd infrastructure/aws
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:
- `environment` - Your environment name
- `db_host` - Your Aurora cluster endpoint
- `db_password_write` - Password for graph_admin
- `db_password_read` - Password for hyperdrive_reader

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Review Plan

```bash
terraform plan
```

Review the resources that will be created.

### 4. Apply Configuration

```bash
terraform apply
```

Type `yes` to confirm.

### 5. Retrieve Outputs

After apply completes, get the important values:

```bash
# CloudWatch credentials for Cloudflare Workers
terraform output cloudwatch_access_key_id
terraform output -raw cloudwatch_secret_access_key

# Secret ARNs
terraform output database_write_secret_arn
terraform output database_read_secret_arn
terraform output cloudwatch_secret_arn

# Dashboard URL
terraform output dashboard_url
```

## Cloudflare Configuration

### Set Environment Variables in Cloudflare Workers

Use the Terraform outputs to configure Cloudflare Workers:

```bash
# Set AWS credentials for Cloudflare Workers
wrangler secret put AWS_ACCESS_KEY_ID
# Paste: <cloudwatch_access_key_id from terraform output>

wrangler secret put AWS_SECRET_ACCESS_KEY
# Paste: <cloudwatch_secret_access_key from terraform output>

# Set AWS region
wrangler secret put AWS_REGION
# Enter: us-east-1
```

### Configure Hyperdrive

In Cloudflare Dashboard:
1. Go to Workers & Pages → Hyperdrive
2. Create new Hyperdrive configuration:
   - Name: `lexara-aurora-hyperdrive`
   - Protocol: PostgreSQL
   - Host: `<db_host from terraform.tfvars>`
   - Port: 5432
   - Database: `graph_db`
   - Username: `hyperdrive_reader`
   - Password: `<db_password_read from terraform.tfvars>`

Or use Wrangler:

```bash
npx wrangler hyperdrive create lexara-aurora-hyperdrive \
  --connection-string="postgres://hyperdrive_reader:<password>@<db_host>:5432/graph_db"
```

## EC2 Worker Configuration

If running Python workers on EC2:

### 1. Launch EC2 with IAM Role

```bash
aws ec2 run-instances \
  --image-id ami-xxxxx \
  --instance-type t3.small \
  --iam-instance-profile Name=production-loader-worker \
  --user-data file://worker-userdata.sh
```

### 2. Worker will automatically have access to:
- Secrets Manager (database-write, cloudwatch)
- CloudWatch Logs

## Raspberry Pi Worker Configuration

For Raspberry Pi workers (no IAM role), use access keys:

### 1. Create IAM User for RPi

```bash
aws iam create-user --user-name rpi-loader-worker
aws iam attach-user-policy --user-name rpi-loader-worker \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
aws iam create-access-key --user-name rpi-loader-worker
```

### 2. Configure RPi with Access Keys

```bash
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_REGION=us-east-1
```

## Monitoring

### CloudWatch Dashboard

Visit the dashboard URL from terraform output:
```bash
terraform output dashboard_url
```

### Log Insights Queries

Pre-configured queries available in CloudWatch Log Insights:
- `{env}/loader-job-performance` - Job execution metrics
- `{env}/data-quality-issues` - Data validation errors
- `{env}/worker-activity` - Worker health and activity

### Alarms

Alarms will trigger when:
- Error rate exceeds 10 errors in 5 minutes
- No jobs complete in 30 minutes
- Processing velocity drops below 30 records/min

## Secrets Rotation

To rotate database passwords:

### 1. Update Aurora Password

```bash
aws rds modify-db-cluster \
  --db-cluster-identifier six-worker-cluster \
  --master-user-password <new-password> \
  --apply-immediately
```

### 2. Update Secrets Manager

```bash
# Update write credentials
aws secretsmanager update-secret \
  --secret-id production/database-write \
  --secret-string '{
    "host": "...",
    "database": "graph_db",
    "user": "graph_admin",
    "password": "<new-password>",
    "port": 5432
  }'

# Update read credentials
aws secretsmanager update-secret \
  --secret-id production/database-read \
  --secret-string '{
    "host": "...",
    "database": "graph_db",
    "user": "hyperdrive_reader",
    "password": "<new-password>",
    "port": 5432
  }'
```

### 3. Update Hyperdrive

Update the Hyperdrive configuration in Cloudflare Dashboard with new password.

### 4. Restart Workers

Workers will automatically fetch the new credentials on next job.

## Cleanup

To destroy all resources:

```bash
terraform destroy
```

**Warning**: This will delete all secrets and IAM resources. Make sure you have backups!

## Cost Estimate

- **Secrets Manager**: ~$1.20/month (3 secrets × $0.40)
- **CloudWatch Logs**: ~$0.50/GB ingested
- **CloudWatch Metrics**: Free (first 10 metrics)
- **CloudWatch Alarms**: Free (first 10 alarms)
- **IAM**: Free

**Total**: ~$5-15/month (depending on log volume)
