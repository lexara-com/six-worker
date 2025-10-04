# Environment Setup & Database Mapping

**Date**: October 3, 2025

## Current Configuration

### Hyperdrive Instances

| Hyperdrive Name | Environment | Database | Status |
|----------------|-------------|----------|--------|
| `lexara-aurora-prod` | **Development** (temporary) | 98.85.51.253:5432/graph_db | ✅ Active |
| `lexara-aurora-staging` | Staging | TBD | ⏳ Not Created |
| `lexara-aurora-test` | Test | TBD | ⏳ Not Created |

**Note**: The "prod" Hyperdrive instance is currently mapped to the **dev** database for testing purposes. This will be remapped to the actual production database once AWS production accounts are set up.

### Cloudflare Workers

| Worker | Environment | Hyperdrive Binding | Database Access |
|--------|-------------|-------------------|-----------------|
| `lexara-coordinator-prod` | Production Config | `lexara-aurora-prod` | Dev DB (98.85.51.253) |
| `lexara-coordinator-staging` | Staging Config | Not configured | - |
| `lexara-coordinator-dev` | Dev Config | Not configured | - |

### Database Environments

#### Development Database (Current)
- **Host**: 98.85.51.253 (EC2 instance)
- **Database**: graph_db
- **Region**: us-east-1
- **Users**:
  - `graph_admin` - Full read/write (for workers and loaders)
  - `hyperdrive_reader` - Read-only (for Cloudflare Hyperdrive)
- **Status**: ✅ Active and tested
- **Tables**: V27 migration applied, all job management tables created

#### Test Database (Future)
- **Status**: ⏳ Not set up yet
- **Purpose**: Integration testing, QA validation
- **Access**: Will need separate Hyperdrive instance

#### Production Database (Future)
- **Status**: ⏳ Not set up yet
- **Purpose**: Live production workloads
- **Access**: Will need separate Hyperdrive instance
- **Requirements**:
  - Aurora Serverless v2 (recommended)
  - Multi-AZ deployment
  - Automated backups
  - Enhanced monitoring

## Current Testing Strategy

Since all environments currently point to the **dev** database:

### ✅ What We Can Test Now:
1. End-to-end job submission workflow
2. Worker coordination and job claiming
3. Hyperdrive query performance
4. Queue Consumer integration
5. Python worker client integration
6. Data quality issue tracking

### ⚠️ Important Notes:
- All testing will write to the **dev** database
- Use distinct job types or test prefixes to avoid confusion
- Monitor for production-like behavior but expect dev data
- Can safely test failures and error conditions

## Future Environment Setup

### When AWS Test/Production Accounts Are Ready:

#### Step 1: Create Production Database
```bash
# Deploy Aurora cluster in production account
cd infrastructure/aws
terraform workspace new production
terraform apply -var-file=production.tfvars
```

#### Step 2: Create Staging/Test Hyperdrive Instances
```bash
# Create test Hyperdrive
wrangler hyperdrive create lexara-aurora-test \
  --connection-string="postgres://hyperdrive_reader:PASSWORD@test-host:5432/graph_db"

# Create staging Hyperdrive
wrangler hyperdrive create lexara-aurora-staging \
  --connection-string="postgres://hyperdrive_reader:PASSWORD@staging-host:5432/graph_db"
```

#### Step 3: Update Hyperdrive Mapping
```bash
# Update prod Hyperdrive to point to actual prod database
wrangler hyperdrive update lexara-aurora-prod \
  --connection-string="postgres://hyperdrive_reader:PASSWORD@prod-host:5432/graph_db"
```

#### Step 4: Update wrangler.toml
```toml
# Production - uses actual prod database
[env.production.hyperdrive]
binding = "HYPERDRIVE"
id = "3b404e5336964e7d9ebd6581c62efa03"  # Will point to prod DB

# Staging - uses staging database
[env.staging.hyperdrive]
binding = "HYPERDRIVE"
id = "STAGING_HYPERDRIVE_ID"

# Development - uses dev database
[env.development.hyperdrive]
binding = "HYPERDRIVE"
id = "DEV_HYPERDRIVE_ID"
```

## Migration Path

### Phase 1: Current State (✅ Complete)
- [x] Dev database operational
- [x] Hyperdrive connected to dev DB
- [x] Coordinator Worker deployed
- [x] All endpoints tested

### Phase 2: Test Environment Setup (⏳ Pending AWS accounts)
- [ ] Create test AWS account/VPC
- [ ] Deploy test PostgreSQL instance
- [ ] Create test Hyperdrive instance
- [ ] Deploy workers to test environment
- [ ] Run integration tests

### Phase 3: Production Environment Setup (⏳ Pending AWS accounts)
- [ ] Create production AWS account/VPC
- [ ] Deploy production Aurora cluster (multi-AZ)
- [ ] Create production Hyperdrive instance
- [ ] Update prod workers to use prod Hyperdrive
- [ ] Set up monitoring and alerting
- [ ] Configure automated backups
- [ ] Run production validation tests

## Database Connection Details

### Current (Dev)
```
Host: 98.85.51.253
Port: 5432
Database: graph_db
Read-Only User: hyperdrive_reader
Read-Write User: graph_admin
```

### Future (Production) - Recommended
```
Type: Aurora Serverless v2 PostgreSQL
Engine: PostgreSQL 14+
Min Capacity: 0.5 ACU
Max Capacity: 4 ACU (adjust based on load)
Multi-AZ: Yes
Backup Retention: 7 days
Encryption: At rest and in transit
```

## Worker Deployment Strategy

### Current Approach
- Single deployment to "production" worker
- Uses dev database
- Suitable for development and testing

### Future Multi-Environment Approach
```bash
# Deploy to development
wrangler deploy --env development

# Deploy to staging
wrangler deploy --env staging

# Deploy to production
wrangler deploy --env production
```

## Monitoring Considerations

### Current (Dev)
- Cloudflare Worker logs via `wrangler tail`
- Manual database monitoring via psql
- No automated alerting

### Future (Production)
- CloudWatch dashboards (from Terraform)
- Automated alerting for:
  - High error rates
  - Slow queries (via Hyperdrive insights)
  - Database connection issues
  - Queue backlog
- Distributed tracing (optional)
- Performance monitoring

## Cost Implications

### Current (Dev)
- Single EC2 PostgreSQL instance
- Single Hyperdrive instance
- Single Worker deployment
- **Cost**: ~$6-10/month (Cloudflare only)

### Future (All Environments)
- 3x Aurora clusters (dev, test, prod)
- 3x Hyperdrive instances
- 3x Worker deployments
- **Estimated Cost**:
  - Cloudflare: ~$15-20/month
  - AWS (Aurora): ~$50-200/month (depending on usage)

## Security Notes

### Current
- ✅ Read-only Hyperdrive user
- ✅ Separate read/write users
- ⚠️ Dev database on public IP (acceptable for testing)

### Future Production Requirements
- [ ] Private VPC for Aurora
- [ ] VPN or bastion for admin access
- [ ] Secrets rotation via AWS Secrets Manager
- [ ] Network ACLs restricting Cloudflare IPs only
- [ ] Enhanced monitoring and audit logging
- [ ] DDoS protection via Cloudflare

## Recommendations

1. **Keep current dev setup** for testing the complete distributed loader system
2. **Test thoroughly** with real Iowa business data loads
3. **Document any issues** before setting up prod environments
4. **Plan Aurora migration** - consider migrating dev DB to Aurora Serverless for consistency
5. **Automate environment provisioning** with Terraform once tested

---

**Current Status**: Development environment fully operational and ready for end-to-end testing.
