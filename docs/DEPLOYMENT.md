# Deployment Guide

## Prerequisites

### Required Tools

- **AWS CLI** v2.x
  ```bash
  aws --version  # Should be 2.x
  aws configure  # Set up credentials
  ```

- **Terraform** >= 1.6
  ```bash
  terraform version
  ```

- **Docker** & **Docker Compose**
  ```bash
  docker --version
  docker-compose --version
  ```

- **Make**
  ```bash
  make --version
  ```

- **Python** 3.11+
  ```bash
  python --version
  ```

### AWS Permissions

Required IAM permissions for deployment:
- EC2: VPC, Subnets, Security Groups
- S3: Bucket creation and management
- Lambda: Function creation and updates
- ECR: Repository and image management
- ECS: Cluster, task definition, service
- RDS: Aurora cluster creation
- Glue: Database and table management
- Bedrock: Knowledge Base creation
- Secrets Manager: Secret creation
- IAM: Role and policy management
- EventBridge: Rule creation
- Step Functions: State machine creation
- CloudWatch: Logs and metrics

### API Keys

Obtain and store these API keys:
1. **CoinGecko API Key**: https://www.coingecko.com/en/api
2. **CryptoNews API Token**: https://cryptonews-api.com/

## Initial Setup

### 1. Clone and Configure

```bash
# Clone repository
git clone <repository-url>
cd token_agents

# Create environment file
cp env/.env_dev.example env/.env_dev
```

### 2. Configure Environment

Edit `env/.env_dev`:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012

# S3 Naming (must be globally unique)
S3_NAMING_PREFIX=mycompany-crypto

# API Keys (will be stored in Secrets Manager)
COINGECKO_API_KEY=your_coingecko_key
CRYPTONEWS_TOKEN=your_cryptonews_token

# LLM Configuration
LLM_BACKEND=litellm
LITELLM_MODEL_NAME=bedrock-claude-haiku
LITELLM_BASE_URL=http://litellm:4000
LITELLM_API_KEY=sk-noop

# Agent Configuration
MAX_ARTICLES=50
TIMEOUT_S=15
EXTRACTOR_TEMPERATURE=0.3
KB_INGEST_MODE=direct
WAIT_FOR_INGEST=true

# Monitoring
LANGFUSE_PUBLIC_KEY=your_langfuse_key
LANGFUSE_SECRET_KEY=your_langfuse_secret
LANGFUSE_HOST=https://cloud.langfuse.com
```

### 3. Store Secrets in AWS

```bash
# CoinGecko API Key
aws secretsmanager create-secret \
  --name coingecko/api_key \
  --secret-string '{"api_key":"your_coingecko_key"}'

# CryptoNews Token
aws secretsmanager create-secret \
  --name cryptonews/api_token \
  --secret-string 'your_cryptonews_token'
```

## Infrastructure Deployment

### Step 1: Build Container Images

```bash
# Build and push Lambda container (CoinGecko ingestion)
make push

# Build and push LangGraph container (News agent)
make push-langgraph
```

This will:
1. Build Docker images for linux/amd64
2. Tag images with latest
3. Push to ECR repositories

### Step 2: Deploy Infrastructure

```bash
# Initialize Terraform
make terraform-init

# Review planned changes
make terraform-plan

# Deploy all resources
make terraform-apply
```

This creates:
- S3 buckets (data, news, research, athena results)
- Aurora PostgreSQL cluster with pgvector
- Bedrock Knowledge Bases (news + research)
- Lambda function for CoinGecko ingestion
- ECS cluster and task definition for news agent
- Step Functions for orchestration
- Glue databases and tables
- IAM roles and policies
- EventBridge rules for scheduling
- Cognito user pool (optional)

**Deployment time**: ~15-20 minutes

### Step 3: Export Infrastructure Outputs

```bash
# Export Terraform outputs to env file
make terraform-env

# This creates env/.env_infra with:
# - AWS_REGION
# - NEWS_KB_ID, NEWS_KB_DS_ID, NEWS_KB_BUCKET, NEWS_KB_PREFIX
# - RESEARCH_KB_ID, RESEARCH_KB_DS_ID, RESEARCH_KB_BUCKET
# - AURORA_CLUSTER_ARN, AURORA_SECRET_ARN, AURORA_DB_NAME
# - RESEARCH_TABLE, NEWS_TABLE
```

### Step 4: Verify Deployment

```bash
# Check Terraform outputs
make print-tf-outputs

# Verify Lambda function
aws lambda get-function --function-name coingecko_snapshot_ingest

# Verify Aurora cluster
aws rds describe-db-clusters --db-cluster-identifier <cluster-id>

# Verify Knowledge Bases
aws bedrock-agent list-knowledge-bases

# Verify ECS task definition
aws ecs describe-task-definition --task-definition news-ingest
```

## Component Configuration

### Lambda Function (Market Data)

**Trigger**: EventBridge rule runs daily at 23:55 UTC

**Manual invocation**:
```bash
aws lambda invoke \
  --function-name coingecko_snapshot_ingest \
  --payload '{}' \
  response.json

cat response.json
```

**Update function code**:
```bash
# After making changes to deployment/lambda_ingest_container/
make update-lambda
```

**View logs**:
```bash
aws logs tail /aws/lambda/coingecko_snapshot_ingest --follow
```

### ECS Task (News Agent)

**Trigger**: Step Functions runs daily at 06:00 America/Bogota

**Manual execution**:
```bash
# Get Step Functions ARN
SFN_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?contains(name, 'news-ingest')].stateMachineArn" \
  --output text)

# Start execution
aws stepfunctions start-execution \
  --state-machine-arn $SFN_ARN \
  --name manual-$(date +%s)
```

**View execution logs**:
```bash
# Get execution ARN from start-execution output
aws stepfunctions describe-execution \
  --execution-arn <execution-arn>

# View ECS task logs
aws logs tail /ecs/news-ingest --follow
```

**Update task**:
```bash
# After making changes to docker/langgraph/
make push-langgraph

# ECS will automatically use new image on next run
# Or force new deployment:
aws ecs update-service \
  --cluster news-ingest-cluster \
  --service news-ingest-service \
  --force-new-deployment
```

### Aurora PostgreSQL

**Connection**:
```bash
# Get cluster endpoint
CLUSTER_ENDPOINT=$(terraform -chdir=deployment output -raw aurora_cluster_endpoint)

# Get secret ARN
SECRET_ARN=$(terraform -chdir=deployment output -raw aurora_secret_arn)

# Get password from Secrets Manager
PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id $SECRET_ARN \
  --query SecretString --output text | jq -r .password)

# Connect via psql
psql -h $CLUSTER_ENDPOINT -U admin -d kbdb
```

**Data API** (serverless queries):
```bash
# Execute SQL via Data API
aws rds-data execute-statement \
  --resource-arn $(terraform -chdir=deployment output -raw aurora_cluster_arn) \
  --secret-arn $(terraform -chdir=deployment output -raw aurora_secret_arn) \
  --database kbdb \
  --sql "SELECT COUNT(*) FROM public.research_kb;"
```

**Scaling**:
```terraform
# Edit deployment/modules/aurora_pgvector/main.tf
resource "aws_rds_cluster" "this" {
  serverlessv2_scaling_configuration {
    min_capacity = 0.5  # Minimum ACU
    max_capacity = 16   # Maximum ACU
  }
}
```

### Bedrock Knowledge Bases

**Sync data sources**:
```bash
# News KB
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id $(terraform -chdir=deployment output -raw news_kb_id) \
  --data-source-id $(terraform -chdir=deployment output -raw news_kb_data_source_id)

# Research KB
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id $(terraform -chdir=deployment output -raw research_kb_id) \
  --data-source-id $(terraform -chdir=deployment output -raw research_kb_data_source_id)
```

**Check ingestion status**:
```bash
aws bedrock-agent list-ingestion-jobs \
  --knowledge-base-id <kb-id> \
  --data-source-id <ds-id>
```

**Query KB**:
```bash
aws bedrock-agent-runtime retrieve \
  --knowledge-base-id <kb-id> \
  --retrieval-query text="Bitcoin price analysis"
```

## Data Management

### Initial Data Load

```bash
# Load sample data (optional, for testing)
cd sample_data
python load_sample_data.py

# Or manually trigger Lambda for real data
aws lambda invoke \
  --function-name coingecko_snapshot_ingest \
  --payload '{}' \
  response.json
```

### Upload Research Papers

```bash
# Upload to research KB bucket
RESEARCH_BUCKET=$(terraform -chdir=deployment output -raw research_kb_bucket)

aws s3 cp research_paper.pdf s3://$RESEARCH_BUCKET/research/

# Trigger ingestion
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id $(terraform -chdir=deployment output -raw research_kb_id) \
  --data-source-id $(terraform -chdir=deployment output -raw research_kb_data_source_id)
```

### Run dbt Transformations

```bash
# Start Spark environment
make compose-up-spark-dbt

# Run dbt
make dbt-run

# Generate documentation
make dbt-docs
```

### Data Cleanup

```bash
# Clean Iceberg news table
make clean-iceberg-news CONFIRM=yes

# Clean Aurora research table
make clean-aurora-research CONFIRM=yes

# Clean Aurora news table (if exists)
make clean-aurora-news CONFIRM=yes

# Clean S3 KB data
make clean-kb-s3 CONFIRM=yes

# Or clean specific date partition
make clean-kb-s3-dt DT=2025-01-15 CONFIRM=yes

# Reset everything (use with caution!)
make reset-data CONFIRM=yes
```

## Monitoring & Observability

### CloudWatch Dashboards

Create custom dashboard:
```bash
aws cloudwatch put-dashboard \
  --dashboard-name crypto-intelligence \
  --dashboard-body file://cloudwatch-dashboard.json
```

### CloudWatch Alarms

```bash
# Lambda failure alarm
aws cloudwatch put-metric-alarm \
  --alarm-name lambda-failures \
  --alarm-description "Alert on Lambda failures" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=FunctionName,Value=coingecko_snapshot_ingest

# Aurora storage alarm
aws cloudwatch put-metric-alarm \
  --alarm-name aurora-storage \
  --alarm-description "Alert on high Aurora storage" \
  --metric-name VolumeBytesUsed \
  --namespace AWS/RDS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 10737418240 \
  --comparison-operator GreaterThanThreshold
```

### Langfuse Setup

1. Create account at https://cloud.langfuse.com
2. Create project
3. Get API keys
4. Add to `env/.env_dev`:
   ```bash
   LANGFUSE_PUBLIC_KEY=pk-...
   LANGFUSE_SECRET_KEY=sk-...
   LANGFUSE_HOST=https://cloud.langfuse.com
   ```

### Log Aggregation

```bash
# View all logs
aws logs tail --follow \
  /aws/lambda/coingecko_snapshot_ingest \
  /ecs/news-ingest

# Filter logs
aws logs filter-log-events \
  --log-group-name /aws/lambda/coingecko_snapshot_ingest \
  --filter-pattern "ERROR"

# Export logs to S3
aws logs create-export-task \
  --log-group-name /aws/lambda/coingecko_snapshot_ingest \
  --from $(date -d '7 days ago' +%s)000 \
  --to $(date +%s)000 \
  --destination logs-bucket \
  --destination-prefix lambda-logs/
```

## Scaling & Performance

### Horizontal Scaling

**Lambda**:
- Automatic concurrency scaling
- Reserved concurrency (optional):
  ```bash
  aws lambda put-function-concurrency \
    --function-name coingecko_snapshot_ingest \
    --reserved-concurrent-executions 10
  ```

**ECS**:
- Adjust task count in Step Functions
- Or use ECS Service with auto-scaling:
  ```terraform
  resource "aws_appautoscaling_target" "ecs" {
    max_capacity       = 10
    min_capacity       = 1
    resource_id        = "service/${cluster}/${service}"
    scalable_dimension = "ecs:service:DesiredCount"
    service_namespace  = "ecs"
  }
  ```

**Aurora**:
- Serverless v2 auto-scales based on load
- Adjust max_capacity for higher throughput:
  ```terraform
  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 32  # Increase for production
  }
  ```

### Vertical Scaling

**Lambda**:
```terraform
resource "aws_lambda_function" "ingest_snapshot" {
  memory_size = 1024  # Increase from 512
  timeout     = 600   # Increase from 360
}
```

**ECS**:
```terraform
resource "aws_ecs_task_definition" "news_ingest" {
  cpu    = "1024"  # Increase from 512
  memory = "2048"  # Increase from 1024
}
```

### Caching

**Athena**:
- Enable query result reuse (24 hours)
- Use workgroup settings

**Aurora**:
- Increase shared_buffers
- Enable query plan cache

**Application**:
- Add Redis for frequently accessed data
- Cache dbt metric definitions
- Cache embeddings for common queries

## Security Hardening

### Network Security

**VPC Configuration**:
```terraform
# Use private subnets for ECS tasks
resource "aws_ecs_service" "news_ingest" {
  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }
}

# Security group rules
resource "aws_security_group_rule" "ecs_egress" {
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.ecs_tasks.id
}
```

**Aurora Access**:
```terraform
# Restrict to specific security groups
resource "aws_security_group_rule" "aurora_ingress" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.ecs_tasks.id
  security_group_id        = aws_security_group.aurora.id
}
```

### IAM Policies

**Least Privilege**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::specific-bucket/specific-prefix/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:*:*:model/anthropic.claude-*"
    }
  ]
}
```

**Service Control Policies**:
- Restrict regions
- Deny root user actions
- Require MFA for sensitive operations

### Secrets Management

**Rotation**:
```bash
# Enable automatic rotation
aws secretsmanager rotate-secret \
  --secret-id coingecko/api_key \
  --rotation-lambda-arn <rotation-function-arn> \
  --rotation-rules AutomaticallyAfterDays=30
```

**Access Logging**:
```bash
# Enable CloudTrail for Secrets Manager
aws cloudtrail create-trail \
  --name secrets-audit \
  --s3-bucket-name audit-logs-bucket
```

### Encryption

**S3**:
```terraform
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.coingecko_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3.arn
    }
  }
}
```

**Aurora**:
```terraform
resource "aws_rds_cluster" "this" {
  storage_encrypted = true
  kms_key_id        = aws_kms_key.rds.arn
}
```

## Disaster Recovery

### Backup Strategy

**Aurora**:
- Automated backups: 7 days retention
- Manual snapshots before major changes
- Cross-region replication (optional)

**S3**:
- Versioning enabled
- Cross-region replication (optional)
- Lifecycle policies for cost optimization

### Recovery Procedures

**1. Aurora Failure**:
```bash
# Restore from snapshot
aws rds restore-db-cluster-from-snapshot \
  --db-cluster-identifier new-cluster \
  --snapshot-identifier <snapshot-id> \
  --engine aurora-postgresql

# Update Terraform state
terraform import aws_rds_cluster.this new-cluster
```

**2. S3 Data Loss**:
```bash
# Restore from version
aws s3api get-object \
  --bucket bucket-name \
  --key file-key \
  --version-id <version-id> \
  restored-file

# Or restore entire bucket
aws s3 sync s3://backup-bucket s3://primary-bucket
```

**3. Complete Region Failure**:
1. Failover to DR region (if configured)
2. Restore Aurora from cross-region snapshot
3. Restore S3 from cross-region replication
4. Update DNS/endpoints
5. Verify data integrity

### Testing DR

```bash
# Quarterly DR drill
1. Create test environment in DR region
2. Restore latest backups
3. Run validation queries
4. Measure RTO/RPO
5. Document issues
6. Update runbooks
```

## Cost Optimization

### Resource Tagging

```terraform
locals {
  common_tags = {
    Project     = "CryptoIntelligence"
    Environment = var.environment
    ManagedBy   = "Terraform"
    CostCenter  = "DataEngineering"
  }
}

resource "aws_s3_bucket" "data" {
  tags = local.common_tags
}
```

### Cost Monitoring

```bash
# Enable Cost Explorer
aws ce get-cost-and-usage \
  --time-period Start=2025-01-01,End=2025-01-31 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=TAG,Key=Project

# Set budget alerts
aws budgets create-budget \
  --account-id 123456789012 \
  --budget file://budget.json \
  --notifications-with-subscribers file://notifications.json
```

### Optimization Tips

1. **Aurora**: Scale to zero during off-hours
2. **S3**: Use Intelligent-Tiering for infrequent access
3. **Lambda**: Right-size memory allocation
4. **Athena**: Use partitions and compression
5. **ECS**: Use Spot instances for non-critical tasks

## Troubleshooting

### Common Issues

**1. Terraform Apply Fails**
```bash
# Check AWS credentials
aws sts get-caller-identity

# Check Terraform state
terraform show

# Force unlock if stuck
terraform force-unlock <lock-id>
```

**2. Lambda Timeout**
```bash
# Increase timeout
aws lambda update-function-configuration \
  --function-name coingecko_snapshot_ingest \
  --timeout 600

# Check logs for bottlenecks
aws logs tail /aws/lambda/coingecko_snapshot_ingest --follow
```

**3. ECS Task Fails to Start**
```bash
# Check task definition
aws ecs describe-task-definition --task-definition news-ingest

# Check stopped tasks
aws ecs describe-tasks \
  --cluster news-ingest-cluster \
  --tasks <task-arn>

# Common causes:
# - Image not found in ECR
# - Insufficient memory/CPU
# - IAM permissions missing
# - Environment variables incorrect
```

**4. Aurora Connection Refused**
```bash
# Check cluster status
aws rds describe-db-clusters --db-cluster-identifier <cluster-id>

# Check security groups
aws ec2 describe-security-groups --group-ids <sg-id>

# Test connectivity
nc -zv <cluster-endpoint> 5432
```

### Support Resources

- AWS Support: https://console.aws.amazon.com/support
- Terraform Registry: https://registry.terraform.io/
- LangGraph Docs: https://langchain-ai.github.io/langgraph/
- Project Issues: [GitHub Issues URL]

## Rollback Procedures

### Infrastructure Rollback

```bash
# Revert to previous Terraform state
terraform state pull > current-state.json
terraform state push previous-state.json

# Or destroy and recreate
terraform destroy -target=aws_ecs_service.news_ingest
terraform apply -target=aws_ecs_service.news_ingest
```

### Application Rollback

```bash
# Lambda: Update to previous image
aws lambda update-function-code \
  --function-name coingecko_snapshot_ingest \
  --image-uri <previous-image-uri>

# ECS: Revert task definition
aws ecs update-service \
  --cluster news-ingest-cluster \
  --service news-ingest-service \
  --task-definition news-ingest:42  # Previous revision
```

## Maintenance Windows

### Planned Maintenance

**Schedule**: First Sunday of each month, 02:00-06:00 UTC

**Checklist**:
1. Notify users 48 hours in advance
2. Create backups of all data
3. Disable EventBridge rules (pause ingestion)
4. Apply updates
5. Run validation tests
6. Re-enable EventBridge rules
7. Monitor for 24 hours

### Emergency Maintenance

**Procedure**:
1. Assess impact and urgency
2. Create incident ticket
3. Notify stakeholders
4. Implement fix
5. Validate resolution
6. Post-mortem within 48 hours
