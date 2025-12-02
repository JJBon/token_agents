# Quick Start Guide

Get up and running with the Crypto Token Intelligence Platform in 15 minutes.

## Prerequisites

- AWS Account
- Docker & Docker Compose installed
- AWS CLI configured
- 4GB RAM minimum
- 10GB disk space

## Step 1: Clone and Configure (2 minutes)

```bash
# Clone repository
git clone <repository-url>
cd token_agents

# Create environment file
cat > env/.env_dev << EOF
AWS_REGION=us-east-1
S3_NAMING_PREFIX=mycompany-crypto
COINGECKO_API_KEY=your_key_here
CRYPTONEWS_TOKEN=your_token_here
EOF
```

## Step 2: Deploy Infrastructure (10 minutes)

```bash
# Build and push containers
make push
make push-langgraph

# Deploy AWS resources
make terraform-apply

# Export infrastructure outputs
make terraform-env
```

This creates:
- S3 buckets for data storage
- Aurora PostgreSQL with pgvector
- Bedrock Knowledge Bases
- Lambda for data ingestion
- ECS for news processing

## Step 3: Load Initial Data (2 minutes)

```bash
# Trigger market data ingestion
aws lambda invoke \
  --function-name coingecko_snapshot_ingest \
  --payload '{}' \
  response.json

# Check result
cat response.json
```

## Step 4: Start Local Services (1 minute)

```bash
# Start Spark + dbt
make compose-up-spark-dbt

# In another terminal, start LangGraph service
make compose-run-langgraph
```

## Step 5: Test the System

### Test via curl

```bash
# Simple query
curl -X POST http://localhost:3000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is the current price of Bitcoin?",
    "session_id": "test-123"
  }'
```

### Test via Python

```python
import requests

response = requests.post(
    "http://localhost:3000/invoke",
    json={
        "message": "Show me Ethereum price trends for the last week",
        "session_id": "test-123"
    }
)

print(response.json()["response"])
```

### Test OpenAI-compatible endpoint

```bash
curl -X POST http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "crypto-agent",
    "messages": [
      {"role": "user", "content": "What are the top 5 cryptocurrencies by market cap?"}
    ],
    "user": "test-user"
  }'
```

## Common Queries to Try

```bash
# Market data
"What is Bitcoin's current price?"
"Show me Ethereum price trends for the last month"
"Which cryptocurrencies have the highest volume today?"

# News analysis
"What are the latest news about Bitcoin?"
"Summarize recent Ethereum developments"
"What tokens are mentioned most in recent news?"

# Research synthesis
"Generate a marketing brief for Bitcoin research"
"What research papers discuss DeFi?"
```

## Next Steps

### 1. Upload Research Papers

```bash
# Upload to research KB
RESEARCH_BUCKET=$(terraform -chdir=deployment output -raw research_kb_bucket)
aws s3 cp your-research.pdf s3://$RESEARCH_BUCKET/research/

# Trigger ingestion
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id $(terraform -chdir=deployment output -raw research_kb_id) \
  --data-source-id $(terraform -chdir=deployment output -raw research_kb_data_source_id)
```

### 2. Run News Ingestion

```bash
# Manual trigger
SFN_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?contains(name, 'news-ingest')].stateMachineArn" \
  --output text)

aws stepfunctions start-execution \
  --state-machine-arn $SFN_ARN \
  --name manual-$(date +%s)
```

### 3. Run dbt Transformations

```bash
# Run dbt models
make dbt-run

# Generate documentation
make dbt-docs

# View docs (opens browser)
cd dbt/coin_spark/target
python -m http.server 8080
# Open http://localhost:8080
```

### 4. Set Up Monitoring

```bash
# View Lambda logs
aws logs tail /aws/lambda/coingecko_snapshot_ingest --follow

# View ECS logs
aws logs tail /ecs/news-ingest --follow

# Check Terraform outputs
make print-tf-outputs
```

## Troubleshooting

### Issue: Lambda timeout

```bash
# Increase timeout
aws lambda update-function-configuration \
  --function-name coingecko_snapshot_ingest \
  --timeout 600
```

### Issue: Aurora connection refused

```bash
# Check cluster status
aws rds describe-db-clusters \
  --db-cluster-identifier $(terraform -chdir=deployment output -raw aurora_cluster_id)

# Wait for cluster to be available (can take 5-10 minutes)
```

### Issue: Docker out of memory

```bash
# Increase Docker memory limit to 4GB
# Docker Desktop → Settings → Resources → Memory

# Or reduce concurrent workers
export EXTRACT_CONCURRENCY=4
```

### Issue: API rate limits

```bash
# CoinGecko free tier: 10-50 calls/minute
# Wait 60 seconds between manual invocations

# Or upgrade to Pro tier
```

## Clean Up

To remove all resources:

```bash
# Stop local services
make compose-down-spark-dbt

# Destroy AWS resources
cd deployment
terraform destroy

# Clean local data
make clean
```

## What's Next?

- Read the [Architecture Guide](ARCHITECTURE.md) to understand the system
- Check the [Agent System Guide](AGENTS.md) to learn about agents
- Review the [API Reference](API.md) for integration
- See the [Development Guide](DEVELOPMENT.md) for contributing

## Getting Help

- Documentation: `docs/` directory
- Issues: GitHub Issues
- Examples: `tests/` directory
- Logs: CloudWatch Logs

## Example Workflows

### Daily Market Analysis

```python
# 1. Get latest market data
response = agent.query("Show me top 10 cryptocurrencies by market cap")

# 2. Analyze trends
response = agent.query("Which coins have increased more than 5% today?")

# 3. Get news context
response = agent.query("What news might explain these price movements?")
```

### Research-Driven Insights

```python
# 1. Upload research paper
aws s3 cp defi-research.pdf s3://$RESEARCH_BUCKET/research/

# 2. Wait for ingestion (5-10 minutes)

# 3. Query research
response = agent.query("Summarize the key findings from DeFi research papers")

# 4. Connect to news
response = agent.query("What recent news relates to these DeFi findings?")
```

### Custom Analytics

```python
# 1. Define custom metric in dbt
# dbt/coin_spark/models/marts/my_metric.sql

# 2. Run dbt
make dbt-run

# 3. Query via agent
response = agent.query("Show me my custom metric for Bitcoin")
```

## Performance Tips

1. **Use partitions**: Always filter by date in queries
2. **Cache results**: Store frequently accessed data
3. **Batch operations**: Process multiple items together
4. **Monitor costs**: Check AWS Cost Explorer regularly
5. **Scale Aurora**: Increase max_capacity for production

## Security Checklist

- [ ] Rotate API keys monthly
- [ ] Enable MFA on AWS account
- [ ] Use private subnets for ECS
- [ ] Enable CloudTrail logging
- [ ] Set up budget alerts
- [ ] Review IAM policies
- [ ] Enable S3 versioning
- [ ] Configure backup retention

## Success Metrics

After setup, you should see:
- ✅ Lambda runs daily without errors
- ✅ News ingestion completes in < 15 minutes
- ✅ Agent responses in < 10 seconds
- ✅ Data freshness < 24 hours
- ✅ Aurora scales to zero when idle
- ✅ Monthly AWS cost < $50 (dev environment)

Congratulations! You're now running a production-ready crypto intelligence platform. 🚀
