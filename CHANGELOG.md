# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation suite
  - Architecture guide
  - Agent system guide
  - Data pipeline guide
  - Deployment guide
  - Development guide
  - API reference
  - Quick start guide

### Changed
- Updated README with documentation links

### Fixed
- N/A

## [1.0.0] - 2025-01-15

### Added
- Multi-agent system with LangGraph
  - Conversation Agent for user interaction
  - Supervisor Agent for orchestration
  - Query Agent for data retrieval
  - News Agent for news processing
  - Market Agent for research synthesis
- Data pipeline infrastructure
  - Lambda function for CoinGecko market data ingestion
  - ECS Fargate task for news processing
  - dbt + Spark for data transformations
- AWS infrastructure via Terraform
  - Aurora PostgreSQL with pgvector
  - Bedrock Knowledge Bases (news + research)
  - S3 buckets for data storage
  - Glue catalog for metadata
  - EventBridge for scheduling
  - Step Functions for orchestration
- BentoML service with REST API
  - `/invoke` endpoint
  - `/v1/chat/completions` OpenAI-compatible endpoint
- MCP (Model Context Protocol) integration
  - dbt semantic layer access
  - Flexible tool loading
- Vector search capabilities
  - Semantic search across research papers
  - Hybrid search (vector + keyword)
  - News-to-research pairing
- Observability
  - Langfuse integration for LLM tracing
  - CloudWatch logs and metrics
  - Structured logging

### Infrastructure
- Terraform modules for reusable components
  - `aurora_pgvector`: PostgreSQL with vector extension
  - `bedrock_kb`: Knowledge Base setup
  - `cognito`: Authentication
  - `ecs_fargate_task`: Container tasks
  - `sfn_ecs_runner`: Step Functions orchestration
- Docker Compose for local development
- Makefile for build automation

### Documentation
- README with project overview
- Sample data for testing
- Environment configuration examples

## [0.1.0] - 2024-12-01

### Added
- Initial project structure
- Basic Lambda function for data ingestion
- S3 bucket setup
- Glue catalog configuration

---

## Version History

- **1.0.0** (2025-01-15): First production release with full agent system
- **0.1.0** (2024-12-01): Initial prototype

## Upgrade Guide

### From 0.1.0 to 1.0.0

This is a major release with breaking changes.

**Infrastructure Changes**:
1. Aurora PostgreSQL replaces previous vector storage
2. Bedrock Knowledge Bases added
3. ECS Fargate replaces previous news processing

**Migration Steps**:
```bash
# 1. Backup existing data
aws s3 sync s3://old-bucket s3://backup-bucket

# 2. Destroy old infrastructure
terraform destroy

# 3. Deploy new infrastructure
make terraform-apply

# 4. Migrate data
python scripts/migrate_data.py

# 5. Verify
make verify-deployment
```

**API Changes**:
- New `/invoke` endpoint replaces `/query`
- Added OpenAI-compatible `/v1/chat/completions`
- Session management via `session_id` parameter

**Configuration Changes**:
- New environment variables required (see `env/.env_dev.example`)
- Terraform variables updated (see `deployment/variables.tf`)

## Deprecation Notices

### Deprecated in 1.0.0
- None

### Removed in 1.0.0
- Legacy `/query` endpoint (replaced by `/invoke`)
- Direct S3 vector storage (replaced by Aurora pgvector)

## Security Updates

### 1.0.0
- Added Secrets Manager for API keys
- Implemented IAM least-privilege policies
- Enabled encryption at rest for all storage
- Added VPC security groups for network isolation

## Known Issues

### 1.0.0
- Aurora cold start can take 30-60 seconds
- News scraping may fail for sites with aggressive anti-bot measures
- LLM extraction accuracy varies by article quality
- MCP stdio mode has higher latency than HTTP mode

**Workarounds**:
- Keep Aurora warm with periodic queries
- Implement retry logic for scraping failures
- Adjust `EXTRACTOR_TEMPERATURE` for better extraction
- Use MCP HTTP mode for production

## Future Roadmap

### Planned for 1.1.0
- Streaming responses for better UX
- GraphQL API for flexible queries
- Real-time data ingestion via Kinesis
- Advanced caching layer with Redis
- Multi-tenancy support

### Planned for 2.0.0
- ML models for price prediction
- Custom embeddings fine-tuned on crypto data
- Advanced analytics dashboard
- Webhook support for event notifications
- Mobile SDK (iOS/Android)

## Contributors

See [CONTRIBUTORS.md](CONTRIBUTORS.md) for the list of contributors.

## License

[Add your license here]
