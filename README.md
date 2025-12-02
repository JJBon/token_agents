# Crypto Token Intelligence Platform

A multi-agent AI system for cryptocurrency market intelligence that combines real-time data ingestion, semantic analysis, and conversational AI to provide insights on crypto markets, news, and research.

## Overview

This platform ingests cryptocurrency market data and news, processes it through specialized AI agents, and provides intelligent query capabilities through a conversational interface. It leverages AWS Bedrock, LangGraph, and a modern data stack to deliver actionable insights.

## Key Features

- **Automated Data Collection**: Daily snapshots of crypto market data from CoinGecko API
- **News Intelligence**: Real-time crypto news ingestion with LLM-powered token extraction
- **Multi-Agent System**: Specialized agents for queries, news processing, and market analysis
- **Semantic Search**: Vector-based search across research papers and news articles
- **Analytics Layer**: dbt + Spark for data transformation and metrics
- **Conversational Interface**: Natural language queries with context-aware responses

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Interface                            │
│                    (Conversation Agent)                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                      Supervisor Agent                            │
│              (Orchestration & Validation)                        │
└─────┬──────────────────────┬──────────────────────┬─────────────┘
      │                      │                      │
┌─────▼──────┐      ┌────────▼────────┐    ┌───────▼──────────┐
│   Query    │      │      News       │    │     Market       │
│   Agent    │      │     Agent       │    │     Agent        │
│  (MCP/dbt) │      │  (Extraction)   │    │  (Synthesis)     │
└─────┬──────┘      └────────┬────────┘    └───────┬──────────┘
      │                      │                      │
┌─────▼──────────────────────▼──────────────────────▼─────────────┐
│                        Data Layer                                │
│  ┌──────────┐  ┌──────────────┐  ┌─────────────────────────┐   │
│  │  Aurora  │  │  S3 + Glue   │  │  Bedrock Knowledge Base │   │
│  │ pgvector │  │  + Athena    │  │    (Research + News)    │   │
│  └──────────┘  └──────────────┘  └─────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- AWS Account with appropriate permissions
- Docker & Docker Compose
- Terraform >= 1.6
- Python 3.11+
- Make

### Environment Setup

1. Clone the repository and set up environment variables:

```bash
# Copy and configure environment files
cp .env.example env/.env_dev
# Edit env/.env_dev with your API keys and configuration
```

2. Deploy infrastructure:

```bash
# Initialize and deploy AWS resources
make terraform-apply

# Export infrastructure outputs to env file
make terraform-env
```

3. Build and push container images:

```bash
# Build and push Lambda container
make push

# Build and push LangGraph container
make push-langgraph
```

4. Start local development environment:

```bash
# Start Spark + dbt environment
make compose-up-spark-dbt

# Run the LangGraph service
make compose-run-langgraph
```

## Project Structure

```
.
├── deployment/              # Terraform infrastructure as code
│   ├── modules/            # Reusable Terraform modules
│   │   ├── aurora_pgvector/    # Aurora PostgreSQL with pgvector
│   │   ├── bedrock_kb/         # Bedrock Knowledge Base setup
│   │   ├── cognito/            # Authentication
│   │   ├── ecs_fargate_task/   # ECS task definitions
│   │   └── sfn_ecs_runner/     # Step Functions orchestration
│   ├── main.tf             # Main infrastructure definition
│   └── lambda_ingest_container/  # CoinGecko data ingestion Lambda
│
├── docker/                  # Container configurations
│   ├── langgraph/          # Multi-agent system
│   │   └── app/
│   │       ├── agents/     # Agent implementations
│   │       ├── tools/      # Agent tools and utilities
│   │       ├── prompts/    # System prompts
│   │       └── vectors/    # Vector store clients
│   ├── spark/              # Spark cluster for analytics
│   └── openwebui/          # Optional UI frontend
│
├── dbt/                     # Data transformation layer
│   └── coin_spark/         # dbt project for crypto metrics
│       ├── models/         # Data models
│       └── macros/         # Custom SQL macros
│
├── sample_data/            # Sample datasets for testing
├── tests/                  # Test suites
└── Makefile               # Build and deployment automation
```

## Documentation

📚 **Getting Started**
- [Quick Start Guide](docs/QUICKSTART.md) - Get running in 15 minutes
- [Deployment Guide](docs/DEPLOYMENT.md) - Production deployment and configuration

🏗️ **Architecture**
- [Architecture Overview](docs/ARCHITECTURE.md) - System design and components
- [Agent System](docs/AGENTS.md) - Multi-agent architecture and workflows
- [Data Pipeline](docs/DATA_PIPELINE.md) - Data ingestion and processing

💻 **Development**
- [Development Guide](docs/DEVELOPMENT.md) - Local setup and best practices
- [API Reference](docs/API.md) - Service endpoints and usage examples

## Common Tasks

### Data Management

```bash
# Download latest market data
make download

# Run dbt transformations
make dbt-run

# Generate dbt documentation
make dbt-docs

# Reset all data (use with caution)
make reset-data CONFIRM=yes
```

### Infrastructure

```bash
# View Terraform outputs
make print-tf-outputs

# Update Lambda function
make update-lambda

# Clean up resources
make clean
```

### Development

```bash
# Run tests
pytest tests/

# Access Spark shell
make compose-run-spark-dbt

# View logs
docker-compose -f docker/spark/docker-compose.yml logs -f
```

## Technology Stack

**AI/ML**: AWS Bedrock (Claude), LangGraph, LangChain, Langfuse  
**Data Storage**: Aurora PostgreSQL (pgvector), S3, Iceberg  
**Data Processing**: Apache Spark, dbt, AWS Glue, Athena  
**Compute**: AWS Lambda, ECS Fargate  
**Infrastructure**: Terraform, Docker, Docker Compose  
**Orchestration**: AWS Step Functions, EventBridge  

## Configuration

Key environment variables (see `env/.env_dev.example`):

- `AWS_REGION` - AWS region for resources
- `COINGECKO_API_KEY` - CoinGecko API key
- `CRYPTONEWS_TOKEN` - CryptoNews API token
- `NEWS_KB_ID` - Bedrock Knowledge Base ID for news
- `RESEARCH_KB_ID` - Bedrock Knowledge Base ID for research
- `AURORA_CLUSTER_ARN` - Aurora cluster ARN
- `LITELLM_BASE_URL` - LiteLLM proxy endpoint (optional)

## Contributing

1. Create a feature branch
2. Make your changes
3. Add tests
4. Update documentation
5. Submit a pull request

## License

[Add your license here]

## Support

For issues and questions, please open a GitHub issue or contact the development team.
