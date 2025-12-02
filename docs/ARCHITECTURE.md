# Architecture Guide

## System Overview

The Crypto Token Intelligence Platform is a cloud-native, event-driven system that combines data engineering, machine learning, and conversational AI to provide cryptocurrency market intelligence.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          External Sources                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  CoinGecko   │  │ CryptoNews   │  │  Research Papers (S3)    │  │
│  │     API      │  │     API      │  │                          │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────────┘  │
└─────────┼──────────────────┼───────────────────────┼──────────────────┘
          │                  │                       │
          │                  │                       │
┌─────────▼──────────────────▼───────────────────────▼──────────────────┐
│                        Ingestion Layer                                 │
│  ┌──────────────┐  ┌──────────────────────────────────────────────┐  │
│  │   Lambda     │  │         ECS Fargate Task                     │  │
│  │  (Scheduled) │  │      (News Agent Pipeline)                   │  │
│  │              │  │  - Fetch news                                │  │
│  │  - Fetch     │  │  - Scrape articles                           │  │
│  │    market    │  │  - Extract tokens (LLM)                      │  │
│  │    data      │  │  - Store in vector DB                        │  │
│  │  - Store in  │  │  - Persist to Iceberg                        │  │
│  │    S3        │  │                                              │  │
│  └──────┬───────┘  └──────────┬───────────────────────────────────┘  │
└─────────┼──────────────────────┼──────────────────────────────────────┘
          │                      │
          │                      │
┌─────────▼──────────────────────▼──────────────────────────────────────┐
│                          Storage Layer                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────────┐ │
│  │     S3       │  │  AWS Glue    │  │   Aurora PostgreSQL         │ │
│  │              │  │              │  │   (pgvector extension)      │ │
│  │ - Raw data   │  │ - Catalog    │  │                             │ │
│  │ - Parquet    │  │ - Iceberg    │  │ - Research vectors          │ │
│  │ - News docs  │  │   tables     │  │ - News vectors              │ │
│  │              │  │              │  │ - Metadata                  │ │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬──────────────────┘ │
└─────────┼──────────────────┼───────────────────────┼───────────────────┘
          │                  │                       │
          │                  │                       │
┌─────────▼──────────────────▼───────────────────────▼───────────────────┐
│                      Processing Layer                                   │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    Apache Spark + dbt                            │  │
│  │  - Data transformations                                          │  │
│  │  - Metric calculations                                           │  │
│  │  - Semantic layer (via MCP)                                      │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │              AWS Bedrock Knowledge Bases                         │  │
│  │  - Research KB (papers, reports)                                 │  │
│  │  - News KB (articles, mentions)                                  │  │
│  │  - Titan embeddings                                              │  │
│  │  - Hybrid search (vector + keyword)                              │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
          │
          │
┌─────────▼───────────────────────────────────────────────────────────────┐
│                      Agent Layer (LangGraph)                             │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                     Supervisor Agent                             │   │
│  │  - Routes requests to specialized agents                         │   │
│  │  - Validates response completeness                               │   │
│  │  - Manages retry logic                                           │   │
│  └────┬─────────────────────┬─────────────────────┬─────────────────┘   │
│       │                     │                     │                     │
│  ┌────▼──────────┐  ┌───────▼────────┐  ┌────────▼──────────┐         │
│  │ Query Agent   │  │  News Agent    │  │  Market Agent     │         │
│  │               │  │                │  │                   │         │
│  │ - MCP tools   │  │ - Ingestion    │  │ - Research sync   │         │
│  │ - dbt metrics │  │ - Extraction   │  │ - News pairing    │         │
│  │ - SQL queries │  │ - Deduplication│  │ - Brief synthesis │         │
│  └───────────────┘  └────────────────┘  └───────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
          │
          │
┌─────────▼───────────────────────────────────────────────────────────────┐
│                      Application Layer                                   │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                  Conversation Agent                              │   │
│  │  - User interaction                                              │   │
│  │  - Context management                                            │   │
│  │  - Feedback loop                                                 │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    BentoML Service                               │   │
│  │  - /invoke endpoint                                              │   │
│  │  - /v1/chat/completions (OpenAI compatible)                      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Data Ingestion

#### CoinGecko Lambda Function
- **Trigger**: EventBridge (daily at 23:55 UTC)
- **Function**: Fetches market data for top cryptocurrencies
- **Output**: Parquet files in S3 (`s3://bucket/raw/YYYY-MM-DD-snapshot.parquet`)
- **Schema**: price, market cap, volume, supply, price changes, ATH/ATL

#### News Agent Pipeline (ECS Fargate)
- **Trigger**: Step Functions (daily at 06:00 America/Bogota)
- **Workflow**:
  1. Fetch news from CryptoNews API
  2. Deduplicate against existing records
  3. Scrape full article text
  4. Extract token mentions using LLM (Claude)
  5. Store in Aurora pgvector
  6. Persist to Iceberg tables
  7. Sync to Bedrock Knowledge Base

### 2. Storage Layer

#### S3 Buckets
- `{prefix}-coingecko-data-pipeline`: Raw market data
- `{prefix}-news-kb`: News articles for Bedrock KB
- `{prefix}-research-kb`: Research papers for Bedrock KB
- `{prefix}-coingecko-athena-results`: Athena query results

#### Aurora PostgreSQL with pgvector
- **Purpose**: Vector storage for semantic search
- **Configuration**: Serverless v2 (0-4 ACU)
- **Tables**:
  - `public.research_kb`: Research paper chunks + embeddings
  - `public.news_kb`: News article chunks + embeddings
- **Indexes**: HNSW for vector similarity, GIN for full-text search

#### AWS Glue Catalog
- **Databases**:
  - `coingecko`: Raw market data
  - `news_agent`: Processed news with Iceberg tables
- **Tables**:
  - `coingecko_raw`: Parquet files from Lambda
  - `cryptoapi_news`: Iceberg table with news + token mentions

### 3. Processing Layer

#### Apache Spark
- **Deployment**: Docker Compose (local), ECS (production)
- **Purpose**: Large-scale data transformations
- **Integration**: Thrift server for SQL access

#### dbt (Data Build Tool)
- **Project**: `coin_spark`
- **Purpose**: Data modeling and metric definitions
- **Output**: Iceberg tables in S3
- **Access**: Via MCP (Model Context Protocol) for agent queries

#### Bedrock Knowledge Bases
- **Research KB**: 
  - Source: S3 (`research/` prefix)
  - Vector store: Aurora pgvector
  - Use case: Long-form research analysis
  
- **News KB**:
  - Source: S3 (`news/` prefix) or direct API
  - Vector store: Aurora pgvector
  - Use case: Recent news and token mentions

### 4. Agent System (LangGraph)

#### Supervisor Agent
- **Role**: Orchestrator and validator
- **LLM**: Claude Haiku (fast, cost-effective)
- **Logic**:
  - Analyzes user request
  - Routes to appropriate specialized agent
  - Validates response has data + insight
  - Retries if incomplete (max 2 attempts)
  - Detects stagnation via signature hashing

#### Query Agent
- **Role**: Data retrieval and analysis
- **Tools**: MCP-based dbt semantic layer
- **Workflow**:
  1. `fetch_metrics`: List available metrics
  2. `create_query`: Generate SQL from natural language
  3. `fetch_query_result`: Execute and return data
- **LLM**: Claude Haiku or Sonnet (configurable)

#### News Agent
- **Role**: News ingestion and processing
- **Tools**: 
  - API fetching
  - Web scraping
  - LLM extraction (token mentions)
  - Vector storage
  - Iceberg persistence
- **Modes**: S3 sync or direct Bedrock ingest

#### Market Agent
- **Role**: Research-news synthesis
- **Workflow**:
  1. Discover relevant research papers
  2. Retrieve research chunks
  3. Fetch recent news from Athena
  4. Generate semantic queries
  5. Query news vectors
  6. Pair news with research papers
  7. Synthesize marketing briefs per paper
- **Output**: Per-document briefs with matched news

#### Conversation Agent
- **Role**: User-facing interface
- **Features**:
  - Persistent conversation history
  - Feedback mechanism (`feedback: <instruction>`)
  - Context-aware responses
  - Memory via LangGraph checkpointer

### 5. Infrastructure (Terraform)

#### Modules
- `aurora_pgvector`: Serverless PostgreSQL with vector extension
- `bedrock_kb`: Knowledge Base with data source configuration
- `cognito`: User authentication and authorization
- `ecs_fargate_task`: Container task definitions
- `sfn_ecs_runner`: Step Functions for scheduled ECS tasks

#### Key Resources
- Lambda function with container image
- ECR repositories for images
- VPC and networking (uses default VPC)
- IAM roles and policies
- Secrets Manager for API keys
- EventBridge rules for scheduling
- SNS topics for notifications

## Data Flow

### Market Data Flow
```
CoinGecko API → Lambda → S3 (Parquet) → Glue Catalog → Athena
                                                           ↓
                                                    Spark + dbt
                                                           ↓
                                                    Iceberg Tables
                                                           ↓
                                                    Query Agent (MCP)
```

### News Data Flow
```
CryptoNews API → ECS Task → Scraping → LLM Extract
                                           ↓
                    ┌──────────────────────┴──────────────────────┐
                    ↓                                              ↓
            Aurora pgvector                                 Iceberg Table
         (Bedrock KB backend)                            (Athena queryable)
                    ↓                                              ↓
            Semantic Search                                  SQL Analysis
```

### Query Flow
```
User → Conversation Agent → Supervisor Agent → Query Agent
                                                     ↓
                                              MCP Tools (dbt)
                                                     ↓
                                              Spark Thrift Server
                                                     ↓
                                              Iceberg Tables
                                                     ↓
                                              Results → User
```

## Design Decisions

### Why LangGraph?
- **State management**: Built-in checkpointing for conversation history
- **Modularity**: Each agent is an independent graph
- **Observability**: Native integration with Langfuse
- **Flexibility**: Easy to add/modify agent workflows

### Why Aurora Serverless?
- **Cost**: Pay only for usage (scales to zero)
- **pgvector**: Native vector similarity search
- **Bedrock integration**: Direct support as KB backend
- **Managed**: No infrastructure management

### Why Iceberg?
- **ACID**: Transactional guarantees for data lake
- **Time travel**: Query historical snapshots
- **Schema evolution**: Add columns without rewrites
- **Performance**: Partition pruning and metadata optimization

### Why MCP (Model Context Protocol)?
- **Abstraction**: Clean interface between agents and data
- **Reusability**: Same tools across multiple agents
- **Standardization**: Industry-standard protocol
- **Flexibility**: Easy to add new data sources

## Security

### Authentication
- Cognito user pools for user management
- Identity pools for AWS resource access
- JWT tokens for API authentication

### Authorization
- IAM roles with least-privilege policies
- Resource-based policies for S3, Aurora
- Secrets Manager for API keys
- VPC security groups for network isolation

### Data Protection
- Encryption at rest (S3, Aurora, Secrets Manager)
- Encryption in transit (TLS)
- No PII in logs or traces
- Audit logging via CloudTrail

## Scalability

### Horizontal Scaling
- Lambda: Automatic concurrency scaling
- ECS Fargate: Task count adjustment
- Aurora: ACU scaling (0-4 for dev, higher for prod)

### Vertical Scaling
- Lambda memory: 512 MB (adjustable)
- ECS task: CPU/memory per task definition
- Spark: Worker node count

### Cost Optimization
- Aurora scales to zero when idle
- Lambda charged per invocation
- S3 lifecycle policies for old data
- Spot instances for Spark (optional)

## Monitoring & Observability

### Metrics
- CloudWatch metrics for all AWS services
- Custom metrics for agent performance
- Langfuse traces for LLM calls

### Logging
- CloudWatch Logs for Lambda, ECS
- Structured logging in agents
- Log retention policies

### Alerting
- SNS topics for failures
- Email notifications for Step Functions
- CloudWatch alarms for resource limits

## Future Enhancements

1. **Real-time streaming**: Kinesis for live market data
2. **Advanced analytics**: ML models for price prediction
3. **Multi-tenancy**: Separate data per user/organization
4. **API gateway**: Public API for external access
5. **UI dashboard**: React/Next.js frontend
6. **Caching layer**: Redis for frequently accessed data
7. **GraphQL API**: Flexible query interface
8. **Webhook support**: Push notifications for events
