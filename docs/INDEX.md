# Documentation Index

Welcome to the Crypto Token Intelligence Platform documentation.

## 🚀 Getting Started

New to the platform? Start here:

1. **[Quick Start Guide](QUICKSTART.md)** - Get up and running in 15 minutes
2. **[Architecture Overview](ARCHITECTURE.md)** - Understand the system design
3. **[Deployment Guide](DEPLOYMENT.md)** - Deploy to production

## 📖 Core Documentation

### Architecture & Design

- **[Architecture Guide](ARCHITECTURE.md)**
  - System overview and components
  - Data flow diagrams
  - Design decisions and rationale
  - Scalability and security considerations

- **[Agent System](AGENTS.md)**
  - Multi-agent architecture
  - Agent descriptions and workflows
  - Communication patterns
  - Best practices and troubleshooting

- **[Data Pipeline](DATA_PIPELINE.md)**
  - Data sources and ingestion
  - Storage formats and optimization
  - Transformation workflows (dbt + Spark)
  - Data quality and monitoring

### Development & Operations

- **[Development Guide](DEVELOPMENT.md)**
  - Local development setup
  - Code style and standards
  - Testing strategies
  - Debugging techniques
  - CI/CD workflows

- **[Deployment Guide](DEPLOYMENT.md)**
  - Infrastructure deployment
  - Configuration management
  - Monitoring and observability
  - Scaling and performance
  - Disaster recovery

- **[API Reference](API.md)**
  - REST API endpoints
  - Request/response formats
  - Authentication
  - Error codes
  - SDK examples

## 🎯 By Role

### For Data Engineers

1. [Data Pipeline](DATA_PIPELINE.md) - Ingestion and transformation
2. [Architecture](ARCHITECTURE.md#storage-layer) - Storage layer details
3. [Deployment](DEPLOYMENT.md#data-management) - Data management

### For ML Engineers

1. [Agent System](AGENTS.md) - Agent architecture
2. [Development Guide](DEVELOPMENT.md#testing-strategy) - Testing agents
3. [API Reference](API.md#agent-tools-internal) - Tool APIs

### For DevOps Engineers

1. [Deployment Guide](DEPLOYMENT.md) - Infrastructure as code
2. [Architecture](ARCHITECTURE.md#infrastructure-terraform) - Terraform modules
3. [Development Guide](DEVELOPMENT.md#continuous-integration) - CI/CD

### For Application Developers

1. [Quick Start](QUICKSTART.md) - Get started quickly
2. [API Reference](API.md) - Integration guide
3. [Development Guide](DEVELOPMENT.md#local-development-setup) - Local setup

## 🔍 By Topic

### Agents

- [Conversation Agent](AGENTS.md#1-conversation-agent) - User interface
- [Supervisor Agent](AGENTS.md#2-supervisor-agent) - Orchestration
- [Query Agent](AGENTS.md#3-query-agent) - Data queries
- [News Agent](AGENTS.md#4-news-agent) - News processing
- [Market Agent](AGENTS.md#5-market-agent) - Research synthesis

### Data

- [Market Data](DATA_PIPELINE.md#market-data-pipeline-lambda) - CoinGecko ingestion
- [News Data](DATA_PIPELINE.md#news-pipeline-ecs-fargate) - News processing
- [Research Papers](DATA_PIPELINE.md#3-research-papers) - Document ingestion
- [dbt Transformations](DATA_PIPELINE.md#dbt-project-structure) - Analytics

### Infrastructure

- [AWS Services](ARCHITECTURE.md#infrastructure-terraform) - Cloud resources
- [Terraform Modules](DEPLOYMENT.md#infrastructure-deployment) - IaC
- [Docker Containers](DEVELOPMENT.md#local-services) - Containerization
- [Monitoring](DEPLOYMENT.md#monitoring--observability) - Observability

### APIs

- [REST Endpoints](API.md#bentoml-service-endpoints) - HTTP APIs
- [Agent Tools](API.md#agent-tools-internal) - Internal tools
- [Data Access](API.md#data-access-apis) - Query interfaces

## 📝 Tutorials

### Basic Workflows

1. **Query Market Data**
   - [Quick Start](QUICKSTART.md#common-queries-to-try)
   - [Query Agent](AGENTS.md#3-query-agent)
   - [API Examples](API.md#examples)

2. **Process News**
   - [News Pipeline](DATA_PIPELINE.md#news-pipeline-ecs-fargate)
   - [News Agent](AGENTS.md#4-news-agent)
   - [Deployment](DEPLOYMENT.md#ecs-task-news-agent)

3. **Generate Insights**
   - [Market Agent](AGENTS.md#5-market-agent)
   - [Research Synthesis](QUICKSTART.md#research-driven-insights)

### Advanced Topics

1. **Custom Agents**
   - [Agent Development](DEVELOPMENT.md#feature-development)
   - [Testing Agents](DEVELOPMENT.md#test-individual-agents)
   - [Best Practices](AGENTS.md#best-practices)

2. **Data Transformations**
   - [dbt Models](DATA_PIPELINE.md#dbt-project-structure)
   - [Spark Integration](DATA_PIPELINE.md#spark-integration)
   - [Custom Metrics](QUICKSTART.md#custom-analytics)

3. **Production Deployment**
   - [Infrastructure Setup](DEPLOYMENT.md#infrastructure-deployment)
   - [Security Hardening](DEPLOYMENT.md#security-hardening)
   - [Scaling](DEPLOYMENT.md#scaling--performance)

## 🛠️ Reference

### Configuration

- [Environment Variables](DEPLOYMENT.md#environment-variables) - All config options
- [Terraform Variables](DEPLOYMENT.md#terraform-variables) - Infrastructure config
- [Agent Configuration](AGENTS.md#configuration) - Agent settings

### Commands

- [Makefile Targets](DEVELOPMENT.md#makefile-commands) - Build commands
- [AWS CLI](DEPLOYMENT.md#aws-cli-commands) - Cloud operations
- [Docker Commands](DEVELOPMENT.md#docker-commands) - Container management

### Troubleshooting

- [Common Issues](DEPLOYMENT.md#troubleshooting) - Known problems
- [Debug Guide](DEVELOPMENT.md#debugging) - Debugging techniques
- [FAQ](DEPLOYMENT.md#faq) - Frequently asked questions

## 🔗 External Resources

### Technologies

- [LangGraph](https://langchain-ai.github.io/langgraph/) - Agent framework
- [LangChain](https://python.langchain.com/) - LLM framework
- [AWS Bedrock](https://docs.aws.amazon.com/bedrock/) - LLM service
- [dbt](https://docs.getdbt.com/) - Data transformation
- [Apache Spark](https://spark.apache.org/docs/latest/) - Data processing
- [Terraform](https://www.terraform.io/docs) - Infrastructure as code

### APIs

- [CoinGecko API](https://www.coingecko.com/en/api/documentation) - Market data
- [CryptoNews API](https://cryptonews-api.com/documentation) - News data
- [AWS SDK](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - AWS services

## 📊 Diagrams

### System Architecture

See [Architecture Guide](ARCHITECTURE.md#high-level-architecture) for:
- High-level system diagram
- Data flow diagrams
- Component interactions

### Agent Workflows

See [Agent System](AGENTS.md#agent-hierarchy) for:
- Agent hierarchy
- Communication patterns
- Decision flows

### Data Pipeline

See [Data Pipeline](DATA_PIPELINE.md#pipeline-architecture) for:
- Ingestion workflows
- Transformation pipelines
- Storage architecture

## 🤝 Contributing

Want to contribute? Check out:

1. [Development Guide](DEVELOPMENT.md) - Setup and standards
2. [Code Style](DEVELOPMENT.md#code-style--standards) - Coding conventions
3. [Testing](DEVELOPMENT.md#testing-strategy) - Test requirements
4. [Pull Request Process](DEVELOPMENT.md#pull-request-checklist) - Contribution workflow

## 📞 Support

- **Documentation Issues**: Open a GitHub issue
- **Bug Reports**: Use issue template
- **Feature Requests**: Use feature request template
- **Questions**: Check FAQ or open discussion

## 📄 License

[Add your license information here]

---

**Last Updated**: January 2025  
**Version**: 1.0.0
