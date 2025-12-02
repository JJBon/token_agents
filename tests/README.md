# Testing Guide

## Overview

This directory contains tests for the Crypto Token Intelligence Platform, with a focus on the Query Agent and its integration with the dbt semantic layer.

## Test Structure

```
tests/
├── unit/                    # Unit tests (mocked dependencies)
├── integration/             # Integration tests (real services, isolated)
├── e2e/                     # End-to-end tests (full system)
├── test_news_agent.py       # Legacy news agent tests
└── test_news_tools.py       # Legacy news tools tests
```

## Quick Start

### 1. Start Services

```bash
# Start all required services
docker-compose -f docker/spark/docker-compose.yml up -d

# Wait for services to be ready (30-60 seconds)
sleep 60

# Verify services are running
docker-compose -f docker/spark/docker-compose.yml ps
```

### 2. Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test level
pytest tests/integration/ -v          # Integration tests only
pytest tests/e2e/ -v                  # E2E tests only

# Run with output
pytest tests/integration/ -v -s       # Show print statements

# Run specific test
pytest tests/integration/test_mcp_dbt_integration.py::TestMCPTools::test_fetch_metrics -v
```

### 3. Interactive Testing

```bash
# Run interactive test script
python scripts/test_query_agent_interactive.py

# Or from docker
docker-compose -f docker/spark/docker-compose.yml exec langgraph-backend \
    python /app/scripts/test_query_agent_interactive.py

# Single query mode
python scripts/test_query_agent_interactive.py --query "What is Bitcoin's price?"

# Interactive mode
python scripts/test_query_agent_interactive.py --mode interactive
```

## Test Levels

### Unit Tests

Test individual components with mocked dependencies.

**Location**: `tests/unit/`

**Run**: `pytest tests/unit/ -v`

**Characteristics**:
- Fast (< 1 second per test)
- No external dependencies
- Mocked LLM, MCP, database

**Example**:
```python
@pytest.mark.unit
async def test_query_agent_with_mocked_tools():
    # Mock MCP tools
    mock_mcp = AsyncMock()
    mock_mcp.get_tools.return_value = [mock_tool]
    
    with patch('agents.query_agent.graph.MultiServerMCPClient', return_value=mock_mcp):
        graph = await build_graph()
        result = await graph.ainvoke(...)
```

### Integration Tests

Test component interactions with real services.

**Location**: `tests/integration/`

**Run**: `pytest tests/integration/ -v`

**Characteristics**:
- Medium speed (5-30 seconds per test)
- Real MCP server, dbt, Spark
- Requires docker services running

**Example**:
```python
@pytest.mark.integration
def test_fetch_query_result():
    result = call_mcp_tool("fetch_query_result", {
        "metrics": ["average_price_usd"],
        "limit": 5
    })
    assert result["status"] == "OK"
```

### End-to-End Tests

Test complete user workflows.

**Location**: `tests/e2e/`

**Run**: `pytest tests/e2e/ -v -s`

**Characteristics**:
- Slow (10-60 seconds per test)
- Full system including LLM
- Tests actual user queries

**Example**:
```python
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_bitcoin_price():
    graph = await build_graph()
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="What is Bitcoin's price?")]
    })
    assert "bitcoin" in result["messages"][-1].content.lower()
```

## Test Markers

Use pytest markers to run specific test categories:

```bash
# Run only integration tests
pytest -m integration

# Run only e2e tests
pytest -m e2e

# Skip slow tests
pytest -m "not slow"

# Run only unit tests
pytest -m unit
```

**Available markers**:
- `unit` - Unit tests (fast, mocked)
- `integration` - Integration tests (medium, real services)
- `e2e` - End-to-end tests (slow, full system)
- `slow` - Slow tests (> 30 seconds)

## Prerequisites

### Required Services

1. **Spark Master** - Thrift server for SQL queries
2. **dbt-mcp** - MCP server for semantic layer
3. **LiteLLM** - LLM proxy for Bedrock
4. **PostgreSQL** - LiteLLM database

### Required Data

Ensure you have data in Glue catalog:

```bash
# Check data exists
aws glue get-table --database-name coingecko --table-name coingecko_raw

# Load sample data if needed
cd sample_data
python load_sample_data.py
```

### Environment Variables

Ensure these env files are configured:
- `env/.env_dev` - Spark/AWS credentials
- `env/.env_dev_langgraph` - LangGraph configuration
- `env/.env_dev_dbt_mcp` - MCP server configuration
- `env/.env_litellm` - LiteLLM/AWS credentials

## Troubleshooting

### Services Not Ready

```bash
# Check service status
docker-compose -f docker/spark/docker-compose.yml ps

# Check logs
docker-compose -f docker/spark/docker-compose.yml logs dbt-mcp
docker-compose -f docker/spark/docker-compose.yml logs litellm

# Restart services
docker-compose -f docker/spark/docker-compose.yml restart
```

### MCP Connection Failed

```bash
# Test MCP endpoint
curl http://localhost:8001/mcp

# Check MCP logs
docker-compose -f docker/spark/docker-compose.yml logs -f dbt-mcp

# Restart MCP server
docker-compose -f docker/spark/docker-compose.yml restart dbt-mcp
```

### Spark Connection Failed

```bash
# Start Thrift server
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
    /spark_utils/start-thrift-server.sh

# Test connection
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
    beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
```

### No Data in Tables

```bash
# Check Glue catalog
aws glue get-tables --database-name coingecko

# Trigger data ingestion
aws lambda invoke \
    --function-name coingecko_snapshot_ingest \
    --payload '{}' \
    response.json
```

### Tests Timing Out

```bash
# Increase timeout in pytest.ini
[pytest]
timeout = 300

# Or skip slow tests
pytest -m "not slow"
```

## Writing New Tests

### Unit Test Template

```python
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.unit
async def test_my_feature():
    # Arrange
    mock_dependency = AsyncMock()
    mock_dependency.method.return_value = "expected"
    
    # Act
    with patch('module.dependency', mock_dependency):
        result = await my_function()
    
    # Assert
    assert result == "expected"
```

### Integration Test Template

```python
import pytest
import requests

@pytest.mark.integration
def test_my_integration():
    # Arrange
    payload = {"key": "value"}
    
    # Act
    response = requests.post("http://localhost:8001/endpoint", json=payload)
    
    # Assert
    assert response.status_code == 200
    assert response.json()["status"] == "OK"
```

### E2E Test Template

```python
import pytest
from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_my_workflow():
    # Arrange
    graph = await build_graph()
    
    # Act
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="My question")]
    }, config={"configurable": {"thread_id": "test-1"}})
    
    # Assert
    last_message = result["messages"][-1]
    assert "expected" in last_message.content.lower()
```

## CI/CD Integration

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-asyncio pytest-mock
      
      - name: Run unit tests
        run: pytest tests/unit/ -v
      
      - name: Start services
        run: docker-compose -f docker/spark/docker-compose.yml up -d
      
      - name: Wait for services
        run: sleep 60
      
      - name: Run integration tests
        run: pytest tests/integration/ -v
      
      - name: Run E2E tests
        run: pytest tests/e2e/ -v
```

## Performance Benchmarks

Expected test performance:

| Test Level | Tests | Time | Pass Rate |
|------------|-------|------|-----------|
| Unit | 20+ | < 10s | 100% |
| Integration | 10+ | 1-2 min | > 95% |
| E2E | 10+ | 3-5 min | > 90% |

## Coverage Goals

- Unit tests: > 80% code coverage
- Integration tests: All critical paths
- E2E tests: All user workflows

Check coverage:
```bash
pytest --cov=agents --cov=tools --cov-report=html
open htmlcov/index.html
```

## Resources

- [Testing Guide](../docs/TESTING_QUERY_AGENT.md) - Detailed testing documentation
- [Development Guide](../docs/DEVELOPMENT.md) - Development best practices
- [pytest Documentation](https://docs.pytest.org/) - pytest reference
