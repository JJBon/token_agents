# Testing Query Agent with dbt Semantic Layer

## Overview

This guide covers testing the Query Agent that uses MCP (Model Context Protocol) to access the dbt semantic layer, which connects to Spark/Glue tables for querying cryptocurrency data.

## Architecture

```
Query Agent (LangGraph)
    ↓
MCP Client (langchain-mcp-adapters)
    ↓
MCP Server (FastMCP - dbt-mcp container)
    ↓
dbt Client (MetricFlow)
    ↓
Spark Thrift Server
    ↓
Glue Catalog / Iceberg Tables
```

## Prerequisites

### 1. Environment Setup

Ensure you have these environment files configured:

**env/.env_dev** (for Spark):
```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_SESSION_TOKEN=your_token  # if using temporary credentials
```

**env/.env_dev_langgraph** (for LangGraph):
```bash
LLM_BACKEND=litellm
LLM_MODE=bedrock
LITELLM_BASE_URL=http://litellm:4000
LITELLM_MODEL_NAME=bedrock-claude-haiku
MCP_MODE=streamable_http
MCP_DBT_URL=http://dbt-mcp:8001
MCP_DBT_PATH=/mcp
```

**env/.env_dev_dbt_mcp** (for MCP server):
```bash
MCP_TRANSPORT=streamable-http
MCP_HOST=0.0.0.0
MCP_PORT=8001
DBT_PROJECT_DIR=/dbt/coin_spark
DBT_PROFILES_DIR=/dbt/profiles
DBT_TARGET=dev
```

**env/.env_litellm** (for LiteLLM proxy):
```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_SESSION_TOKEN=your_token
```

### 2. Data Setup

Ensure you have data in your Glue catalog:

```bash
# Check if data exists
aws glue get-table --database-name coingecko --table-name coingecko_raw

# Or load sample data
cd sample_data
python load_sample_data.py
```

### 3. dbt Configuration

Verify your dbt project is configured:

**dbt/coin_spark/dbt_project.yml**:
```yaml
name: coin_spark
version: '1.0.0'
profile: coin_spark

models:
  coin_spark:
    +location_root: 's3://your-bucket/dbt'
    +file_format: iceberg
    +materialized: table
```

**dbt/profiles.yml**:
```yaml
coin_spark:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: spark-master
      port: 10000
      schema: default
      threads: 4
```

## Testing Levels

### Level 1: Component Testing (Individual Services)

#### Test 1.1: Spark Thrift Server

```bash
# Start services
docker-compose -f docker/spark/docker-compose.yml up -d spark-master

# Wait for Spark to be ready
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
  /spark_utils/start-thrift-server.sh

# Test connection
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
  beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"

# Query Glue tables
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
  beeline -u "jdbc:hive2://localhost:10000" -e "SELECT * FROM coingecko.coingecko_raw LIMIT 5;"
```

**Expected Output**:
```
+----------+---------------+------------+
| name     | current_price | market_cap |
+----------+---------------+------------+
| Bitcoin  | 45234.56      | 890000000  |
| Ethereum | 2345.67       | 280000000  |
+----------+---------------+------------+
```

#### Test 1.2: dbt Semantic Layer

```bash
# Enter Spark container
docker-compose -f docker/spark/docker-compose.yml exec spark-master bash

# Run dbt
cd /var/lib/spark/coin_spark
dbt debug
dbt run

# Test MetricFlow
mf list metrics
mf query --metrics average_price_usd --group-by metric_time__day --limit 10
```

**Expected Output**:
```
✓ Connection test: OK
✓ dbt version: 1.9.0
✓ Spark connection: OK

Available metrics:
- average_price_usd
- total_market_cap
- daily_volume
```

#### Test 1.3: MCP Server

```bash
# Start MCP server
docker-compose -f docker/spark/docker-compose.yml up -d dbt-mcp

# Check logs
docker-compose -f docker/spark/docker-compose.yml logs -f dbt-mcp

# Test HTTP endpoint
curl http://localhost:8001/mcp

# Test fetch_metrics tool
curl -X POST http://localhost:8001/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "fetch_metrics",
      "arguments": {}
    },
    "id": 1
  }'
```

**Expected Output**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"metrics\": [{\"name\": \"average_price_usd\", ...}]}"
      }
    ]
  },
  "id": 1
}
```

#### Test 1.4: LiteLLM Proxy

```bash
# Start LiteLLM
docker-compose -f docker/spark/docker-compose.yml up -d litellm

# Test health
curl http://localhost:4000/health

# Test model
curl -X POST http://localhost:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "bedrock-claude-haiku",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

### Level 2: Integration Testing (MCP + dbt)

Create a test script to verify MCP → dbt → Spark flow:

**tests/integration/test_mcp_dbt_integration.py**:
```python
import pytest
import requests
import json

MCP_URL = "http://localhost:8001/mcp"

def call_mcp_tool(tool_name: str, arguments: dict = None):
    """Helper to call MCP tools."""
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
        },
        "id": 1
    }
    response = requests.post(MCP_URL, json=payload)
    response.raise_for_status()
    return response.json()

@pytest.mark.integration
def test_fetch_metrics():
    """Test fetching available metrics."""
    result = call_mcp_tool("fetch_metrics")
    
    assert "result" in result
    content = json.loads(result["result"]["content"][0]["text"])
    assert "metrics" in content
    assert len(content["metrics"]) > 0
    
    # Verify expected metrics exist
    metric_names = [m["name"] for m in content["metrics"]]
    assert "average_price_usd" in metric_names

@pytest.mark.integration
def test_create_query():
    """Test query validation."""
    result = call_mcp_tool("create_query", {
        "metrics": ["average_price_usd"],
        "group_by": [{"name": "metric_time__day"}],
        "limit": 10
    })
    
    assert "result" in result
    content = json.loads(result["result"]["content"][0]["text"])
    assert content["status"] == "OK"
    assert "sql" in content

@pytest.mark.integration
def test_fetch_query_result():
    """Test query execution."""
    result = call_mcp_tool("fetch_query_result", {
        "metrics": ["average_price_usd"],
        "group_by": [{"name": "metric_time__day"}],
        "order_by": ["metric_time__day desc"],
        "limit": 5
    })
    
    assert "result" in result
    content = json.loads(result["result"]["content"][0]["text"])
    assert content["status"] == "OK"
    assert "table" in content  # Markdown table
    assert "rows" in content
    assert len(content["rows"]) > 0

@pytest.mark.integration
def test_query_with_filters():
    """Test query with WHERE conditions."""
    result = call_mcp_tool("fetch_query_result", {
        "metrics": ["average_price_usd"],
        "group_by": [{"name": "name"}],
        "where": {
            "conditions": [
                {
                    "dimension": "name",
                    "operator": "=",
                    "value": "Bitcoin"
                }
            ],
            "logic": "AND"
        },
        "limit": 1
    })
    
    assert "result" in result
    content = json.loads(result["result"]["content"][0]["text"])
    assert content["status"] == "OK"
    assert len(content["rows"]) == 1
    assert content["rows"][0]["name"] == "Bitcoin"
```

Run integration tests:
```bash
# Start all services
docker-compose -f docker/spark/docker-compose.yml up -d

# Wait for services to be ready (30-60 seconds)
sleep 60

# Run tests
pytest tests/integration/test_mcp_dbt_integration.py -v
```

### Level 3: Agent Testing (Query Agent)

#### Test 3.1: Unit Tests (Mocked)

**tests/unit/test_query_agent_unit.py**:
```python
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.tools import StructuredTool

def mock_fetch_metrics() -> str:
    return json.dumps({
        "metrics": [
            {"name": "average_price_usd", "type": "simple"},
            {"name": "total_market_cap", "type": "simple"}
        ],
        "dimensions": [
            {"name": "name", "type": "categorical"},
            {"name": "metric_time__day", "type": "time"}
        ]
    })

def mock_fetch_query_result(metrics, group_by, **kwargs) -> str:
    return json.dumps({
        "status": "OK",
        "rows": [
            {"name": "Bitcoin", "average_price_usd": 45234.56},
            {"name": "Ethereum", "average_price_usd": 2345.67}
        ],
        "table": "| name | average_price_usd |\n|------|------------------|\n| Bitcoin | 45234.56 |"
    })

@pytest.mark.asyncio
async def test_query_agent_with_mocked_tools():
    """Test agent with mocked MCP tools."""
    
    # Create mock tools
    fetch_metrics_tool = StructuredTool.from_function(
        func=mock_fetch_metrics,
        name="fetch_metrics",
        description="List available metrics"
    )
    
    fetch_query_tool = StructuredTool.from_function(
        func=mock_fetch_query_result,
        name="fetch_query_result",
        description="Execute query"
    )
    
    mock_mcp = AsyncMock()
    mock_mcp.get_tools.return_value = [fetch_metrics_tool, fetch_query_tool]
    
    with patch('agents.query_agent.graph.MultiServerMCPClient', return_value=mock_mcp):
        graph = await build_graph()
        
        result = await graph.ainvoke({
            "messages": [HumanMessage(content="What is Bitcoin's average price?")]
        }, config={"configurable": {"thread_id": "test-1"}})
        
        assert "messages" in result
        last_message = result["messages"][-1]
        assert isinstance(last_message, AIMessage)
        assert "bitcoin" in last_message.content.lower()
```

#### Test 3.2: End-to-End Tests (Real Services)

**tests/e2e/test_query_agent_e2e.py**:
```python
import pytest
import asyncio
from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_bitcoin_price():
    """Test querying Bitcoin price."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="What is Bitcoin's average price?")]
    }, config={"configurable": {"thread_id": "e2e-1"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Verify response contains expected elements
    assert "bitcoin" in content
    assert any(word in content for word in ["price", "average", "usd"])
    # Should contain a number
    assert any(char.isdigit() for char in content)

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_top_coins():
    """Test querying top cryptocurrencies."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="Show me the top 5 cryptocurrencies by market cap")]
    }, config={"configurable": {"thread_id": "e2e-2"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Should mention multiple coins
    assert "bitcoin" in content or "ethereum" in content
    # Should have data
    assert any(char.isdigit() for char in content)

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_time_series():
    """Test querying time series data."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="Show Bitcoin price trends for the last 7 days")]
    }, config={"configurable": {"thread_id": "e2e-3"}})
    
    last_message = result["messages"][-1]
    content = last_message.content
    
    # Should contain dates or time references
    assert any(word in content.lower() for word in ["day", "date", "time", "trend"])
    # Should have multiple data points
    assert content.count("|") > 5  # Markdown table
```

Run E2E tests:
```bash
# Ensure all services are running
docker-compose -f docker/spark/docker-compose.yml up -d

# Run E2E tests
pytest tests/e2e/test_query_agent_e2e.py -v -s
```

### Level 4: Interactive Testing

#### Test 4.1: Direct Agent Invocation

Create a test script:

**scripts/test_query_agent_interactive.py**:
```python
#!/usr/bin/env python
import asyncio
from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage

async def test_query(question: str):
    """Test a single query."""
    print(f"\n{'='*60}")
    print(f"Question: {question}")
    print(f"{'='*60}\n")
    
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content=question)]
    }, config={"configurable": {"thread_id": "interactive-test"}})
    
    last_message = result["messages"][-1]
    print(f"Response:\n{last_message.content}\n")
    
    return result

async def main():
    """Run multiple test queries."""
    queries = [
        "What metrics are available?",
        "What is Bitcoin's average price?",
        "Show me the top 5 cryptocurrencies by market cap",
        "Compare Bitcoin and Ethereum prices",
        "Show price trends for the last 7 days",
    ]
    
    for query in queries:
        try:
            await test_query(query)
            await asyncio.sleep(2)  # Rate limiting
        except Exception as e:
            print(f"Error: {e}\n")

if __name__ == "__main__":
    asyncio.run(main())
```

Run:
```bash
# From langgraph container
docker-compose -f docker/spark/docker-compose.yml exec langgraph-backend \
  python /app/scripts/test_query_agent_interactive.py
```

#### Test 4.2: Via BentoML Service

```bash
# Start BentoML service
docker-compose -f docker/spark/docker-compose.yml exec langgraph-backend \
  bentoml serve service:FeedbackAgentService --host 0.0.0.0 --port 8000

# Test via curl
curl -X POST http://localhost:8000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is Bitcoin average price?",
    "session_id": "test-session"
  }'

# Test OpenAI-compatible endpoint
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "query-agent",
    "messages": [
      {"role": "user", "content": "Show me top 5 cryptocurrencies"}
    ]
  }'
```

## Troubleshooting

### Issue 1: MCP Connection Failed

**Symptoms**:
```
Error: Connection refused to dbt-mcp:8001
```

**Solutions**:
```bash
# Check MCP server is running
docker-compose -f docker/spark/docker-compose.yml ps dbt-mcp

# Check logs
docker-compose -f docker/spark/docker-compose.yml logs dbt-mcp

# Restart MCP server
docker-compose -f docker/spark/docker-compose.yml restart dbt-mcp

# Test connectivity
docker-compose -f docker/spark/docker-compose.yml exec langgraph-backend \
  curl http://dbt-mcp:8001/mcp
```

### Issue 2: Spark Connection Failed

**Symptoms**:
```
Error: Could not connect to Thrift server
```

**Solutions**:
```bash
# Check Spark is running
docker-compose -f docker/spark/docker-compose.yml ps spark-master

# Start Thrift server
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
  /spark_utils/start-thrift-server.sh

# Test connection
docker-compose -f docker/spark/docker-compose.yml exec spark-master \
  beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
```

### Issue 3: No Data in Glue Tables

**Symptoms**:
```
Error: Table coingecko.coingecko_raw not found
```

**Solutions**:
```bash
# Check Glue catalog
aws glue get-tables --database-name coingecko

# Load sample data
cd sample_data
python load_sample_data.py

# Or trigger Lambda
aws lambda invoke \
  --function-name coingecko_snapshot_ingest \
  --payload '{}' \
  response.json
```

### Issue 4: dbt Metrics Not Found

**Symptoms**:
```
Error: No metrics found
```

**Solutions**:
```bash
# Run dbt
docker-compose -f docker/spark/docker-compose.yml exec spark-master bash
cd /var/lib/spark/coin_spark
dbt run

# Check metrics
mf list metrics

# Validate semantic models
dbt parse
```

## Performance Testing

### Latency Benchmarks

**scripts/benchmark_query_agent.py**:
```python
import asyncio
import time
from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage

async def benchmark_query(question: str, iterations: int = 5):
    """Benchmark query latency."""
    graph = await build_graph()
    latencies = []
    
    for i in range(iterations):
        start = time.time()
        await graph.ainvoke({
            "messages": [HumanMessage(content=question)]
        }, config={"configurable": {"thread_id": f"bench-{i}"}})
        latency = time.time() - start
        latencies.append(latency)
        print(f"Iteration {i+1}: {latency:.2f}s")
    
    avg = sum(latencies) / len(latencies)
    print(f"\nAverage latency: {avg:.2f}s")
    print(f"Min: {min(latencies):.2f}s, Max: {max(latencies):.2f}s")

asyncio.run(benchmark_query("What is Bitcoin's average price?"))
```

**Expected Performance**:
- Cold start: 5-10 seconds
- Warm queries: 2-5 seconds
- Cached results: < 1 second

## Test Checklist

Before considering testing complete:

- [ ] Spark Thrift Server accessible
- [ ] dbt can connect to Spark
- [ ] MetricFlow lists metrics
- [ ] MCP server responds to HTTP requests
- [ ] MCP tools return valid JSON
- [ ] LiteLLM proxy works
- [ ] Query Agent can call MCP tools
- [ ] Agent returns data + insight
- [ ] Queries with filters work
- [ ] Time series queries work
- [ ] Error handling works
- [ ] Performance is acceptable

## Next Steps

After testing is complete:
1. Document any issues found
2. Create tickets for bugs
3. Update documentation with learnings
4. Add more test cases for edge cases
5. Set up CI/CD for automated testing
