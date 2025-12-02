# Agent System Guide

## Overview

The platform uses a multi-agent architecture built with LangGraph, where specialized agents handle different aspects of cryptocurrency intelligence. Each agent is an independent graph with its own state, tools, and decision logic.

## Agent Hierarchy

```
                    ┌─────────────────────┐
                    │  Conversation Agent │
                    │  (User Interface)   │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Supervisor Agent   │
                    │  (Orchestrator)     │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼────────┐ ┌────▼─────┐ ┌────────▼────────┐
    │   Query Agent    │ │  News    │ │  Market Agent   │
    │                  │ │  Agent   │ │                 │
    └──────────────────┘ └──────────┘ └─────────────────┘
```

## Agent Descriptions

### 1. Conversation Agent

**Purpose**: User-facing interface for natural language interaction

**Location**: `docker/langgraph/app/agents/conversation_agent/graph.py`

**State**:
```python
{
    "messages": List[BaseMessage],  # Conversation history
    "feedback": Optional[str]        # Persistent user preferences
}
```

**Features**:
- Maintains conversation history via LangGraph checkpointer
- Supports persistent feedback mechanism
- Routes between assistant and feedback storage
- Configurable LLM backend (Ollama, LiteLLM/Bedrock)

**Workflow**:
```
User Input → Route Decision
              ├─→ "feedback:" prefix → Store Feedback → Acknowledge
              └─→ Regular message → Assistant → Response
```

**Usage Example**:
```python
# Regular query
"What's the current price of Bitcoin?"

# Set persistent feedback
"feedback: always respond in Spanish and be concise"

# Subsequent queries will follow the feedback
"Tell me about Ethereum"  # Response will be in Spanish
```

**Configuration**:
- `LLM_BACKEND`: "litellm" or "ollama" (default: litellm)
- `LITELLM_MODEL_NAME`: Model to use via LiteLLM
- `LITELLM_BASE_URL`: LiteLLM proxy endpoint
- `OLLAMA_MODEL`: Model for local Ollama

---

### 2. Supervisor Agent

**Purpose**: Orchestrates specialized agents and validates response quality

**Location**: `docker/langgraph/app/agents/supervisor_agent/graph.py`

**State**:
```python
{
    "messages": List[BaseMessage],
    "agent_calls": int,              # Number of agent invocations
    "last_signature": Optional[str], # Hash for stagnation detection
    "next": Optional[str]            # Routing decision
}
```

**LLM**: Claude Haiku (fast, cost-effective for routing decisions)

**Decision Logic**:
The supervisor uses structured output to evaluate responses:

```python
class InferredRoute:
    next: Literal["FINISH", "query_agent"]
    inferred_tools: List[str]        # Tools the agent should have used
    violations: List[str]            # Missing elements
    feedback: str                    # Guidance for next iteration
    has_data: bool                   # Response contains data
    has_insight: bool                # Response contains analysis
    signature: str                   # Unique response fingerprint
    reasoning: Optional[str]         # Explanation
```

**Workflow**:
```
1. Receive user request
2. Call Query Agent
3. Evaluate response:
   ├─→ Has data + insight? → FINISH
   ├─→ Missing elements? → Provide feedback → Retry (max 2)
   ├─→ Stagnation detected? → FINISH
   └─→ Max attempts reached? → FINISH
```

**Stagnation Detection**:
- Computes SHA256 hash of response signature
- If signature matches previous attempt → terminates
- Prevents infinite loops

**Termination Conditions**:
1. Response has both data and insight
2. Agent called 2+ times
3. Response signature unchanged (stagnation)
4. No critical violations

---

### 3. Query Agent

**Purpose**: Executes data queries using MCP tools and dbt semantic layer

**Location**: `docker/langgraph/app/agents/query_agent/graph.py`

**State**:
```python
{
    "messages": List[BaseMessage]
}
```

**Tools** (via MCP):
- `fetch_metrics`: List available metrics from dbt semantic layer
- `create_query`: Generate SQL from natural language
- `fetch_query_result`: Execute query and return results

**MCP Configuration**:
Two modes supported:

1. **stdio mode** (default):
```python
{
    "dbt": {
        "command": "python",
        "args": ["/app/tools/query_tools/mcp_tools.py"],
        "transport": "stdio"
    }
}
```

2. **streamable_http mode**:
```python
{
    "dbt": {
        "url": "http://dbt-mcp:8001/mcp",
        "transport": "streamable_http"
    }
}
```

**Workflow**:
```
User Query
    ↓
1. fetch_metrics → Get available metrics
    ↓
2. create_query → Generate SQL from natural language
    ↓
3. fetch_query_result → Execute via Spark Thrift Server
    ↓
Response with data + insight
```

**LLM Configuration**:
- `LLM_MODE`: "bedrock" or "litellm"
- Model: Claude Haiku or Sonnet (configurable)
- Temperature: 0.0 (deterministic)

**Bedrock Hygiene**:
The agent includes special handling for Bedrock's strict message ordering:
- Sanitizes message history before LLM calls
- Removes trailing human messages after tool_use without tool_result
- Prevents API errors from malformed message sequences

**Example Query Flow**:
```
User: "Show me Bitcoin price trends for the last week"
    ↓
Agent: [calls fetch_metrics]
    ← Returns: ["bitcoin_price", "bitcoin_volume", ...]
    ↓
Agent: [calls create_query with metrics and user intent]
    ← Returns: SQL query
    ↓
Agent: [calls fetch_query_result with SQL]
    ← Returns: Query results
    ↓
Agent: Formats response with data + insight
```

---

### 4. News Agent

**Purpose**: Ingests and processes cryptocurrency news articles

**Location**: `docker/langgraph/app/agents/news_agent/graph.py`

**State**:
```python
{
    "messages": List[Any],
    "api_url": str,
    "max_articles": int,
    "timeout_s": int,
    "extractor_temperature": float,
    "ingest_mode": str,              # "s3" or "direct"
    "wait_for_ingest": bool,
    
    # Working data
    "items": List[Dict],             # Raw API response
    "to_process": List[Dict],        # After deduplication
    "enriched_new": List[Dict],      # After extraction
    "enriched_all": List[Dict],      # Final merged data
    
    # Metrics
    "dedup_skipped": int,
    "kb_uploaded": int,
    "kb_ingestion_job_id": str,
    "bronze_count": int,
    "iceberg_count": int
}
```

**Pipeline Stages**:

1. **Ensure Tables**: Create Iceberg tables if not exist
2. **Fetch API**: Get news from CryptoNews API
3. **Dedupe**: Check against existing news_ids in Iceberg
4. **Extract**: 
   - Scrape full article text
   - Use LLM to extract token mentions
   - Merge with keyword hints
5. **Ingest**: Store in Bedrock Knowledge Base (S3 or direct)
6. **Stitch**: Merge enriched data with original items
7. **Persist Bronze**: Store raw data in S3
8. **Persist Iceberg**: Store in queryable Iceberg table

**LLM Extraction**:
```python
# Extracts structured token mentions
class TokenMention:
    symbol: str          # e.g., "BTC"
    name: str           # e.g., "Bitcoin"
    context: str        # Surrounding text
    sentiment: str      # "positive", "negative", "neutral"
```

**Ingest Modes**:

1. **S3 Mode** (traditional):
   - Upload documents to S3
   - Start Bedrock ingestion job
   - Optionally wait for completion

2. **Direct Mode** (new):
   - Call Bedrock IngestDocuments API directly
   - Per-document success/failure tracking
   - Faster for small batches

**Concurrency**:
- Parallel article scraping (default: 8 concurrent)
- Configurable via `EXTRACT_CONCURRENCY` env var
- Retry logic for LLM extraction failures

**Deployment**:
- Runs as ECS Fargate task
- Triggered by Step Functions (daily schedule)
- Can also run standalone via CLI

**CLI Usage**:
```bash
python graph.py \
  --api-url "https://cryptonews-api.com/api/v1/..." \
  --max-articles 50 \
  --timeout-s 15 \
  --extractor-temperature 0.3 \
  --ingest-mode direct \
  --wait-for-ingest
```

---

### 5. Market Agent

**Purpose**: Synthesizes research papers with recent news for marketing briefs

**Location**: `docker/langgraph/app/agents/market_agent/graph.py`

**State**:
```python
{
    # Inputs
    "s3_uri": str,                   # Optional specific research doc
    "max_docs": int,                 # Number of papers to analyze
    "k_per_doc": int,                # Chunks per document
    
    # Working data
    "research_doc_ids": List[str],
    "research_chunks_by_doc": Dict[str, List[Dict]],
    "research_chunks_all": List[Dict],
    "athena_news": List[Dict],
    "queries": List[str],
    "news_items": List[Dict],
    "news_assignments": List[Dict],  # News-to-paper pairings
    
    # Outputs
    "per_doc_briefs": Dict[str, Dict],  # Brief per research paper
    "overview": Dict                     # Aggregate statistics
}
```

**Pipeline Stages**:

1. **Ensure Tables**: Create marketing tables in Glue
2. **Discover Research Docs**: Find relevant research papers
3. **Retrieve Research Multi**: Get chunks from all papers
4. **Fetch Athena Latest**: Get recent news from data lake
5. **Choose Queries**: Generate semantic search queries from research
6. **Query News Vectors**: Find relevant news via vector search
7. **Assign News to Papers**: Pair each news item with best-matching paper
8. **Synthesize Briefs**: Generate marketing brief per paper

**News-to-Paper Pairing**:
```python
# For each news item:
1. Embed news text (title + tags + symbols)
2. Embed all research chunks
3. Compute cosine similarity
4. Assign to paper with highest similarity (if > threshold)
```

**Pairing Configuration**:
- `PAIR_MIN_SIM`: Minimum similarity threshold (default: 0.22)
- Prevents weak/irrelevant pairings

**Brief Structure**:
```python
{
    "doc_id": str,
    "title": str,
    "summary": str,              # Executive summary
    "key_findings": List[str],   # From research
    "news_highlights": List[str], # Relevant news
    "market_implications": str,   # Analysis
    "recommendations": List[str]  # Action items
}
```

**Use Cases**:
- Marketing team needs to connect research to current events
- Investment analysis combining fundamentals + news
- Content creation for newsletters/reports

---

## Agent Communication

### Message Types

**HumanMessage**: User input or inter-agent communication
```python
HumanMessage(
    content="fetch bitcoin data",
    name="user"  # or "supervisor", "query_agent", etc.
)
```

**AIMessage**: LLM responses
```python
AIMessage(
    content="Here's the data...",
    tool_calls=[...]  # Optional tool invocations
)
```

**ToolMessage**: Tool execution results
```python
ToolMessage(
    content=json.dumps(result),
    tool_call_id="call_123"
)
```

### State Management

**Checkpointing**: All agents use `MemorySaver` for conversation history
```python
checkpointer = MemorySaver()
graph = graph_builder.compile(checkpointer=checkpointer)
```

**Thread IDs**: Separate conversations via `thread_id`
```python
config = {"configurable": {"thread_id": "user-123"}}
result = graph.invoke(state, config=config)
```

**State Merging**: LangGraph automatically merges state updates
```python
# Agent returns partial state update
return {"messages": [new_message], "agent_calls": calls + 1}
# LangGraph merges with existing state
```

---

## Observability

### Langfuse Integration

All agents support Langfuse tracing:
```python
from langfuse.langchain import CallbackHandler
lf_handler = CallbackHandler()

config = {
    "callbacks": [lf_handler],
    "tags": ["query_agent", "production"],
    "metadata": {"user_id": "123"}
}
```

**Trace Hierarchy**:
```
Session (thread_id)
  └─ Agent Execution
      ├─ LLM Call
      ├─ Tool Call
      │   └─ Sub-tool Call
      └─ LLM Call (final)
```

### Logging

Structured logging throughout:
```python
import logging
logger = logging.getLogger(__name__)

logger.info("Processing request", extra={
    "user_id": user_id,
    "agent": "query_agent",
    "tools_used": ["fetch_metrics"]
})
```

---

## Best Practices

### 1. Agent Design

**Single Responsibility**: Each agent has one clear purpose
- Query Agent: Data retrieval only
- News Agent: Ingestion only
- Market Agent: Synthesis only

**Stateless Tools**: Tools should be pure functions when possible
```python
# Good: Pure function
def calculate_metric(data: List[float]) -> float:
    return sum(data) / len(data)

# Avoid: Stateful tool
class MetricCalculator:
    def __init__(self):
        self.cache = {}  # State makes testing harder
```

**Error Handling**: Always handle tool failures gracefully
```python
try:
    result = tool.invoke(input)
except Exception as e:
    logger.error(f"Tool failed: {e}")
    return {"error": str(e), "fallback": default_value}
```

### 2. Prompt Engineering

**System Prompts**: Store in `prompts/prompts.py`
```python
query_agent_system_prompt = Prompt(
    name="query_agent",
    prompt="""You are a data analyst specializing in cryptocurrency markets.
    
Your task is to:
1. Understand the user's data request
2. Use available tools to fetch relevant data
3. Provide clear insights based on the data

Always include both raw data and your analysis."""
)
```

**Tool Policies**: Guide tool usage in prompts
```python
tool_policy = """
Prefer this workflow:
1. fetch_metrics → see what's available
2. create_query → generate SQL
3. fetch_query_result → get data

Avoid repeating the same tool call with identical arguments.
"""
```

### 3. Testing

**Unit Tests**: Test individual nodes
```python
def test_dedupe_node():
    state = {
        "items": [
            {"news_id": "1", "title": "Bitcoin rises"},
            {"news_id": "1", "title": "Bitcoin rises"},  # Duplicate
        ]
    }
    result = dedupe_node(state)
    assert len(result["to_process"]) == 1
    assert result["dedup_skipped"] == 1
```

**Integration Tests**: Test full graph execution
```python
async def test_news_agent_pipeline():
    result = await run_once(
        api_url="https://test-api.com",
        max_articles=5,
        ingest_mode="direct"
    )
    assert result["iceberg_count"] > 0
    assert result["kb_direct_ok"] > 0
```

### 4. Performance

**Concurrency**: Use asyncio for I/O-bound operations
```python
async def process_batch(items: List[Dict]):
    tasks = [process_item(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]
```

**Caching**: Cache expensive operations
```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_metric_definition(metric_name: str) -> Dict:
    # Expensive lookup
    return fetch_from_catalog(metric_name)
```

**Batching**: Batch API calls when possible
```python
# Good: Single batch call
embeddings = embed_texts(all_texts)

# Avoid: Individual calls
embeddings = [embed_text(t) for t in all_texts]
```

---

## Troubleshooting

### Common Issues

**1. Agent Loops Infinitely**
- Check supervisor termination conditions
- Verify stagnation detection is working
- Add max_attempts limit

**2. Tool Calls Fail**
- Check MCP server is running
- Verify environment variables
- Test tool independently

**3. Memory Issues**
- Limit conversation history length
- Clear old checkpoints
- Reduce batch sizes

**4. Slow Response Times**
- Profile with Langfuse
- Check for sequential operations that could be parallel
- Consider faster LLM for routing (Haiku vs Sonnet)

### Debug Mode

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or per-agent
logger = logging.getLogger("agents.query_agent")
logger.setLevel(logging.DEBUG)
```

Inspect state at each step:
```python
for step in graph.stream(initial_state, config):
    print(f"Step: {step}")
    print(f"State: {step.get('messages', [])[-1]}")
```

---

## Future Enhancements

1. **Agent Collaboration**: Multiple agents working together on complex tasks
2. **Human-in-the-Loop**: Approval steps for critical operations
3. **Dynamic Tool Loading**: Load tools based on user permissions
4. **Multi-Modal**: Support for images, charts in responses
5. **Streaming**: Real-time response streaming for better UX
6. **Agent Memory**: Long-term memory beyond conversation history
7. **Self-Improvement**: Agents learn from feedback and improve over time
