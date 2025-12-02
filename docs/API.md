# API Reference

## BentoML Service Endpoints

The platform exposes a BentoML service with two main endpoints for interacting with the conversation agent.

### Base URL

```
http://localhost:3000  # Local development
https://api.example.com  # Production
```

### Authentication

Currently no authentication required for local development. For production, implement:
- API keys via headers
- JWT tokens
- AWS Cognito integration

---

## POST /invoke

Simple endpoint for agent invocation.

### Request

**Content-Type**: `application/json`

**Body**:
```json
{
  "message": "string (required)",
  "session_id": "string (optional)"
}
```

**Parameters**:
- `message`: User query or command
- `session_id`: Conversation thread identifier (default: "default")

### Response

**Status**: 200 OK

**Body**:
```json
{
  "ok": true,
  "thread_id": "string",
  "response": "string"
}
```

**Fields**:
- `ok`: Success indicator
- `thread_id`: Session identifier used
- `response`: Agent's text response

### Examples

**Basic Query**:
```bash
curl -X POST http://localhost:3000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What is the current price of Bitcoin?",
    "session_id": "user-123"
  }'
```

**Response**:
```json
{
  "ok": true,
  "thread_id": "user-123",
  "response": "Based on the latest data, Bitcoin is trading at $45,234.56 USD..."
}
```

**Set Feedback**:
```bash
curl -X POST http://localhost:3000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "message": "feedback: always respond in Spanish and be concise",
    "session_id": "user-123"
  }'
```

**Response**:
```json
{
  "ok": true,
  "thread_id": "user-123",
  "response": "Got your feedback: 'always respond in Spanish and be concise'. I'll adapt my future responses accordingly."
}
```

**Error Response**:
```json
{
  "ok": false,
  "thread_id": "user-123",
  "response": "Graph error: Connection timeout"
}
```

---

## POST /v1/chat/completions

OpenAI-compatible endpoint for chat completions.

### Request

**Content-Type**: `application/json`

**Body**:
```json
{
  "model": "string (required)",
  "messages": [
    {
      "role": "user|assistant|system",
      "content": "string"
    }
  ],
  "user": "string (optional)",
  "session_id": "string (optional)",
  "temperature": 0.0,
  "max_tokens": 1000,
  "top_p": 1.0,
  "stream": false
}
```

**Parameters**:
- `model`: Model identifier (e.g., "crypto-agent")
- `messages`: Conversation history
- `user`: User identifier (used as thread_id)
- `session_id`: Alternative to `user` for thread_id
- `temperature`: Not used (agent has fixed temperature)
- `max_tokens`: Not used (no token limit)
- `top_p`: Not used
- `stream`: Must be false (streaming not supported)

### Response

**Status**: 200 OK

**Body**:
```json
{
  "id": "chatcmpl-1234567890",
  "object": "chat.completion",
  "created": 1234567890,
  "model": "crypto-agent",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "string"
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  }
}
```

**Fields**:
- `id`: Unique completion identifier
- `object`: Always "chat.completion"
- `created`: Unix timestamp
- `model`: Model name from request
- `choices`: Array of completion choices (always 1)
  - `index`: Choice index (always 0)
  - `message`: Assistant's response
  - `finish_reason`: Always "stop"
- `usage`: Token usage (always 0, not tracked)

### Examples

**Basic Chat**:
```bash
curl -X POST http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "crypto-agent",
    "messages": [
      {"role": "user", "content": "Show me Ethereum price trends"}
    ],
    "user": "user-456"
  }'
```

**Response**:
```json
{
  "id": "chatcmpl-1705234567",
  "object": "chat.completion",
  "created": 1705234567,
  "model": "crypto-agent",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Here are the Ethereum price trends for the last 7 days:\n\nDate       | Price (USD) | Change\n-----------|-------------|-------\n2025-01-15 | $2,345.67   | +2.3%\n..."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  }
}
```

**Multi-turn Conversation**:
```bash
curl -X POST http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "crypto-agent",
    "messages": [
      {"role": "user", "content": "What is Bitcoin?"},
      {"role": "assistant", "content": "Bitcoin is a decentralized digital currency..."},
      {"role": "user", "content": "What is its current price?"}
    ],
    "user": "user-456"
  }'
```

---

## Agent Tools (Internal)

These tools are used internally by agents and not exposed as public APIs.

### Query Agent Tools (MCP)

#### fetch_metrics

List available metrics from dbt semantic layer.

**Input**: None

**Output**:
```json
{
  "metrics": [
    {
      "name": "bitcoin_price",
      "description": "Current Bitcoin price in USD",
      "type": "simple"
    }
  ]
}
```

#### create_query

Generate SQL query from natural language.

**Input**:
```json
{
  "request": "Show Bitcoin price for last week",
  "metrics": ["bitcoin_price"],
  "dimensions": ["date"]
}
```

**Output**:
```json
{
  "sql": "SELECT date, AVG(current_price) as bitcoin_price FROM ...",
  "explanation": "This query aggregates Bitcoin prices by date"
}
```

#### fetch_query_result

Execute SQL query and return results.

**Input**:
```json
{
  "sql": "SELECT date, price FROM bitcoin_prices LIMIT 10"
}
```

**Output**:
```json
{
  "rows": [
    {"date": "2025-01-15", "price": 45234.56},
    {"date": "2025-01-14", "price": 44123.45}
  ],
  "row_count": 2
}
```

### News Agent Tools

#### fetch_news_api_tool

Fetch news from CryptoNews API.

**Input**:
```python
{
    "api_url": str,
    "timeout_s": int
}
```

**Output**:
```python
{
    "items": List[Dict[str, Any]]
}
```

#### scrape_article_text_tool

Scrape full article text from URL.

**Input**:
```python
{
    "url": str,
    "timeout_s": int
}
```

**Output**:
```python
{
    "text": str,
    "success": bool
}
```

#### llm_extract_mentions_tool

Extract token mentions from article using LLM.

**Input**:
```python
{
    "title": str,
    "source": str,
    "url": str,
    "body": str
}
```

**Output**:
```python
{
    "tokens": List[TokenMention]
}

# TokenMention schema:
{
    "symbol": str,      # e.g., "BTC"
    "name": str,        # e.g., "Bitcoin"
    "context": str,     # Surrounding text
    "sentiment": str    # "positive", "negative", "neutral"
}
```

### Market Agent Tools

#### list_research_docs_tool

List available research documents.

**Input**:
```python
{
    "seed": str,  # Search query
    "k": int      # Number of results
}
```

**Output**:
```python
{
    "docs": List[Dict[str, Any]]
}
```

#### retrieve_research_chunks_tool

Retrieve chunks from a research document.

**Input**:
```python
{
    "doc_id": str,
    "k": int
}
```

**Output**:
```python
{
    "chunks": List[Dict[str, Any]]
}
```

#### query_news_vectors_tool

Query news vectors with semantic search.

**Input**:
```python
{
    "queries": List[str],
    "top_k_per_query": int,
    "max_total": int,
    "metadata_filter": Dict[str, Any]
}
```

**Output**:
```python
List[Dict[str, Any]]  # News items with scores
```

#### synthesize_marketing_brief_tool

Generate marketing brief from research and news.

**Input**:
```python
{
    "doc_meta": Dict[str, Any],
    "research_chunks": List[Dict[str, Any]],
    "news_items": List[Dict[str, Any]]
}
```

**Output**:
```python
{
    "title": str,
    "summary": str,
    "key_findings": List[str],
    "news_highlights": List[str],
    "market_implications": str,
    "recommendations": List[str]
}
```

---

## Data Access APIs

### Athena SQL

Query data lake via Athena:

```python
import boto3

athena = boto3.client('athena', region_name='us-east-1')

response = athena.start_query_execution(
    QueryString='SELECT * FROM coingecko.coingecko_raw LIMIT 10',
    QueryExecutionContext={'Database': 'coingecko'},
    ResultConfiguration={
        'OutputLocation': 's3://bucket/athena-results/'
    }
)

query_execution_id = response['QueryExecutionId']

# Wait for completion
waiter = athena.get_waiter('query_succeeded')
waiter.wait(QueryExecutionId=query_execution_id)

# Get results
results = athena.get_query_results(QueryExecutionId=query_execution_id)
```

### Aurora Data API

Query Aurora via Data API (serverless):

```python
import boto3

rds_data = boto3.client('rds-data', region_name='us-east-1')

response = rds_data.execute_statement(
    resourceArn='arn:aws:rds:us-east-1:123456789012:cluster:kb-pg-cluster',
    secretArn='arn:aws:secretsmanager:us-east-1:123456789012:secret:...',
    database='kbdb',
    sql='SELECT COUNT(*) FROM public.research_kb'
)

print(response['records'])
```

### Bedrock Knowledge Base

Query Knowledge Base:

```python
import boto3

bedrock_agent = boto3.client('bedrock-agent-runtime', region_name='us-east-1')

response = bedrock_agent.retrieve(
    knowledgeBaseId='KB123456',
    retrievalQuery={
        'text': 'Bitcoin price analysis'
    },
    retrievalConfiguration={
        'vectorSearchConfiguration': {
            'numberOfResults': 10
        }
    }
)

for result in response['retrievalResults']:
    print(result['content']['text'])
    print(result['score'])
```

---

## Error Codes

### HTTP Status Codes

- `200 OK`: Successful request
- `400 Bad Request`: Invalid input
- `401 Unauthorized`: Missing or invalid authentication
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Service temporarily unavailable

### Application Error Codes

```json
{
  "error": {
    "code": "AGENT_TIMEOUT",
    "message": "Agent execution timed out after 60 seconds",
    "details": {
      "agent": "query_agent",
      "duration_ms": 60000
    }
  }
}
```

**Error Codes**:
- `AGENT_TIMEOUT`: Agent execution exceeded timeout
- `TOOL_FAILURE`: Tool execution failed
- `LLM_ERROR`: LLM API error
- `INVALID_INPUT`: Invalid request parameters
- `RESOURCE_NOT_FOUND`: Requested resource not found
- `RATE_LIMIT_EXCEEDED`: Too many requests

---

## Rate Limits

### Development

- No rate limits

### Production

- 100 requests per minute per user
- 1000 requests per hour per user
- Burst: 10 requests per second

**Headers**:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1705234567
```

---

## Webhooks

### Event Types

Subscribe to events via webhooks:

- `ingestion.completed`: Data ingestion finished
- `agent.response`: Agent generated response
- `error.occurred`: Error in pipeline

### Webhook Payload

```json
{
  "event": "ingestion.completed",
  "timestamp": "2025-01-15T10:30:00Z",
  "data": {
    "source": "news_agent",
    "records_processed": 42,
    "duration_ms": 15000
  }
}
```

### Webhook Configuration

```bash
# Register webhook
curl -X POST https://api.example.com/webhooks \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "url": "https://your-app.com/webhook",
    "events": ["ingestion.completed"],
    "secret": "your-webhook-secret"
  }'
```

---

## SDK Examples

### Python SDK

```python
from crypto_intelligence import CryptoAgent

# Initialize client
agent = CryptoAgent(
    base_url="http://localhost:3000",
    api_key="your-api-key"  # Optional
)

# Simple query
response = agent.query("What is Bitcoin price?")
print(response.text)

# With session
session = agent.create_session(user_id="user-123")
response1 = session.query("Show me Bitcoin trends")
response2 = session.query("What about Ethereum?")  # Maintains context

# Set preferences
session.set_feedback("always respond in Spanish")
response3 = session.query("Tell me about Solana")  # Response in Spanish
```

### JavaScript SDK

```javascript
import { CryptoAgent } from '@crypto-intelligence/sdk';

// Initialize client
const agent = new CryptoAgent({
  baseUrl: 'http://localhost:3000',
  apiKey: 'your-api-key'  // Optional
});

// Simple query
const response = await agent.query('What is Bitcoin price?');
console.log(response.text);

// With session
const session = agent.createSession({ userId: 'user-123' });
const response1 = await session.query('Show me Bitcoin trends');
const response2 = await session.query('What about Ethereum?');

// Set preferences
await session.setFeedback('always respond in Spanish');
const response3 = await session.query('Tell me about Solana');
```

---

## GraphQL API (Future)

Planned GraphQL API for flexible queries:

```graphql
query {
  bitcoin {
    currentPrice
    marketCap
    priceHistory(days: 7) {
      date
      price
      volume
    }
  }
  
  news(limit: 10, tokens: ["BTC", "ETH"]) {
    id
    title
    date
    tokens {
      symbol
      sentiment
    }
  }
}
```

---

## API Versioning

- Current version: v1
- Version specified in URL: `/v1/chat/completions`
- Breaking changes will increment version
- Old versions supported for 6 months after deprecation

---

## Support

For API support:
- Documentation: https://docs.example.com
- Issues: https://github.com/org/repo/issues
- Email: api-support@example.com
