# Data Pipeline Guide

## Overview

The data pipeline ingests cryptocurrency market data and news from external sources, processes and enriches it, and stores it in multiple formats optimized for different query patterns.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                  │
├─────────────────────────────────────────────────────────────────────┤
│  CoinGecko API    │  CryptoNews API    │  Research Papers (S3)     │
└────────┬──────────┴──────────┬─────────┴──────────┬─────────────────┘
         │                     │                     │
         │                     │                     │
┌────────▼─────────────────────▼─────────────────────▼─────────────────┐
│                        Ingestion Layer                                │
├───────────────────────────────────────────────────────────────────────┤
│  Lambda (Scheduled)   │  ECS Fargate (News)  │  Manual Upload       │
└────────┬──────────────┴──────────┬─────────────────┬─────────────────┘
         │                         │                 │
         │                         │                 │
┌────────▼─────────────────────────▼─────────────────▼─────────────────┐
│                         Raw Storage                                   │
├───────────────────────────────────────────────────────────────────────┤
│  S3 (Parquet)         │  S3 (JSON/Text)      │  S3 (PDF/Docs)       │
└────────┬──────────────┴──────────┬─────────────────┬─────────────────┘
         │                         │                 │
         │                         │                 │
┌────────▼─────────────────────────▼─────────────────▼─────────────────┐
│                      Catalog & Processing                             │
├───────────────────────────────────────────────────────────────────────┤
│  Glue Catalog         │  Spark + dbt         │  Bedrock KB          │
│  (Metadata)           │  (Transformations)   │  (Embeddings)        │
└────────┬──────────────┴──────────┬─────────────────┬─────────────────┘
         │                         │                 │
         │                         │                 │
┌────────▼─────────────────────────▼─────────────────▼─────────────────┐
│                      Serving Layer                                    │
├───────────────────────────────────────────────────────────────────────┤
│  Athena (SQL)         │  Iceberg Tables      │  Aurora pgvector     │
│                       │  (Time Travel)       │  (Semantic Search)   │
└───────────────────────────────────────────────────────────────────────┘
```

## Data Sources

### 1. CoinGecko API

**Purpose**: Real-time cryptocurrency market data

**Endpoint**: `/api/v3/coins/markets`

**Data Collected**:
- Current price (USD)
- Market capitalization
- Trading volume (24h)
- Circulating supply
- Total supply
- Price changes (1h, 24h, 7d)
- All-time high/low (ATH/ATL)
- ROI (if available)

**Schedule**: Daily at 23:55 UTC (EventBridge)

**Rate Limits**: 
- Free tier: 10-50 calls/minute
- Pro tier: Higher limits
- Implemented: Exponential backoff on 429 errors

**Schema**:
```python
{
    "name": str,                                      # e.g., "Bitcoin"
    "current_price": float,                           # USD price
    "market_cap": int,                                # Total market cap
    "circulating_supply": float,                      # Coins in circulation
    "total_supply": float,                            # Max supply
    "last_updated": timestamp,                        # API timestamp
    "ath": float,                                     # All-time high
    "atl": float,                                     # All-time low
    "price_change_percentage_1h_in_currency": float,
    "price_change_percentage_24h_in_currency": float,
    "price_change_percentage_7d_in_currency": float,
    "total_volume": float,                            # 24h volume
    "high_24h": float,
    "low_24h": float,
    "roi": float,                                     # Return on investment
    "inserted_at": timestamp,                         # Our ingestion time
    "ds": str                                         # Partition key (YYYY-MM-DD)
}
```

### 2. CryptoNews API

**Purpose**: Cryptocurrency news articles

**Endpoint**: `/api/v1/category?section=general`

**Data Collected**:
- Article title
- Source name
- Publication date
- Article URL
- Summary text
- Image URL
- Tags/categories

**Schedule**: Daily at 06:00 America/Bogota (Step Functions)

**Enrichment**:
- Full article text (scraped)
- Token mentions (LLM extracted)
- Sentiment analysis
- Currency symbols

**Schema**:
```python
{
    "news_id": str,              # Unique identifier
    "title": str,                # Article headline
    "text": str,                 # Summary from API
    "full_text": str,            # Scraped full article
    "source_name": str,          # Publisher
    "date": timestamp,           # Publication date
    "news_url": str,             # Article URL
    "image_url": str,            # Featured image
    "tags": List[str],           # Categories
    "currencies": List[str],     # Extracted token symbols
    "sentiment": str,            # positive/negative/neutral
    "inserted_at": timestamp,    # Ingestion time
    "extractor_temperature": float  # LLM temperature used
}
```

### 3. Research Papers

**Purpose**: Long-form cryptocurrency research and analysis

**Source**: Manual upload to S3

**Formats**: PDF, DOCX, TXT, Markdown

**Processing**:
- Text extraction
- Chunking (configurable size)
- Embedding generation (Titan)
- Metadata extraction

**Schema**:
```python
{
    "doc_id": str,               # Filename without extension
    "chunk_id": str,             # research#{doc_id}#{chunk_num}
    "text": str,                 # Chunk content
    "embedding": List[float],    # 1024-dim vector (Titan v2)
    "metadata": {
        "source": str,           # S3 URI
        "page": int,             # Page number (if PDF)
        "chunk_index": int,      # Position in document
        "total_chunks": int
    },
    "custom_metadata": {
        "doc_type": "research",
        "upload_date": timestamp
    }
}
```

## Ingestion Pipelines

### Market Data Pipeline (Lambda)

**Trigger**: EventBridge rule (cron: `55 23 * * ? *`)

**Function**: `coingecko_snapshot_ingest`

**Workflow**:
```python
1. Fetch API key from Secrets Manager
2. Call CoinGecko API with pagination
3. Transform response to schema
4. Add metadata (inserted_at, ds)
5. Convert to Parquet
6. Upload to S3: s3://bucket/raw/YYYY-MM-DD-snapshot.parquet
7. Log metrics (count, size, duration)
```

**Error Handling**:
- Retry on transient failures (3 attempts)
- Alert on persistent failures (SNS)
- Partial success: Save what we got

**Monitoring**:
- CloudWatch metrics: invocation count, duration, errors
- Custom metrics: records processed, API latency
- Alarms: failure rate > 10%

**Code Location**: `deployment/lambda_ingest_container/app.py`

**Configuration**:
```python
TIMEOUT = 360  # 6 minutes
MEMORY = 512   # MB
ENVIRONMENT = {
    "S3_BUCKET": "...",
    "COINGECKO_API_KEY": "arn:aws:secretsmanager:..."
}
```

### News Pipeline (ECS Fargate)

**Trigger**: Step Functions (cron: `0 6 * * ? *`)

**Task**: `news-ingest-langgraph`

**Workflow**:
```python
1. Ensure Iceberg tables exist
2. Fetch news from API (max 50 articles)
3. Deduplicate against existing news_ids
4. For each new article (parallel):
   a. Scrape full text from URL
   b. Extract token mentions via LLM
   c. Merge with keyword hints
5. Ingest to Bedrock KB (S3 or direct)
6. Persist to bronze layer (S3)
7. Persist to Iceberg table (Athena-queryable)
8. Return metrics
```

**Concurrency**:
- Article scraping: 8 parallel workers (configurable)
- LLM extraction: 3 retries with exponential backoff
- Semaphore to prevent overwhelming services

**Deduplication**:
```sql
-- Check existing news_ids in Iceberg
SELECT news_id FROM news_agent.cryptoapi_news
WHERE news_id IN (...)
```

**LLM Extraction**:
```python
# Prompt template
"""
Extract cryptocurrency token mentions from this article.

Title: {title}
Source: {source}
Body: {body}

For each token mentioned, provide:
- symbol (e.g., BTC)
- name (e.g., Bitcoin)
- context (surrounding sentence)
- sentiment (positive/negative/neutral)
"""

# Output schema
class TokenMention(BaseModel):
    symbol: str
    name: str
    context: str
    sentiment: Literal["positive", "negative", "neutral"]
```

**Ingest Modes**:

1. **S3 Mode** (traditional):
```python
# Upload to S3
s3.put_object(
    Bucket=NEWS_KB_BUCKET,
    Key=f"news/dt={date}/{news_id}.txt",
    Body=full_text
)
s3.put_object(
    Bucket=NEWS_KB_BUCKET,
    Key=f"news/dt={date}/{news_id}.txt.metadata.json",
    Body=json.dumps(metadata)
)

# Start ingestion job
bedrock_agent.start_ingestion_job(
    knowledgeBaseId=NEWS_KB_ID,
    dataSourceId=NEWS_KB_DS_ID
)
```

2. **Direct Mode** (new):
```python
# Direct API call
bedrock_agent.ingest_documents(
    knowledgeBaseId=NEWS_KB_ID,
    documents=[{
        "documentId": news_id,
        "content": {"text": full_text},
        "metadata": {...}
    }]
)
```

**Error Handling**:
- Scraping failures: Log and continue
- LLM failures: Retry 3x, then skip
- Ingestion failures: Collect errors, report at end
- Partial success: Process what we can

**Monitoring**:
- ECS metrics: CPU, memory, task count
- Custom metrics: articles processed, extraction success rate
- Langfuse traces: LLM calls, latency, cost

**Code Location**: `docker/langgraph/app/agents/news_agent/graph.py`

## Data Transformation (dbt + Spark)

### dbt Project Structure

```
dbt/coin_spark/
├── models/
│   ├── sources/
│   │   └── coingecko_raw.sql      # Source definition
│   └── marts/
│       ├── daily_metrics.sql       # Daily aggregations
│       ├── weekly_metrics.sql      # Weekly rollups
│       └── token_performance.sql   # Performance calculations
├── macros/
│   └── aws_s3_auth.sql            # S3 authentication macro
├── dbt_project.yml                # Project configuration
└── profiles.yml                   # Connection profiles
```

### dbt Configuration

**Profile** (`profiles.yml`):
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

**Project** (`dbt_project.yml`):
```yaml
name: coin_spark
version: '1.0.0'
profile: coin_spark

models:
  coin_spark:
    +location_root: 's3://bucket/dbt'
    +file_format: iceberg
    +materialized: table
```

### Spark Integration

**Thrift Server**:
- Exposes Spark SQL via JDBC/ODBC
- Runs on port 10000
- Supports concurrent queries

**S3 Authentication**:
```sql
-- Macro: set_s3a_session_creds()
SET spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID};
SET spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY};
SET spark.hadoop.fs.s3a.session.token=${AWS_SESSION_TOKEN};
```

**Iceberg Configuration**:
```python
spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.warehouse = s3://bucket/dbt
spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
```

### Example Transformations

**Daily Metrics**:
```sql
-- models/marts/daily_metrics.sql
{{ config(
    materialized='incremental',
    unique_key='date_coin',
    file_format='iceberg'
) }}

SELECT
    ds as date,
    name as coin,
    AVG(current_price) as avg_price,
    MAX(high_24h) as high,
    MIN(low_24h) as low,
    SUM(total_volume) as volume,
    MAX(market_cap) as market_cap,
    CONCAT(ds, '_', name) as date_coin
FROM {{ source('coingecko', 'coingecko_raw') }}
{% if is_incremental() %}
WHERE ds > (SELECT MAX(date) FROM {{ this }})
{% endif %}
GROUP BY ds, name
```

**Token Performance**:
```sql
-- models/marts/token_performance.sql
WITH price_changes AS (
    SELECT
        name,
        current_price,
        LAG(current_price, 1) OVER (PARTITION BY name ORDER BY ds) as prev_price,
        LAG(current_price, 7) OVER (PARTITION BY name ORDER BY ds) as price_7d_ago,
        ds
    FROM {{ source('coingecko', 'coingecko_raw') }}
)
SELECT
    name,
    current_price,
    (current_price - prev_price) / prev_price * 100 as daily_change_pct,
    (current_price - price_7d_ago) / price_7d_ago * 100 as weekly_change_pct,
    ds
FROM price_changes
WHERE prev_price IS NOT NULL
```

## Storage Formats

### Parquet (Raw Data)

**Advantages**:
- Columnar format (efficient for analytics)
- Compression (typically 10x smaller than JSON)
- Schema evolution support
- Fast predicate pushdown

**Usage**:
- Lambda output (market snapshots)
- Bronze layer (raw news)

**Example**:
```python
import pyarrow.parquet as pq

# Write
table = pa.Table.from_pandas(df)
pq.write_table(table, 'output.parquet', compression='snappy')

# Read
table = pq.read_table('output.parquet')
df = table.to_pandas()
```

### Iceberg (Processed Data)

**Advantages**:
- ACID transactions
- Time travel (query historical versions)
- Schema evolution (add/remove columns)
- Partition evolution (change partitioning without rewrite)
- Hidden partitioning (automatic partition pruning)

**Usage**:
- dbt model outputs
- News agent persistence
- Queryable via Athena/Spark

**Example**:
```sql
-- Create Iceberg table
CREATE TABLE news_agent.cryptoapi_news (
    news_id STRING,
    title STRING,
    full_text STRING,
    currencies ARRAY<STRING>,
    inserted_at TIMESTAMP,
    ds STRING
)
USING iceberg
PARTITIONED BY (ds)
LOCATION 's3://bucket/iceberg/cryptoapi_news';

-- Time travel
SELECT * FROM news_agent.cryptoapi_news
TIMESTAMP AS OF '2025-01-01 00:00:00';

-- Snapshot query
SELECT * FROM news_agent.cryptoapi_news
VERSION AS OF 12345;
```

### Aurora pgvector (Embeddings)

**Advantages**:
- Native vector similarity search
- ACID transactions
- Hybrid search (vector + keyword)
- Bedrock KB integration

**Usage**:
- Research paper embeddings
- News article embeddings
- Semantic search

**Schema**:
```sql
CREATE TABLE public.research_kb (
    id UUID PRIMARY KEY,
    chunks TEXT,
    embedding VECTOR(1024),  -- Titan v2 dimension
    metadata JSONB,
    custom_metadata JSONB
);

-- HNSW index for fast similarity search
CREATE INDEX research_kb_hnsw
ON public.research_kb
USING hnsw (embedding vector_cosine_ops);

-- GIN index for full-text search
CREATE INDEX research_kb_chunks_tsv_gin
ON public.research_kb
USING gin (to_tsvector('simple', chunks));

-- GIN index for metadata queries
CREATE INDEX research_kb_custom_md_gin
ON public.research_kb
USING gin (custom_metadata);
```

**Query Examples**:
```sql
-- Vector similarity search
SELECT id, chunks, 1 - (embedding <=> query_vector) as similarity
FROM public.research_kb
ORDER BY embedding <=> query_vector
LIMIT 10;

-- Hybrid search (vector + keyword)
SELECT id, chunks,
       1 - (embedding <=> query_vector) as vector_score,
       ts_rank(to_tsvector('simple', chunks), query) as text_score
FROM public.research_kb
WHERE to_tsvector('simple', chunks) @@ query
ORDER BY (vector_score + text_score) DESC
LIMIT 10;

-- Metadata filtering
SELECT * FROM public.research_kb
WHERE custom_metadata @> '{"doc_type": "research"}'::jsonb
  AND custom_metadata->>'upload_date' > '2025-01-01';
```

## Data Quality

### Validation Rules

**Market Data**:
```python
# Schema validation
assert df['current_price'].dtype == float
assert df['market_cap'].dtype == int
assert df['ds'].str.match(r'\d{4}-\d{2}-\d{2}').all()

# Business rules
assert (df['current_price'] > 0).all()
assert (df['market_cap'] >= 0).all()
assert (df['circulating_supply'] <= df['total_supply']).all()

# Completeness
assert df['name'].notna().all()
assert df['current_price'].notna().all()
```

**News Data**:
```python
# Required fields
assert df['news_id'].notna().all()
assert df['title'].notna().all()
assert df['news_url'].notna().all()

# URL validation
assert df['news_url'].str.startswith('http').all()

# Date validation
assert pd.to_datetime(df['date'], errors='coerce').notna().all()

# Deduplication
assert df['news_id'].is_unique
```

### Data Freshness

**Monitoring**:
```sql
-- Check latest data
SELECT MAX(ds) as latest_date,
       COUNT(*) as record_count
FROM coingecko.coingecko_raw;

-- Check for gaps
WITH dates AS (
    SELECT DISTINCT ds FROM coingecko.coingecko_raw
    ORDER BY ds
),
expected AS (
    SELECT DATE_ADD('day', seq, DATE('2025-01-01')) as ds
    FROM UNNEST(SEQUENCE(0, 365)) AS t(seq)
)
SELECT e.ds as missing_date
FROM expected e
LEFT JOIN dates d ON e.ds = d.ds
WHERE d.ds IS NULL
  AND e.ds <= CURRENT_DATE;
```

**Alerts**:
- Data older than 48 hours → Warning
- Data older than 72 hours → Critical
- Missing partitions → Investigation

## Performance Optimization

### Partitioning Strategy

**Time-based** (most common):
```sql
-- Partition by date
PARTITIONED BY (ds STRING)

-- Query with partition filter (fast)
SELECT * FROM table WHERE ds = '2025-01-15';

-- Query without partition filter (slow)
SELECT * FROM table WHERE name = 'Bitcoin';
```

**Compound partitioning**:
```sql
-- Partition by date and category
PARTITIONED BY (ds STRING, category STRING)

-- Efficient query
SELECT * FROM table
WHERE ds = '2025-01-15'
  AND category = 'defi';
```

### Indexing Strategy

**Aurora pgvector**:
```sql
-- HNSW for vector search (fast, approximate)
CREATE INDEX USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- IVFFlat for vector search (slower, exact)
CREATE INDEX USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- GIN for full-text search
CREATE INDEX USING gin (to_tsvector('english', chunks));

-- B-tree for exact matches
CREATE INDEX ON table (news_id);
```

### Query Optimization

**Athena**:
```sql
-- Use partition filters
WHERE ds BETWEEN '2025-01-01' AND '2025-01-31'

-- Use columnar projections
SELECT name, current_price  -- Only read needed columns
FROM table

-- Use approximate functions
SELECT APPROX_DISTINCT(name) FROM table;  -- Faster than COUNT(DISTINCT)
```

**Spark**:
```python
# Cache frequently accessed data
df.cache()

# Broadcast small tables
spark.sql.autoBroadcastJoinThreshold = 10485760  # 10MB

# Partition for parallelism
df.repartition(200, "name")
```

## Backup & Recovery

### S3 Versioning

**Enable versioning**:
```terraform
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.coingecko_data.id
  versioning_configuration {
    status = "Enabled"
  }
}
```

**Lifecycle policies**:
```terraform
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.coingecko_data.id

  rule {
    id     = "archive_old_versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}
```

### Aurora Backups

**Automated backups**:
- Retention: 7 days (configurable)
- Backup window: 03:00-04:00 UTC
- Point-in-time recovery: Any second within retention

**Manual snapshots**:
```bash
aws rds create-db-cluster-snapshot \
  --db-cluster-identifier kb-pg-cluster \
  --db-cluster-snapshot-identifier manual-backup-2025-01-15
```

### Disaster Recovery

**RTO/RPO**:
- RTO (Recovery Time Objective): 4 hours
- RPO (Recovery Point Objective): 24 hours

**Recovery procedures**:
1. Restore Aurora from snapshot
2. Restore S3 data from versioning
3. Rebuild Glue catalog (metadata only)
4. Re-run dbt transformations
5. Validate data integrity

## Cost Optimization

### Storage Costs

**S3 Tiering**:
```
Hot data (< 30 days):     S3 Standard
Warm data (30-90 days):   S3 Intelligent-Tiering
Cold data (> 90 days):    S3 Glacier
Archive (> 1 year):       S3 Glacier Deep Archive
```

**Compression**:
- Parquet with Snappy: ~10x reduction
- Iceberg with Zstd: ~15x reduction

### Compute Costs

**Aurora Serverless**:
- Scales to zero when idle
- Charged per ACU-second
- Dev: 0-4 ACU (~$0.12/hour max)
- Prod: 0.5-16 ACU (~$2/hour max)

**Lambda**:
- Charged per invocation + duration
- 512 MB, 360s timeout
- ~$0.01 per execution

**ECS Fargate**:
- Charged per vCPU-hour + GB-hour
- 0.5 vCPU, 1 GB RAM
- ~$0.03 per hour
- Daily run: ~$1/month

### Query Costs

**Athena**:
- $5 per TB scanned
- Use partitions to reduce scan
- Use columnar formats (Parquet, Iceberg)
- Compress data

**Example savings**:
```
Without partitions: 1 TB scan = $5
With partitions:    10 GB scan = $0.05 (100x cheaper)
```

## Monitoring & Alerts

### Key Metrics

**Ingestion**:
- Records processed per run
- Success/failure rate
- Processing duration
- API latency

**Storage**:
- S3 bucket size
- Aurora storage used
- Partition count
- Table row count

**Query Performance**:
- Athena query duration
- Spark job duration
- Cache hit rate
- Data scanned per query

### CloudWatch Dashboards

**Pipeline Health**:
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Lambda", "Invocations", {"stat": "Sum"}],
          [".", "Errors", {"stat": "Sum"}],
          [".", "Duration", {"stat": "Average"}]
        ],
        "period": 300,
        "stat": "Average",
        "region": "us-east-1",
        "title": "Lambda Metrics"
      }
    }
  ]
}
```

### Alarms

**Critical**:
- Lambda failure rate > 10%
- ECS task failed
- Aurora storage > 90%
- Data freshness > 72 hours

**Warning**:
- Lambda duration > 300s
- Athena query cost > $10/day
- S3 bucket size growth > 50% week-over-week

## Troubleshooting

### Common Issues

**1. Lambda Timeout**
```
Symptom: Function times out at 360s
Cause: API slow or large dataset
Solution: Increase timeout, add pagination, optimize code
```

**2. Iceberg Table Not Found**
```
Symptom: Athena query fails with "table not found"
Cause: Glue catalog out of sync
Solution: Run MSCK REPAIR TABLE or recreate table
```

**3. Vector Search Slow**
```
Symptom: pgvector queries take > 5s
Cause: Missing index or large result set
Solution: Create HNSW index, reduce k, add filters
```

**4. dbt Run Fails**
```
Symptom: dbt run fails with "connection refused"
Cause: Spark Thrift Server not running
Solution: Start Thrift Server, check network connectivity
```

### Debug Commands

```bash
# Check S3 data
aws s3 ls s3://bucket/raw/ --recursive --human-readable

# Query Glue catalog
aws glue get-table --database-name coingecko --table-name coingecko_raw

# Test Aurora connection
psql -h cluster-endpoint -U admin -d kbdb -c "SELECT COUNT(*) FROM public.research_kb;"

# Check Spark Thrift Server
nc -zv spark-master 10000

# View dbt logs
cat dbt/coin_spark/logs/dbt.log
```
