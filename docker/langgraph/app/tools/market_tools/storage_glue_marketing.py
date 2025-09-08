import os, time, json, io, textwrap, re, boto3
from typing import Dict, List, Tuple, Optional

from tools.athena_client import (
    query, query_global, rows, table_exists, column_exists
)

AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET   = os.environ["S3_BUCKET"]
# separate prefix/db for marketing (you can override via env)
MKT_DB      = os.getenv("MARKETING_GLUE_DATABASE", "marketing")
MKT_PREFIX  = os.getenv("MARKETING_S3_PREFIX", "marketing")
BRONZE_BASE = os.getenv("MARKETING_BRONZE_BASE", f"s3://{S3_BUCKET}/{MKT_PREFIX}")

TBL_RDOCS   = os.getenv("ICEBERG_TABLE_RESEARCH_DOCS",   "research_docs")
TBL_RCHUNKS = os.getenv("ICEBERG_TABLE_RESEARCH_CHUNKS", "research_chunks")
TBL_BRIEFS  = os.getenv("ICEBERG_TABLE_MARKETING_BRIEFS","marketing_briefs")

s3 = boto3.client("s3", region_name=AWS_REGION)

# ---------------- helpers ----------------
def _with_trailing_slash(uri: str) -> str:
    return uri if uri.endswith("/") else uri + "/"

def _esc(s: Optional[str]) -> str:
    if s is None: return ""
    return s.replace("'", "''")

def _create_iceberg_table(*, table: str, columns_sql: str, location: Optional[str], file_format: str = "PARQUET") -> None:
    cols = textwrap.dedent(columns_sql).strip().rstrip(",")
    loc  = f"LOCATION '{location}'" if location else ""
    ddl = f"""CREATE TABLE IF NOT EXISTS {table} (
{cols}
)
{loc}
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='{file_format.lower()}'
)
"""
    query(ddl)

def _create_json_stage_table(table_name: str, columns_sql: str, location: str) -> None:
    cols = textwrap.dedent(columns_sql).strip()
    cols = re.sub(r'--.*$', '', cols, flags=re.M)
    cols = re.sub(r'/\*.*?\*/', '', cols, flags=re.S)
    cols = re.sub(r',\s*\)$', ')', cols, flags=re.M)
    ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {MKT_DB}.{table_name} (
      {cols}
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
    LOCATION '{location}'
    """
    query(ddl)

def _jsonl_bytes(rows_: List[Dict]) -> bytes:
    buf = io.StringIO()
    for r in rows_:
        buf.write(json.dumps(r, ensure_ascii=False))
        buf.write("\n")
    return buf.getvalue().encode("utf-8")

# ---------------- ensure tables ----------------
def ensure_marketing_tables() -> None:
    # 1) DB
    query_global(f"CREATE DATABASE IF NOT EXISTS {MKT_DB}")

    # 2) locations
    rdocs_loc, rchunks_loc, briefs_loc = None, None, None
    if BRONZE_BASE:
        base = _with_trailing_slash(BRONZE_BASE.rstrip("/"))
        rdocs_loc   = _with_trailing_slash(f"{base}research_docs")
        rchunks_loc = _with_trailing_slash(f"{base}research_chunks")
        briefs_loc  = _with_trailing_slash(f"{base}marketing_briefs")

    # 3) research_docs (typed)
    if not table_exists(MKT_DB, TBL_RDOCS):
        _create_iceberg_table(
            table=f"{MKT_DB}.{TBL_RDOCS}",
            location=rdocs_loc,
            columns_sql="""
              doc_id          string,
              source_s3       string,
              title           string,
              authors         array<string>,
              published_at    timestamp,
              content_hash    string,
              pages           int,
              chunks_count    int,
              topics          array<string>,
              keywords        array<string>,
              last_indexed_at timestamp
            """,
        )

    # 4) research_chunks (typed)
    if not table_exists(MKT_DB, TBL_RCHUNKS):
        _create_iceberg_table(
            table=f"{MKT_DB}.{TBL_RCHUNKS}",
            location=rchunks_loc,
            columns_sql="""
              doc_id     string,
              chunk_hash string,
              ord        int,
              text       string,
              tokens     int
            """,
        )

    # 5) marketing_briefs (typed, includes array<struct>)
    if not table_exists(MKT_DB, TBL_BRIEFS):
        _create_iceberg_table(
            table=f"{MKT_DB}.{TBL_BRIEFS}",
            location=briefs_loc,
            columns_sql="""
              brief_id        string,
              doc_id          string,
              created_at      timestamp,
              summary         string,
              highlights      array<string>,
              news_hits       array<struct<
                news_id:string,
                headline:string,
                url:string,
                published_at:timestamp,
                score:double,
                symbols:array<string>
              >>,
              recommendations array<string>
            """,
        )

# ---------------- persists ----------------
def persist_research_doc(meta: Dict) -> None:
    """
    Upsert single research doc row into marketing.research_docs (typed arrays via json_parse).
    Expected keys: doc_id, source_s3, title, authors[], topics[], keywords[], content_hash, pages, chunks_count, published_at (ISO or None)
    """
    doc_id       = _esc(meta.get("doc_id"))
    source_s3    = _esc(meta.get("source_s3"))
    title        = _esc(meta.get("title") or "")
    authors_json = _esc(json.dumps(meta.get("authors", []), ensure_ascii=False))
    topics_json  = _esc(json.dumps(meta.get("topics",  []), ensure_ascii=False))
    keys_json    = _esc(json.dumps(meta.get("keywords",[]), ensure_ascii=False))
    content_hash = _esc(meta.get("content_hash") or "")
    pages        = meta.get("pages")
    chunks_cnt   = meta.get("chunks_count", 0)
    published_iso= meta.get("published_at")  # optional ISO string

    published_sql = (
        f"CAST(from_iso8601_timestamp('{_esc(published_iso)}') AT TIME ZONE 'UTC' AS timestamp(6))"
        if published_iso else "NULL"
    )
    pages_sql = "NULL" if pages is None else str(int(pages))
    chunks_sql = str(int(chunks_cnt))

    sql = f"""
    MERGE INTO {MKT_DB}.{TBL_RDOCS} t
    USING (
      SELECT
        '{doc_id}' AS doc_id,
        '{source_s3}' AS source_s3,
        '{title}' AS title,
        CAST(json_parse('{authors_json}') AS ARRAY(VARCHAR)) AS authors,
        {published_sql} AS published_at,
        '{content_hash}' AS content_hash,
        {pages_sql} AS pages,
        {chunks_sql} AS chunks_count,
        CAST(json_parse('{topics_json}')  AS ARRAY(VARCHAR)) AS topics,
        CAST(json_parse('{keys_json}')    AS ARRAY(VARCHAR)) AS keywords,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) AS last_indexed_at
    ) s
    ON (t.doc_id = s.doc_id)
    WHEN MATCHED THEN UPDATE SET
      source_s3       = s.source_s3,
      title           = s.title,
      authors         = s.authors,
      published_at    = s.published_at,
      content_hash    = s.content_hash,
      pages           = s.pages,
      chunks_count    = s.chunks_count,
      topics          = s.topics,
      keywords        = s.keywords,
      last_indexed_at = s.last_indexed_at
    WHEN NOT MATCHED THEN INSERT (
      doc_id, source_s3, title, authors, published_at, content_hash, pages, chunks_count, topics, keywords, last_indexed_at
    ) VALUES (
      s.doc_id, s.source_s3, s.title, s.authors, s.published_at, s.content_hash, s.pages, s.chunks_count, s.topics, s.keywords, s.last_indexed_at
    )
    """
    query(sql)

def persist_research_chunks(chunks: List[Dict]) -> None:
    """
    Upsert chunk rows (unique on (doc_id, chunk_hash)) via staging JSONL + MERGE.
    Expected keys per item: doc_id, chunk_hash, ord, text, tokens (optional)
    """
    if not chunks:
        return

    run_id = str(int(time.time()))
    stage_prefix = f"{MKT_PREFIX}/staging/research_chunks/run_id={run_id}/"
    key = stage_prefix + "part-00000.jsonl"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=_jsonl_bytes(chunks))
    loc = f"s3://{S3_BUCKET}/{stage_prefix}"
    tmp_tbl = f"tmp_stage_rchunks_{run_id}"

    _create_json_stage_table(
        tmp_tbl,
        columns_sql="""
          doc_id     string,
          chunk_hash string,
          ord        int,
          text       string,
          tokens     int
        """,
        location=loc,
    )

    try:
        sql = f"""
        MERGE INTO {MKT_DB}.{TBL_RCHUNKS} t
        USING (
          SELECT
            doc_id,
            chunk_hash,
            CAST(ord AS integer)     AS ord,
            text,
            CAST(tokens AS integer)  AS tokens
          FROM {MKT_DB}.{tmp_tbl}
        ) s
        ON (t.doc_id = s.doc_id AND t.chunk_hash = s.chunk_hash)
        WHEN MATCHED THEN UPDATE SET
          ord   = s.ord,
          text  = s.text,
          tokens= s.tokens
        WHEN NOT MATCHED THEN INSERT (
          doc_id, chunk_hash, ord, text, tokens
        ) VALUES (
          s.doc_id, s.chunk_hash, s.ord, s.text, s.tokens
        )
        """
        query(sql)
    finally:
        query(f"DROP TABLE IF EXISTS {MKT_DB}.{tmp_tbl}")

def persist_marketing_brief(doc_id: str, brief: Dict, news_hits: List[Dict]) -> str:
    """
    Upsert a brief with typed news_hits array<struct<...>>.
    brief expects: summary:str, highlights:[str], recommendations:[str]
    news_hits expects each: {news_id, headline, url, published_at(ISO or ts), score, symbols:[str]}
    """
    brief_id = f"br_{int(time.time())}_{abs(hash(doc_id))%10**6}"

    # build JSON strings, cast with json_parse in SQL
    summary        = _esc(brief.get("summary", ""))
    highlights_js  = _esc(json.dumps(brief.get("highlights", []), ensure_ascii=False))
    recs_js        = _esc(json.dumps(brief.get("recommendations", []), ensure_ascii=False))

    # news_hits -> JSON list of dicts with normalized ts strings
    nh_norm = []
    for n in (news_hits or []):
        ts = n.get("published_at")
        if isinstance(ts, str):
            ts_iso = ts
        elif ts is None:
            ts_iso = None
        else:
            # if a datetime object leaked in
            ts_iso = getattr(ts, "isoformat", lambda: str(ts))()
        nh_norm.append({
            "news_id": n.get("news_id"),
            "headline": n.get("headline") or n.get("title"),
            "url": n.get("url") or n.get("news_url"),
            "published_at": ts_iso,
            "score": float(n.get("score", 1.0) or 1.0),
            "symbols": n.get("symbols", []),
        })
    nh_js = _esc(json.dumps(nh_norm, ensure_ascii=False))

    sql = f"""
    MERGE INTO {MKT_DB}.{TBL_BRIEFS} t
    USING (
      SELECT
        '{_esc(doc_id)}' AS doc_id,
        '{_esc(brief_id)}' AS brief_id,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) AS created_at,
        '{summary}' AS summary,
        CAST(json_parse('{highlights_js}') AS ARRAY(VARCHAR)) AS highlights,
        CAST(
          TRANSFORM(
            CAST(json_parse('{nh_js}') AS ARRAY(ROW(
              news_id VARCHAR,
              headline VARCHAR,
              url VARCHAR,
              published_at VARCHAR,
              score DOUBLE,
              symbols ARRAY(VARCHAR)
            ))),
            x -> CAST(ROW(
              x.news_id,
              x.headline,
              x.url,
              CASE WHEN x.published_at IS NULL THEN NULL
                   ELSE CAST(from_iso8601_timestamp(x.published_at) AT TIME ZONE 'UTC' AS timestamp(6))
              END,
              x.score,
              x.symbols
            ) AS ROW(
              news_id VARCHAR,
              headline VARCHAR,
              url VARCHAR,
              published_at TIMESTAMP,
              score DOUBLE,
              symbols ARRAY(VARCHAR)
            ))
          )
          AS ARRAY(ROW(
            news_id VARCHAR,
            headline VARCHAR,
            url VARCHAR,
            published_at TIMESTAMP,
            score DOUBLE,
            symbols ARRAY(VARCHAR)
          ))
        ) AS news_hits,
        CAST(json_parse('{recs_js}') AS ARRAY(VARCHAR)) AS recommendations
    ) s
    ON (t.brief_id = s.brief_id)
    WHEN MATCHED THEN UPDATE SET
      doc_id         = s.doc_id,
      created_at     = s.created_at,
      summary        = s.summary,
      highlights     = s.highlights,
      news_hits      = s.news_hits,
      recommendations= s.recommendations
    WHEN NOT MATCHED THEN INSERT (
      brief_id, doc_id, created_at, summary, highlights, news_hits, recommendations
    ) VALUES (
      s.brief_id, s.doc_id, s.created_at, s.summary, s.highlights, s.news_hits, s.recommendations
    )
    """
    query(sql)
    return brief_id
