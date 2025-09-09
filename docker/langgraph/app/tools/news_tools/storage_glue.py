# storage_glue.py
import os, time, json, hashlib, io
import boto3
from typing import Dict, List, Tuple

from tools.athena_client import (
    query, query_global, rows, table_exists, column_exists
)

REGION         = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET      = os.environ["S3_BUCKET"]
S3_PREFIX      = os.environ.get("S3_PREFIX", "news_agent")
GLUE_DATABASE  = os.environ.get("GLUE_DATABASE", "news_agent")
TBL_NEWS       = os.environ.get("ICEBERG_TABLE_NEWS", "cryptoapi_news")  # fixed default typo
TBL_REASONS    = os.environ.get("ICEBERG_TABLE_REASONS", "table_reasons")
BRONZE_BASE    = os.environ.get("BRONZE_BASE", f"s3://{S3_BUCKET}/{S3_PREFIX}")

# ---- clients
s3 = boto3.client("s3", region_name=REGION)

# ---- helpers

def _with_trailing_slash(uri: str) -> str:
    return uri if uri.endswith("/") else uri + "/"

def sha256(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

import textwrap

def _create_iceberg_table(*, table: str, columns_sql: str, location: str | None, file_format: str = "PARQUET") -> None:
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
    query(ddl)  # your athena_client.query

def ensure_iceberg_tables() -> None:
    # 1) Ensure database (no DB context)
    query_global(f"CREATE DATABASE IF NOT EXISTS {GLUE_DATABASE}")

    # 2) Locations
    news_loc = None
    reasons_loc = None
    if BRONZE_BASE:
        base = _with_trailing_slash(BRONZE_BASE.rstrip("/"))
        news_loc = _with_trailing_slash(f"{base}news_bronze")
        reasons_loc = _with_trailing_slash(f"{base}reasons_bronze")

    # 3) NEWS (Iceberg) — create if missing with TYPED columns only
    if not table_exists(GLUE_DATABASE, TBL_NEWS):
        columns_sql = """
          news_id        string,
          news_url       string,
          title          string,
          source_name    string,
          published_at   timestamp,
          sentiment      string,
          currencies_arr array<struct<name:string, symbol:string, confidence:double>>,
          api_payload_obj struct<
            news_url:string,
            title:string,
            date_iso:string,
            source:string,
            sentiment:string,
            tags:array<string>
          >,
          first_seen_at  timestamp,
          last_seen_at   timestamp
        """
        _create_iceberg_table(
            table=f"{GLUE_DATABASE}.{TBL_NEWS}",
            columns_sql=columns_sql,
            location=news_loc,
            file_format="PARQUET",
        )
    else:
        # If table exists, add typed columns when missing (Hive DDL types)
        if not column_exists(GLUE_DATABASE, TBL_NEWS, "api_payload_obj"):
            query(f"""
              ALTER TABLE {GLUE_DATABASE}.{TBL_NEWS}
              ADD COLUMNS (
                api_payload_obj struct<
                  news_url:string,
                  title:string,
                  date_iso:string,
                  source:string,
                  sentiment:string,
                  tags:array<string>
                >
              )
            """)
        if not column_exists(GLUE_DATABASE, TBL_NEWS, "currencies_arr"):
            query(f"""
              ALTER TABLE {GLUE_DATABASE}.{TBL_NEWS}
              ADD COLUMNS (
                currencies_arr array<struct<name:string, symbol:string, confidence:double>>
              )
            """)

        # Drop legacy string columns if they still exist
        if column_exists(GLUE_DATABASE, TBL_NEWS, "api_payload"):
            query(f"ALTER TABLE {GLUE_DATABASE}.{TBL_NEWS} DROP COLUMN api_payload")
        if column_exists(GLUE_DATABASE, TBL_NEWS, "currencies"):
            query(f"ALTER TABLE {GLUE_DATABASE}.{TBL_NEWS} DROP COLUMN currencies")

    # 4) REASONS (Iceberg) — simple schema; keep evidence as string for now
    if not table_exists(GLUE_DATABASE, TBL_REASONS):
        reasons_columns_sql = """
          news_id         string,
          extractor_model string,
          temperature     double,
          prompt_version  string,
          evidence        string,
          created_at      timestamp
        """
        _create_iceberg_table(
            table=f"{GLUE_DATABASE}.{TBL_REASONS}",
            columns_sql=reasons_columns_sql,
            location=reasons_loc,
            file_format="PARQUET",
        )


def existing_ids(ids: List[str]) -> List[str]:
    """
    Return subset of ids that already exist in the Iceberg news table.
    """
    if not ids:
        return []
    out: List[str] = []
    for chunk in [ids[i:i+500] for i in range(0, len(ids), 500)]:
        lit = ",".join(f"('{x}')" for x in chunk)
        qid = query(f"""
          WITH incoming(news_id) AS (VALUES {lit})
          SELECT n.news_id
          FROM {GLUE_DATABASE}.{TBL_NEWS} n
          JOIN incoming i ON i.news_id = n.news_id
        """)
        out.extend([r[0] for r in rows(qid)])
    return out

def write_run_to_s3_jsonl(items: List[Dict], reasons: List[Dict]) -> Tuple[str, str]:
    """
    Append-only bronze layer (raw enriched payloads) written as JSONL to S3.
    """
    run_id = str(int(time.time()))
    date_path = time.strftime("%Y-%m-%d")
    news_key = f"{S3_PREFIX}/bronze/news/dt={date_path}/run_id={run_id}.jsonl"
    rsn_key  = f"{S3_PREFIX}/bronze/reasons/dt={date_path}/run_id={run_id}.jsonl"

    def _to_jsonl(objs: List[Dict]) -> bytes:
        buf = io.StringIO()
        for o in objs:
            buf.write(json.dumps(o, ensure_ascii=False) + "\n")
        return buf.getvalue().encode("utf-8")

    s3.put_object(Bucket=S3_BUCKET, Key=news_key, Body=_to_jsonl(items))
    s3.put_object(Bucket=S3_BUCKET, Key=rsn_key,  Body=_to_jsonl(reasons))
    return (f"s3://{S3_BUCKET}/{news_key}", f"s3://{S3_BUCKET}/{rsn_key}")

# helper: create a JSON staging table over S3 (Hive DDL, no WITH(...))
def _create_json_stage_table(table_name: str, columns_sql: str, location: str) -> None:
    ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DATABASE}.{table_name} (
      {columns_sql}
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
    LOCATION '{location}'
    """
    query(ddl)

def stage_to_iceberg(enriched: List[Dict], meta_rows: List[Dict]) -> None:
    """
    Upsert into Iceberg using MERGE.
    - Stage JSONL to S3
    - CREATE EXTERNAL TABLEs over the staged JSON (all strings for nested fields)
    - MERGE into Iceberg tables (parse/cast JSON -> typed columns)
    - DROP staging tables
    """
    run_id = str(int(time.time()))
    stage_news_prefix = f"{S3_PREFIX}/staging/news/run_id={run_id}/"
    stage_rsn_prefix  = f"{S3_PREFIX}/staging/reasons/run_id={run_id}/"

    def _jsonl_bytes(rows: List[Dict]) -> bytes:
        from io import StringIO
        buf = StringIO()
        for r in rows:
            buf.write(json.dumps(r, ensure_ascii=False))
            buf.write("\n")
        return buf.getvalue().encode("utf-8")

    news_key = stage_news_prefix + "part-00000.jsonl"
    rsn_key  = stage_rsn_prefix  + "part-00000.jsonl"
    s3.put_object(Bucket=S3_BUCKET, Key=news_key, Body=_jsonl_bytes(enriched))
    s3.put_object(Bucket=S3_BUCKET, Key=rsn_key,  Body=_jsonl_bytes(meta_rows))

    news_loc = f"s3://{S3_BUCKET}/{stage_news_prefix}"
    rsn_loc  = f"s3://{S3_BUCKET}/{stage_rsn_prefix}"

    tmp_news_tbl = f"tmp_stage_news_{run_id}"
    tmp_rsn_tbl  = f"tmp_stage_reasons_{run_id}"

    # --- staging tables (strings for nested JSON; cast in MERGE) ---
    _create_json_stage_table(
        tmp_news_tbl,
        columns_sql="""
          news_id        string,
          news_url       string,
          title          string,
          source_name    string,
          published_at   string,
          sentiment      string,
          currencies     string,
          api_payload    string
        """,
        location=news_loc,
    )

    _create_json_stage_table(
        tmp_rsn_tbl,
        columns_sql="""
          news_id          string,
          extractor_model  string,
          temperature      double,
          prompt_version   string,
          evidence         string
        """,
        location=rsn_loc,
    )

    try:
        # --- MERGE NEWS: parse/cast JSON strings into typed Iceberg columns ---
        query(f"""
        MERGE INTO {GLUE_DATABASE}.{TBL_NEWS} AS t
        USING (
          SELECT
            news_id,
            news_url,
            title,
            source_name,
            CAST(from_iso8601_timestamp(published_at) AT TIME ZONE 'UTC' AS timestamp(6)) AS published_at,
            sentiment,
            CAST(json_parse(currencies) AS ARRAY(ROW(name VARCHAR, symbol VARCHAR, confidence DOUBLE))) AS currencies_arr,
            CAST(json_parse(api_payload) AS ROW(
              news_url VARCHAR,
              title VARCHAR,
              date_iso VARCHAR,
              source VARCHAR,
              sentiment VARCHAR,
              tags ARRAY(VARCHAR)
            )) AS api_payload_obj,
            CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) AS now_ts
          FROM {GLUE_DATABASE}.{tmp_news_tbl}
        ) AS s
        ON (t.news_id = s.news_id)
        WHEN MATCHED THEN UPDATE SET
          news_url        = s.news_url,
          title           = s.title,
          source_name     = s.source_name,
          published_at    = s.published_at,
          sentiment       = s.sentiment,
          currencies_arr  = s.currencies_arr,
          api_payload_obj = s.api_payload_obj,
          last_seen_at    = s.now_ts
        WHEN NOT MATCHED THEN INSERT (
          news_id, news_url, title, source_name, published_at, sentiment,
          currencies_arr, api_payload_obj,
          first_seen_at, last_seen_at
        ) VALUES (
          s.news_id, s.news_url, s.title, s.source_name, s.published_at, s.sentiment,
          s.currencies_arr, s.api_payload_obj,
          s.now_ts, s.now_ts
        )
        """)

        # --- MERGE REASONS (unchanged; evidence stays string for now) ---
        query(f"""
        MERGE INTO {GLUE_DATABASE}.{TBL_REASONS} AS t
        USING (
          SELECT
            news_id,
            extractor_model,
            CAST(temperature AS double) AS temperature,
            prompt_version,
            CAST(evidence AS VARCHAR) AS evidence,
            CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) AS created_at
          FROM {GLUE_DATABASE}.{tmp_rsn_tbl}
        ) AS s
        ON (t.news_id = s.news_id)
        WHEN MATCHED THEN UPDATE SET
          extractor_model = s.extractor_model,
          temperature     = s.temperature,
          prompt_version  = s.prompt_version,
          evidence        = s.evidence,
          created_at      = s.created_at
        WHEN NOT MATCHED THEN INSERT (
          news_id, extractor_model, temperature, prompt_version, evidence, created_at
        ) VALUES (
          s.news_id, s.extractor_model, s.temperature, s.prompt_version, s.evidence, s.created_at
        )
        """)
    finally:
        query(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{tmp_news_tbl}")
        query(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{tmp_rsn_tbl}")

