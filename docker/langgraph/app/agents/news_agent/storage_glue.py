# storage_glue.py
import os, time, json, hashlib, io
import boto3
from typing import Dict, List, Tuple, Iterable
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "news_agent")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "news_agent")
TBL_NEWS = os.environ.get("ICEBERG_TABLE_NEWS", "crpytoapi_news")
TBL_REASONS = os.environ.get("ICEBERG_TABLE_REASONS", "table_reasons")
ATHENA_OUT = os.environ["ATHENA_OUTPUT_S3"]  # e.g. s3://my-athena-results/

# ---- clients
s3 = boto3.client("s3", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

# ---- helpers
def _s3_uri_with_trailing_slash(uri: str) -> str:
    uri = uri.strip()
    if not uri.endswith("/"):
        uri += "/"
    return uri

ATHENA_OUT = _s3_uri_with_trailing_slash(ATHENA_OUT)

def _sanitize_location(loc: str) -> str:
    return loc if loc.endswith("/") else (loc + "/")

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _athena_query(sql: str) -> str:
    # start
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
        WorkGroup="primary",
    )
    qid = q["QueryExecutionId"]
    # wait
    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state in ("FAILED", "CANCELLED"):
            reason = r["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena FAILED: {reason}\nSQL:\n{sql}")
        if state == "SUCCEEDED":
            return qid
        time.sleep(0.5)

def _athena_table_exists(db: str, table: str) -> bool:
    try:
        glue.get_table(DatabaseName=db, Name=table)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("EntityNotFoundException", "TableNotFoundException"):
            return False
        raise

def _create_iceberg_table_direct(db: str, table: str, location: str, columns_sql: str) -> None:
    """
    Plain CREATE TABLE for Iceberg (no CTAS). Use 'string' (not varchar).
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {db}.{table} (
      {columns_sql}
    )
    LOCATION '{_sanitize_location(location)}'
    TBLPROPERTIES ('table_type'='ICEBERG')
    """
    _athena_query(ddl)

def ensure_iceberg_tables() -> None:
    # database
    _athena_query(f"CREATE DATABASE IF NOT EXISTS {GLUE_DATABASE}")

    news_loc    = f"s3://{S3_BUCKET}/{S3_PREFIX}/iceberg/news/"
    reasons_loc = f"s3://{S3_BUCKET}/{S3_PREFIX}/iceberg/reasons/"

    # Use 'string' for Athena compatibility.
    news_columns_sql = ",".join([
        "news_id string",
        "news_url string",
        "title string",
        "source_name string",
        "published_at timestamp",
        "sentiment string",
        "api_payload string",
        "currencies string",
        "first_seen_at timestamp",
        "last_seen_at timestamp",
    ])

    reasons_columns_sql = ",".join([
        "news_id string",
        "extractor_model string",
        "temperature double",
        "prompt_version string",
        "evidence string",
        "created_at timestamp",
    ])

    # Create both tables if missing
    if not _athena_table_exists(GLUE_DATABASE, TBL_NEWS):
        _create_iceberg_table_direct(GLUE_DATABASE, TBL_NEWS, news_loc, news_columns_sql)
    if not _athena_table_exists(GLUE_DATABASE, TBL_REASONS):
        _create_iceberg_table_direct(GLUE_DATABASE, TBL_REASONS, reasons_loc, reasons_columns_sql)

def existing_ids(ids: Iterable[str]) -> set:
    ids = list({x for x in ids if x})
    if not ids:
        return set()
    out = set()
    # IN-chunking
    for i in range(0, len(ids), 500):
        chunk = ids[i:i+500]
        in_list = ",".join(f"'{x}'" for x in chunk)
        qid = _athena_query(
            f"SELECT news_id FROM {GLUE_DATABASE}.{TBL_NEWS} WHERE news_id IN ({in_list})"
        )
        # page through results
        next_token = None
        first = True
        while True:
            res = athena.get_query_results(QueryExecutionId=qid, NextToken=next_token) if next_token \
                  else athena.get_query_results(QueryExecutionId=qid)
            rows = res.get("ResultSet", {}).get("Rows", [])
            if first and rows:
                rows = rows[1:]  # skip header row
                first = False
            for row in rows:
                cells = row.get("Data", [])
                if not cells:
                    continue
                val = cells[0].get("VarCharValue")
                if val:
                    out.add(val)
            next_token = res.get("NextToken")
            if not next_token:
                break
    return out

def write_run_to_s3_jsonl(items: List[Dict], reasons: List[Dict]) -> Tuple[str, str]:
    # Append-only bronze layer
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

def stage_to_iceberg(enriched: List[Dict], meta_rows: List[Dict]) -> None:
    """
    Upsert into Iceberg using MERGE.
    We stage JSONL to S3, expose with JSON SerDe external tables, then MERGE.
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

    # JSONL external tables for staging
    _athena_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DATABASE}.{tmp_news_tbl} (
      news_id        string,
      news_url       string,
      title          string,
      source_name    string,
      published_at   string,
      sentiment      string,
      api_payload    string,
      currencies     string
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
    LOCATION '{news_loc}'
    """)

    _athena_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DATABASE}.{tmp_rsn_tbl} (
      news_id          string,
      extractor_model  string,
      temperature      double,
      prompt_version   string,
      evidence         string
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
    LOCATION '{rsn_loc}'
    """)

    try:
        # NEWS MERGE
        _athena_query(f"""
        MERGE INTO {GLUE_DATABASE}.{TBL_NEWS} AS t
        USING (
          SELECT
            news_id,
            news_url,
            title,
            source_name,
            try_cast(from_iso8601_timestamp(published_at) AS timestamp) AS published_at,
            sentiment,
            api_payload,
            currencies,
            current_timestamp AS first_seen_at,
            current_timestamp AS last_seen_at
          FROM {GLUE_DATABASE}.{tmp_news_tbl}
        ) AS s
        ON (t.news_id = s.news_id)
        WHEN MATCHED THEN UPDATE SET
          news_url     = s.news_url,
          title        = s.title,
          source_name  = s.source_name,
          published_at = s.published_at,
          sentiment    = s.sentiment,
          api_payload  = s.api_payload,
          currencies   = s.currencies,
          last_seen_at = current_timestamp
        WHEN NOT MATCHED THEN INSERT (
          news_id, news_url, title, source_name, published_at, sentiment,
          api_payload, currencies, first_seen_at, last_seen_at
        ) VALUES (
          s.news_id, s.news_url, s.title, s.source_name, s.published_at, s.sentiment,
          s.api_payload, s.currencies, current_timestamp, current_timestamp
        )
        """)

        # REASONS MERGE
        _athena_query(f"""
        MERGE INTO {GLUE_DATABASE}.{TBL_REASONS} AS t
        USING (
          SELECT
            news_id,
            extractor_model,
            CAST(temperature AS double) AS temperature,
            prompt_version,
            evidence,
            current_timestamp AS created_at
          FROM {GLUE_DATABASE}.{tmp_rsn_tbl}
        ) AS s
        ON (t.news_id = s.news_id)
        WHEN MATCHED THEN UPDATE SET
          extractor_model = s.extractor_model,
          temperature     = s.temperature,
          prompt_version  = s.prompt_version,
          evidence        = s.evidence,
          created_at      = current_timestamp
        WHEN NOT MATCHED THEN INSERT (
          news_id, extractor_model, temperature, prompt_version, evidence, created_at
        ) VALUES (
          s.news_id, s.extractor_model, s.temperature, s.prompt_version, s.evidence, current_timestamp
        )
        """)
    finally:
        # cleanup staging tables
        _athena_query(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{tmp_news_tbl}")
        _athena_query(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{tmp_rsn_tbl}")
