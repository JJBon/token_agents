from tools.market_tools.athena_client import query, query_global, table_exists, column_exists
from tools.market_tools.config import GLUE_DATABASE, WAREHOUSE
import json
from typing import List, Optional, Dict, Any, Tuple, Literal

def parse_array_literal(s: Optional[str]) -> List[str]:
    """Parse Athena/Trino array string like:
       ["a","b"]  (JSON)  ✅
       [a, b]     (no quotes) ✅
       ['a','b']  (single quotes) ✅
    into a Python list of strings.
    """
    if not s:
        return []
    s = s.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return []
    # Try strict JSON first
    try:
        return json.loads(s)
    except Exception:
        inner = s[1:-1].strip()
        if not inner:
            return []
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",")]
        return [p for p in parts if p]

def ensure_database():
    query_global(f"CREATE DATABASE IF NOT EXISTS {GLUE_DATABASE}")

# ========= Fact tables: ensure =========



def _sanitize_s3(p: str) -> str:
    return p if p.endswith("/") else (p + "/")

def ensure_summary_table():
    ensure_database()

    if table_exists(GLUE_DATABASE, "market_summaries"):
        # Try to add the JSON column if the table is from an older schema.
        try:
            if not column_exists(GLUE_DATABASE, "market_summaries", "new_insights_json"):
                query(
                    f"ALTER TABLE {GLUE_DATABASE}.market_summaries "
                    f"ADD COLUMN new_insights_json varchar(65535)"
                )
        except Exception:
            pass
        return  # already present; nothing else to do

    wh = _sanitize_s3(WAREHOUSE)
    tbl_loc = _sanitize_s3(f"{wh}{GLUE_DATABASE}/market_summaries")

    query(f"""
    CREATE TABLE IF NOT EXISTS {GLUE_DATABASE}.market_summaries
    WITH (
      table_type = 'ICEBERG',
      location   = '{tbl_loc}',
      is_external = false
    ) AS
    SELECT
      CAST(NULL AS timestamp)             AS run_ts,
      CAST(NULL AS timestamp)             AS window_start,
      CAST(NULL AS timestamp)             AS window_end,
      CAST(NULL AS integer)               AS days,
      CAST(NULL AS integer)               AS sample_size,
      CAST(NULL AS integer)               AS pos,
      CAST(NULL AS integer)               AS neg,
      CAST(NULL AS integer)               AS neu,
      CAST(NULL AS double)                AS net_score,
      CAST(NULL AS varchar(32))           AS outlook,
      CAST(NULL AS double)                AS confidence,
      CAST(NULL AS array(varchar(1024)))  AS drivers,
      CAST(NULL AS array(varchar(1024)))  AS risks,
      CAST(NULL AS array(varchar(1024)))  AS top_assets,
      CAST(NULL AS varchar(65535))        AS narrative,
      CAST(NULL AS varchar(65535))        AS new_insights_json
    WHERE 1=0
    """)


def ensure_insights_table():
    """Managed Iceberg table for atomic insights/hypotheses."""
    ensure_database()
    wh = _sanitize_s3(WAREHOUSE)
    tbl_loc = _sanitize_s3(f"{wh}{GLUE_DATABASE}/insights")

    query(f"""
    CREATE TABLE IF NOT EXISTS {GLUE_DATABASE}.insights (
      insight_id    string,
      created_at    timestamp,
      window_start  timestamp,
      window_end    timestamp,
      title         string,
      thesis        string,
      tags          array<string>,
      evidence_refs array<string>,
      confidence    double,
      valid_until   timestamp,
      parent_ids    array<string>,
      hit           boolean,
      hit_reason    string
    )
    LOCATION '{tbl_loc}'
    TBLPROPERTIES (
      'table_type'='ICEBERG',
      'format'='PARQUET'
    )
    """)