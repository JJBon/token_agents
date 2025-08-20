# ========= History feature fetchers =========
from tools.market_tools.iceberg_admin import ensure_insights_table, parse_array_literal
from tools.market_tools.athena_client import query, rows as athena_rows
from tools.market_tools.config import GLUE_DATABASE
from typing import List, Optional, Dict, Any, Tuple, Literal
from pydantic import BaseModel, Field, ValidationError



class HistoryFeatures(BaseModel):
    last_runs: List[Dict[str, Any]]
    asset_trends_30d: List[Dict[str, Any]]


def fetch_history_features(days_back: int = 30) -> HistoryFeatures:
    """Return rolling stats + per-asset stance counts to ground the LLM."""
    qid1 = query(f"""
    SELECT
      run_ts,
      net_score,
      AVG(net_score) OVER (ORDER BY run_ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS net_7dma,
      net_score - LAG(net_score) OVER (ORDER BY run_ts)                               AS net_delta
    FROM {GLUE_DATABASE}.market_summaries
    WHERE run_ts >= current_timestamp - INTERVAL '{int(days_back)}' DAY
    ORDER BY run_ts DESC
    LIMIT 30
    """)
    rows1 = athena_rows(qid1)
    last_runs = []
    for r in rows1:
        run_ts, net, dma7, d = r
        last_runs.append({
            "run_ts": run_ts, "net_score": float(net) if net else None,
            "net_7dma": float(dma7) if dma7 else None, "net_delta": float(d) if d else None
        })

    qid2 = query(f"""
    WITH exploded AS (
      SELECT
        run_ts,
        trim(split_part(x, ',', 1)) AS sym,
        trim(split_part(x, ',', 2)) AS name,
        trim(split_part(x, ',', 3)) AS stance
      FROM {GLUE_DATABASE}.market_summaries
      CROSS JOIN UNNEST(top_assets) AS t(x)
    )
    SELECT sym, name,
      SUM(CASE WHEN stance='bullish' THEN 1 ELSE 0 END) AS bull_ct,
      SUM(CASE WHEN stance='bearish' THEN 1 ELSE 0 END) AS bear_ct,
      SUM(CASE WHEN stance='neutral' THEN 1 ELSE 0 END) AS neu_ct
    FROM exploded
    WHERE run_ts >= current_timestamp - INTERVAL '30' DAY
    GROUP BY 1,2
    ORDER BY bull_ct DESC NULLS LAST
    """)
    rows2 = athena_rows(qid2)
    asset_trends = []
    for r in rows2:
        sym, name, b, br, n = r
        asset_trends.append({"sym": sym, "name": name, "bull_ct": int(b or 0), "bear_ct": int(br or 0), "neu_ct": int(n or 0)})

    return HistoryFeatures(last_runs=last_runs, asset_trends_30d=asset_trends)

# ========= Recent insights retrieval =========

def fetch_recent_insights(tags: Optional[List[str]] = None, limit: int = 20) -> List[Dict[str, Any]]:
    ensure_insights_table()
    tag_filter = ""
    if tags:
        tag_list = ",".join("'{}'".format(t.replace("'", "''")) for t in tags)
        tag_filter = f"WHERE cardinality(array_intersect(tags, ARRAY[{tag_list}])) > 0"
    qid = query(f"""
      SELECT created_at, title, thesis, tags, evidence_refs, confidence, valid_until
      FROM {GLUE_DATABASE}.insights
      {tag_filter}
      ORDER BY created_at DESC
      LIMIT {int(limit)}
    """)
    rows = athena_rows(qid)
    out = []
    for r in rows:
        ca, title, thesis, tags_s, refs_s, conf, vu = r
        out.append({
            "created_at": ca,
            "title": title,
            "thesis": thesis,
            "tags": parse_array_literal(tags_s),
            "evidence_refs": parse_array_literal(refs_s),
            "confidence": float(conf) if conf else None,
            "valid_until": vu,
        })
    return out