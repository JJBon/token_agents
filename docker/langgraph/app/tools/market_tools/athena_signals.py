# tools/athena_signals.py
from typing import List, Tuple
from datetime import datetime, timedelta, timezone
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from tools.market_tools.config import GLUE_DATABASE, TBL_NEWS
from tools.market_tools.athena_client import query, rows

class FetchSignalsIn(BaseModel):
    days: int = Field(default=3, ge=1, le=30)
    limit_headlines: int = Field(default=40, ge=5, le=200)

class AssetSignal(BaseModel):
    name: str | None = None
    symbol: str | None = None
    mentions: int
    pos: int
    neg: int
    neu: int
    net_score: float

class SignalsOut(BaseModel):
    window_start_utc: str
    window_end_utc: str
    sample_size: int
    pos: int
    neg: int
    neu: int
    net_score: float
    top_sources: List[Tuple[str,int]] = []
    top_assets: List[AssetSignal] = []
    headlines: List[str] = []
    query_ids: List[str] = []

@tool("athena_fetch_signals")
def athena_fetch_signals_tool(days: int = 3, limit_headlines: int = 40) -> SignalsOut:
    """Aggregate the last N days of crypto-news from Athena into sentiment signals.

    Args:
        days: Lookback window in days (1–30).
        limit_headlines: Maximum number of recent headlines to sample (5–200).

    Returns:
        SignalsOut: window start/end (UTC), sample_size, pos/neg/neu counts,
        net_score, top_sources [(source, count)], top_assets (per-asset counts & net),
        and a small list of representative headlines.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    start_iso = start.strftime("%Y-%m-%d %H:%M:%S")
    end_iso   = end.strftime("%Y-%m-%d %H:%M:%S")

    base_filter = f"""
    FROM {GLUE_DATABASE}.{TBL_NEWS}
    WHERE published_at BETWEEN TIMESTAMP '{start_iso}' AND TIMESTAMP '{end_iso}'
    """

    qid_counts = query(f"""
      SELECT
        COUNT(1) AS total,
        SUM(CASE WHEN sentiment='Positive' THEN 1 ELSE 0 END) AS pos,
        SUM(CASE WHEN sentiment='Negative' THEN 1 ELSE 0 END) AS neg,
        SUM(CASE WHEN sentiment NOT IN ('Positive','Negative') THEN 1 ELSE 0 END) AS neu
      {base_filter}
    """)
    r_counts = rows(qid_counts)
    total = int(r_counts[0][0] or 0) if r_counts else 0
    pos   = int(r_counts[0][1] or 0) if r_counts else 0
    neg   = int(r_counts[0][2] or 0) if r_counts else 0
    neu   = int(r_counts[0][3] or 0) if r_counts else 0
    net_score = (pos - neg) / max(1, total)

    qid_sources = query(f"""
      SELECT source_name, COUNT(1) AS c
      {base_filter}
      GROUP BY source_name
      ORDER BY c DESC
      LIMIT 10
    """)
    top_sources = [(r[0] or "unknown", int(r[1] or 0)) for r in rows(qid_sources)]

    qid_assets = query(f"""
    WITH base AS (
      SELECT sentiment, currencies
      FROM {GLUE_DATABASE}.{TBL_NEWS}
      WHERE published_at BETWEEN TIMESTAMP '{start_iso}' AND TIMESTAMP '{end_iso}'
    ),
    exploded AS (
      SELECT
        json_extract_scalar(elem, '$.name')   AS name,
        json_extract_scalar(elem, '$.symbol') AS symbol,
        sentiment
      FROM base
      CROSS JOIN UNNEST(
        CAST(json_parse(currencies) AS array(json))
      ) AS u(elem)
    )
    SELECT
      COALESCE(symbol, '') AS symbol,
      COALESCE(name,   '') AS name,
      COUNT(1) AS mentions,
      SUM(CASE WHEN sentiment='Positive' THEN 1 ELSE 0 END) AS pos,
      SUM(CASE WHEN sentiment='Negative' THEN 1 ELSE 0 END) AS neg,
      SUM(CASE WHEN sentiment NOT IN ('Positive','Negative') THEN 1 ELSE 0 END) AS neu
    FROM exploded
    GROUP BY 1,2
    HAVING COUNT(1) >= 2
    ORDER BY mentions DESC, (pos - neg) DESC
    LIMIT 25
    """)
    top_assets: list[AssetSignal] = []
    for sym, name, m, p, n, u in rows(qid_assets):
        m, p, n, u = int(m or 0), int(p or 0), int(n or 0), int(u or 0)
        sc = (p - n) / max(1, m)
        top_assets.append(AssetSignal(symbol=sym or None, name=name or None,
                                      mentions=m, pos=p, neg=n, neu=u, net_score=sc))

    qid_headlines = query(f"""
      SELECT title
      {base_filter}
      ORDER BY published_at DESC
      LIMIT {int(limit_headlines)}
    """)
    headlines = [(r[0] or "") for r in rows(qid_headlines) if r and r[0]]

    return SignalsOut(
        window_start_utc=start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        window_end_utc=end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        sample_size=total, pos=pos, neg=neg, neu=neu, net_score=round(net_score, 4),
        top_sources=top_sources, top_assets=top_assets, headlines=headlines,
        query_ids=[qid_counts, qid_sources, qid_assets, qid_headlines],
    )
