# tools/market_tools/tool_wrappers.py
from __future__ import annotations
from typing import Optional, List
from langchain_core.tools import tool

from tools.market_tools.models import MarketSummary, NewInsight
from tools.market_tools.historic_features import fetch_history_features, fetch_recent_insights
from tools.market_tools.athena_signals import athena_fetch_signals_tool, SignalsOut
from tools.market_tools.athena_summaries import summarize_market, upsert_summary, store_insights, score_insights


# Re-export the existing signals tool as-is
signals_tool = athena_fetch_signals_tool

@tool("summarize_market")
async def summarize_market_tool(days: int = 3) -> MarketSummary:
    """LLM-grounded market summary using Athena/Iceberg data."""
    return await summarize_market(days=days)

@tool("fetch_history_features")
def fetch_history_features_tool(days_back: int = 30):
    """Rolling stats + asset stance counts for the last N days."""
    return fetch_history_features(days_back=days_back).model_dump()

@tool("fetch_recent_insights")
def fetch_recent_insights_tool(tags: Optional[List[str]] = None, limit: int = 15):
    """Recent insights from the insights table, optionally filtered by tags."""
    return fetch_recent_insights(tags=tags, limit=limit)

@tool("upsert_summary")
def upsert_summary_tool(days: int, summary: dict):
    """Persist a MarketSummary (dict form) into market_summaries."""
    s = MarketSummary.model_validate(summary)
    upsert_summary(days, s)
    return {"ok": True}

@tool("store_insights")
def store_insights_tool(window_start: str, window_end: str, insights: List[dict], valid_days: int = 7):
    """Persist new insights into insights table."""
    ni = [NewInsight.model_validate(x) for x in insights]
    store_insights(window_start, window_end, ni, valid_days=valid_days)
    return {"ok": True}

@tool("score_insights")
def score_insights_tool(window_end_ts: str, net_threshold: float = 0.05):
    """Score open insights w.r.t. movement in subsequent runs."""
    score_insights(window_end_ts, net_threshold=net_threshold)
    return {"ok": True}
