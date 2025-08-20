
from tools.market_tools.models import MarketSummary, NewInsight
from tools.market_tools.iceberg_admin import ensure_summary_table, ensure_insights_table
from tools.market_tools.athena_signals import SignalsOut , athena_fetch_signals_tool
from tools.market_tools.historic_features import HistoryFeatures, fetch_history_features, fetch_recent_insights
from tools.market_tools.athena_client import query, column_exists, rows as athena_rows
from tools.market_tools.normalizers import normalize_new_insights
from tools.market_tools.sql_builders import esc, ts, arr_str
from tools.market_tools.write_models import SummaryWriteRow

from tools.market_tools.write_models import CleanInsight
from langchain_core.prompts import ChatPromptTemplate
import boto3
from langchain_aws import ChatBedrockConverse
from tools.market_tools.config import AWS_REGION, GLUE_DATABASE
from datetime import datetime, timedelta, timezone
import json
import logging
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Dict, Any, Tuple, Literal
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _make_llm(temperature: float = 0.2) -> ChatBedrockConverse:
    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    return ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=temperature,
        client=bedrock,
    )


async def summarize_market(days: int = 3) -> MarketSummary:
    ensure_summary_table()
    sigs: SignalsOut = athena_fetch_signals_tool.invoke({"days": days, "limit_headlines": 40})
    feats: HistoryFeatures = fetch_history_features(days_back=30)
    recent_ins = fetch_recent_insights(limit=15)

    llm = _make_llm(temperature=0.2)
    extractor = llm.with_structured_output(MarketSummary)

    evidence_choices = f"runs:{sigs.window_start_utc}..{sigs.window_end_utc}, " + \
                   ", ".join(f"sql:qid={q}" for q in sigs.query_ids)

    prompt = ChatPromptTemplate.from_messages([
        ("system",
        "You are a crypto market analyst.\n"
        "Ground ALL claims in the provided numbers and prior insights.\n"
        "Return only a tool call that conforms to the schema; do NOT write raw JSON.\n"
        "Rules: outlook in {{bullish|bearish|neutral}}; narrative ≤ 120 words.\n"
        "Every item in new_insights must cite evidence_refs derived from the provided facts (runs, query ids)."
        f"... Every new_insight.evidence_refs must be chosen from this set: {evidence_choices} ...\n"
         "drivers` must be market catalysts/themes (macro/liquidity/flows/regulation/tech/adoption). Never a publisher or asset ticker. Use [] if none.\n"
         "Each new_insights[i].tags must include ≥1 topical tags (e.g., ['stablecoins','SOL','DeFi']).\n"
        ),
        ("human",
         "... Evidence options: {evidence_choices}\n..."
         "Window: {start} .. {end}\n"
         "Totals: n={total} (pos={pos}, neg={neg}, neu={neu}), net={net_score}\n"
         "Top sources: {top_sources}\n"
         "Top assets (sym,name,mentions,net):\n{asset_lines}\n\n"
         "Rolling features (last runs):\n{last_runs}\n\n"
         "Asset stance counts (30d):\n{asset_trends}\n\n"
         "Recent insights (most recent first):\n{recent_insights}\n\n"
         "Return the structured summary.")
    ])

    asset_lines = [f"{(a.symbol or '-')},{(a.name or '-')},{a.mentions},{round(a.net_score,3)}" for a in sigs.top_assets]
    last_runs = "\n".join(
        f"- {r['run_ts']}: net={r['net_score']}, 7dma={r['net_7dma']}, Δ={r['net_delta']}"
        for r in feats.last_runs
    )
    asset_trends = "\n".join(
        f"- {r['sym']} {r['name']}: bull={r['bull_ct']}, bear={r['bear_ct']}, neu={r['neu_ct']}"
        for r in feats.asset_trends_30d[:20]
    )
    recent_insights_lines = "\n".join(
        f"- {i['created_at']} | {i['title']} | conf={i.get('confidence')} | tags={i.get('tags', [])}"
        for i in recent_ins
    )

    chain = prompt | extractor
    try:
        result: MarketSummary = chain.invoke({
            "start": sigs.window_start_utc,
            "end": sigs.window_end_utc,
            "total": sigs.sample_size,
            "pos": sigs.pos,
            "neg": sigs.neg,
            "neu": sigs.neu,
            "net_score": sigs.net_score,
            "top_sources": sigs.top_sources,
            "asset_lines": "\n".join(asset_lines),
            "last_runs": last_runs,
            "asset_trends": asset_trends,
            "evidence_choices": evidence_choices,
            "recent_insights": recent_insights_lines,
        })
    except ValidationError:
        # Fallback: ask for raw JSON and coerce
        base_llm = _make_llm(temperature=0.0)
        raw = (prompt | base_llm).invoke({
            "start": sigs.window_start_utc,
            "end": sigs.window_end_utc,
            "total": sigs.sample_size,
            "pos": sigs.pos,
            "neg": sigs.neg,
            "neu": sigs.neu,
            "net_score": sigs.net_score,
            "top_sources": sigs.top_sources,
            "asset_lines": "\n".join(asset_lines),
            "last_runs": last_runs,
            "asset_trends": asset_trends,
            "evidence_choices": evidence_choices,
            "recent_insights": recent_insights_lines,
        })
        raw_text = getattr(raw, "content", None) or str(raw)
        start_i = raw_text.find("{")
        end_i = raw_text.rfind("}")
        payload = raw_text[start_i:end_i+1] if start_i != -1 and end_i != -1 else raw_text
        result = MarketSummary.model_validate_json(payload)

    # mirror hard counts
    result.time_window = f"{sigs.window_start_utc}..{sigs.window_end_utc}"
    result.sample_size = sigs.sample_size
    result.pos = sigs.pos
    result.neg = sigs.neg
    result.neu = sigs.neu
    result.net_score = sigs.net_score

    # If missing narrative, synthesize compact one
    if not result.narrative.strip():
        tilt = "bullish" if result.net_score > 0.1 else "bearish" if result.net_score < -0.1 else "neutral"
        leaders = ", ".join(f"{a.symbol or a.name}({a.stance})" for a in (result.top_assets[:5] or [])) or "no dominant assets"
        result.narrative = (
            f"News flow skews {tilt} (net={result.net_score:.2f}; "
            f"pos={result.pos}, neg={result.neg}, neu={result.neu}, n={result.sample_size}). "
            f"Notable: {leaders}."
        )
    return result


def upsert_summary(days: int, s: MarketSummary):
    ensure_summary_table()
    now = datetime.now(timezone.utc)

    ws, we = s.time_window.split("..")
    row = SummaryWriteRow(
        run_ts=now,
        window_start=ws,
        window_end=we,
        days=int(getattr(s, "days", days) or days),
        sample_size=int(s.sample_size),
        pos=int(s.pos), neg=int(s.neg), neu=int(s.neu),
        net_score=float(s.net_score),
        outlook=s.outlook,
        confidence=float(s.confidence),
        drivers=list(s.drivers or []),
        risks=list(s.risks or []),
        top_assets=[f"{(a.symbol or '-')},{(a.name or '-')},{a.stance}" for a in (s.top_assets or [])],
        narrative=s.narrative or "",
        new_insights_json=json.dumps([ni.model_dump() for ni in (s.new_insights or [])]) if hasattr(s, "new_insights") else None,
    )

    has_json  = column_exists(GLUE_DATABASE, "market_summaries", "new_insights_json")
    has_array = column_exists(GLUE_DATABASE, "market_summaries", "new_insights")

    cols = [
        "run_ts","window_start","window_end","days","sample_size","pos","neg","neu",
        "net_score","outlook","confidence","drivers","risks","top_assets","narrative"
    ]
    vals = [
        ts(row.run_ts),
        ts(row.window_start),
        ts(row.window_end),
        str(row.days),
        str(row.sample_size),
        str(row.pos), str(row.neg), str(row.neu),
        f"CAST({row.net_score} AS DOUBLE)",
        f"'{esc(row.outlook)}'",
        f"CAST({row.confidence} AS DOUBLE)",
        arr_str(row.drivers),
        arr_str(row.risks),
        arr_str(row.top_assets),
        f"'{esc(row.narrative)}'",
    ]

    if has_json:
        cols.append("new_insights_json")
        vals.append(f"'{esc(row.new_insights_json or '[]')}'")
    elif has_array:
        cols.append("new_insights")
        vals.append(arr_str([row.new_insights_json or "[]"]))

    query(f"INSERT INTO {GLUE_DATABASE}.market_summaries ({', '.join(cols)}) VALUES ({', '.join(vals)})")
    logger.info("Summary written to Iceberg: market_summaries")


def _uuid_like() -> str:
    # Lightweight unique id (no external deps)
    return f"ins_{int(time.time()*1000)}"


def store_insights(window_start: str, window_end: str, insights: List[NewInsight], valid_days: int = 7):
    """Persist each NewInsight into insights table (normalized & validated)."""
    ensure_insights_table()

    cleaned: List[CleanInsight] = normalize_new_insights(
        insights, window_start, window_end, default_valid_days=valid_days
    )

    for ni in cleaned:
        query(f"""
        INSERT INTO {GLUE_DATABASE}.insights (
          insight_id, created_at, window_start, window_end,
          title, thesis, tags, evidence_refs, confidence, valid_until,
          parent_ids, hit, hit_reason
        ) VALUES (
          '{esc(ni.insight_id)}',
          {ts(ni.created_at)},
          {ts(ni.window_start)},
          {ts(ni.window_end)},
          '{esc(ni.title)}',
          '{esc(ni.thesis)}',
          {arr_str(ni.tags)},
          {arr_str(ni.evidence_refs)},
          CAST({float(ni.confidence)} AS DOUBLE),
          {ts(ni.valid_until)},
          {arr_str(ni.parent_ids)},
          { 'NULL' if ni.hit is None else str(ni.hit).lower() },
          { 'NULL' if ni.hit_reason is None else "'" + esc(ni.hit_reason) + "'" }
        )
        """)
    logger.info("Insights stored: %d", len(cleaned))


def score_insights(window_end_ts: str, net_threshold: float = 0.05):
    ensure_insights_table()

    qsum = query(f"""
      SELECT run_ts, net_score
      FROM {GLUE_DATABASE}.market_summaries
      WHERE run_ts <= TIMESTAMP '{window_end_ts.replace('T',' ').replace('Z','')}'
      ORDER BY run_ts DESC
      LIMIT 2
    """)
    srows = athena_rows(qsum)
    if len(srows) < 2:
        logger.warning("Not enough runs to score insights")
        return

    latest_ts, latest_net = srows[0]
    prev_ts, prev_net = srows[1]
    delta = float(latest_net) - float(prev_net)
    direction = "up" if delta >= 0 else "down"

    qins = query(f"""
      SELECT insight_id, thesis
      FROM {GLUE_DATABASE}.insights
      WHERE hit IS NULL
        AND created_at <= TIMESTAMP '{latest_ts}'
        AND valid_until >= TIMESTAMP '{latest_ts}'
    """)
    to_score = athena_rows(qins)

    for iid, thesis in to_score:
        t = (thesis or "").lower()
        expect = None
        if any(k in t for k in ["down","bear","deterior","-"]):
            expect = "down"
        elif any(k in t for k in ["up","bull","improv","+"]):
            expect = "up"

        if expect is None:
            query(f"""
              UPDATE {GLUE_DATABASE}.insights
              SET hit = false, hit_reason = 'unknown_expectation'
              WHERE insight_id = '{iid}'
            """)
            continue

        hit = (direction == expect) and (abs(delta) >= net_threshold)
        reason = f"delta={delta:.3f}, expect={expect}"
        query(f"""
          UPDATE {GLUE_DATABASE}.insights
          SET hit = {str(hit).lower()}, hit_reason = '{reason.replace("'", "''")}'
          WHERE insight_id = '{iid}'
        """)
    logger.info("Scored %d insights", len(to_score))