# tools/market_hybrid_tools.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, re, math, time, logging, hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlparse

import boto3
from langchain_core.tools import tool
from langchain_aws import ChatBedrockConverse

from tools.athena_client import query as athena_query, rows as athena_rows
from vectors.embeddings import embed_texts  # used only for cosine pairing

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ========= CONFIG (provided by your Make terraform-env) =========
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")

NEWS_KB_ID        = os.getenv("NEWS_KB_ID")            # required
RESEARCH_KB_ID    = os.getenv("RESEARCH_KB_ID") 
RESEARCH_KB_DS_ID   = os.getenv("RESEARCH_KB_DS_ID")     
RESEARCH_KB_BUCKET  = os.getenv("RESEARCH_KB_BUCKET")
RESEARCH_KB_PREFIX  = (os.getenv("RESEARCH_KB_PREFIX") or "research/").strip("/")
AURORA_CLUSTER_ARN = os.getenv("AURORA_CLUSTER_ARN")   # required
AURORA_SECRET_ARN  = os.getenv("AURORA_SECRET_ARN")    # required
AURORA_DB_NAME     = os.getenv("AURORA_DB_NAME", "kbdb")
RESEARCH_TABLE     = os.getenv("RESEARCH_TABLE", "public.research_kb")

_s3   = boto3.client("s3", region_name=AWS_REGION)
_kbcp = boto3.client("bedrock-agent", region_name=AWS_REGION)

TOPK_LIMIT = 30

_br_rt = boto3.client("bedrock-agent-runtime", region_name=AWS_REGION)
_br    = boto3.client("bedrock-runtime",       region_name=AWS_REGION)
_rds   = boto3.client("rds-data",              region_name=AWS_REGION)

# ======================= helpers =======================

def _to_score(m: Dict[str, Any]) -> float:
    if "score" in m:
        try: return float(m["score"])
        except Exception: return 0.0
    if "distance" in m:
        try:
            d = float(m["distance"])
            return 1.0 / (1.0 + max(0.0, d))
        except Exception:
            return 0.0
    return 0.0

def _flatten_md(md_obj: Any) -> Dict[str, Any]:
    """Normalize KB metadata to a flat dict."""
    if not md_obj: return {}
    if isinstance(md_obj, dict):
        if "metadataAttributes" not in md_obj:
            return md_obj
        out = {}
        for k, v in (md_obj.get("metadataAttributes") or {}).items():
            val = v.get("value") if isinstance(v, dict) else v
            if isinstance(val, dict):
                out[k] = val.get("stringValue") or val.get("numberValue") \
                       or val.get("booleanValue") or val.get("stringListValue")
            else:
                out[k] = val
        return out
    return {}

def _kb_retrieve(kb_id: str, text: str, top_k: int = 10, flt: Optional[dict] = None) -> List[dict]:
    cfg = {"vectorSearchConfiguration": {"numberOfResults": int(top_k)}}
    if flt: cfg["vectorSearchConfiguration"]["filter"] = flt
    resp = _br_rt.retrieve(
        knowledgeBaseId=kb_id,
        retrievalConfiguration=cfg,
        retrievalQuery={"text": text or " "},
    )
    out = []
    for r in resp.get("retrievalResults", []):
        out.append({
            "text": ((r.get("content") or {}).get("text") or "").strip(),
            "score": float(_to_score(r)),
            "metadata": _flatten_md(r.get("metadata")),
            "location": r.get("location") or {},
        })
    return out

def _llm(model="anthropic.claude-3-haiku-20240307-v1:0", temperature=0.1):
    return ChatBedrockConverse(client=_br, provider="anthropic", model=model, temperature=temperature)

def _embed(texts: List[str]) -> List[List[float]]:
    vecs = embed_texts(texts, region=AWS_REGION) or []
    return list(vecs) if isinstance(vecs, list) else [list(vecs)]

def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b): return 0.0
    dot = sum(x*y for x, y in zip(a, b))
    na = sum(x*x for x in a) ** 0.5
    nb = sum(y*y for y in b) ** 0.5
    return 0.0 if na == 0.0 or nb == 0.0 else dot / (na * nb)

# ---- KB filter helpers ----
def _kb_and(clauses: List[dict] | None) -> Optional[dict]:
    cs = [c for c in (clauses or []) if c]
    if not cs: return None
    return cs[0] if len(cs) == 1 else {"andAll": cs}

def _kb_or(clauses: List[dict] | None) -> Optional[dict]:
    cs = [c for c in (clauses or []) if c]
    if not cs: return None
    return cs[0] if len(cs) == 1 else {"orAll": cs}

def _kb_string_equals(key: str, val: str) -> dict:
    return {"equals": {"key": key, "value": str(val)}}

def _kb_list_contains(key: str, val: str) -> dict:
    return {"listContains": {"key": key, "value": str(val)}}

def _kb_gte(key: str, val: str) -> dict:
    return {"greaterThanOrEquals": {"key": key, "value": str(val)}}

def _kb_lte(key: str, val: str) -> dict:
    return {"lessThanOrEquals": {"key": key, "value": str(val)}}

def _needs_cls(md: Dict[str, Any]) -> bool:
    """True if symbols/tags/drivers missing or empty."""
    if not md: return True
    for k in ("symbols", "tags", "drivers"):
        v = md.get(k)
        if v is None: return True
        if isinstance(v, (list, tuple)) and len([x for x in v if str(x).strip()]) == 0:
            return True
        if isinstance(v, str) and not v.strip():
            return True
    return False

def _exec_sql(sql: str, params: List[Dict[str, Any]]):
    return _rds.execute_statement(
        resourceArn=AURORA_CLUSTER_ARN,
        secretArn=AURORA_SECRET_ARN,
        database=AURORA_DB_NAME,
        sql=sql,
        parameters=params,
    )

def _extract_syms_dates_from_filter(flt: dict) -> Tuple[Optional[List[str]], Optional[str], Optional[str]]:
    if not isinstance(flt, dict):
        return None, None, None
    syms, dt_from, dt_to = set(), None, None

    def walk(node):
        nonlocal dt_from, dt_to
        if not isinstance(node, dict): return
        if "listContains" in node:
            lc = node["listContains"]
            if lc.get("key") == "symbols" and lc.get("value"):
                syms.add(str(lc["value"]).strip())
        if "greaterThanOrEquals" in node:
            ge = node["greaterThanOrEquals"]
            if ge.get("key") == "as_of": dt_from = ge.get("value")
        if "lessThanOrEquals" in node:
            le = node["lessThanOrEquals"]
            if le.get("key") == "as_of": dt_to = le.get("value")
        for k in ("andAll","orAll"):
            for child in node.get(k, []) or []:
                walk(child)

    walk(flt)
    return (sorted(syms) or None), dt_from, dt_to

_PROPER_RX = re.compile(r"\b(?:[A-Z][a-z0-9]+(?:[-\s][A-Z][a-z0-9]+){0,3})\b")
_TICKER_RX = re.compile(r"\b(BTC|ETH|SOL|XRP|TON|DOGE|LTC|ADA|DOT|AVAX|BNB|OP|ARB|LINK|USDC|USDT|DAI|RWA)\b", re.I)
_STOP = {"The","This","That","And","For","From","With","Into","In","On","Of","To","By","A","An","As"}

def _extract_terms(text: str) -> List[str]:
    if not text: return []
    terms = set()
    for m in _TICKER_RX.findall(text): terms.add(m.upper())
    for m in _PROPER_RX.findall(text):
        if m not in _STOP and len(m) > 2:
            terms.add(m)
    return list(terms)


def _news_only_brief(news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    heads = [n.get("title") for n in (news_items or []) if n.get("title")]
    heads = heads[:8]
    if not heads:
        return {"summary": "No summary available.", "highlights": [], "recommendations": [], "mappings": []}
    return {
        "summary": " · ".join(heads[:5]),
        "highlights": heads[:8],
        "recommendations": [],
        "mappings": []
    }

# ======================= research: retrieve chunks =======================

def _iso_to_epoch_seconds(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        s2 = s.strip()
        if not s2:
            return None
        # epoch seconds or ms passed as string?
        if s2.isdigit():
            iv = int(s2)
            return iv // 1000 if iv > 1_000_000_000_000 else iv
        # tolerate trailing Z
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        dt = datetime.fromisoformat(s2)
        # normalize to UTC-naive seconds
        if dt.tzinfo:
            dt = dt.astimezone(tz=None).replace(tzinfo=None)
        return int(dt.timestamp())
    except Exception:
        return None

def _kb_gte_num(key: str, val: int | float) -> dict:
    return {"greaterThanOrEquals": {"key": key, "value": val}}

def _kb_lte_num(key: str, val: int | float) -> dict:
    return {"lessThanOrEquals": {"key": key, "value": val}}

def _typed_sidecar(meta: dict) -> dict:
    def v(x):
        if isinstance(x, bool):  return {"type":"BOOLEAN","booleanValue": x}
        if isinstance(x,(int,float)) and not isinstance(x,bool): return {"type":"NUMBER","numberValue": float(x)}
        if isinstance(x,(list,tuple)): return {"type":"STRING_LIST","stringListValue":[str(i) for i in x if str(i)]}
        return {"type":"STRING","stringValue": str(x)}
    out = {"metadataAttributes": {}}
    for k,val in (meta or {}).items():
        if val in (None, "", [], {}): continue
        out["metadataAttributes"][k] = {"value": v(val)}
    return out

def _iso_to_epoch_seconds_str(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try:
        ss = s.strip()
        if ss.endswith("Z"): ss = ss[:-1] + "+00:00"
        return int(datetime.fromisoformat(ss).timestamp())
    except Exception:
        return None
    
def _partition_symbols_for_news(syms: List[str] | None) -> Tuple[List[str], List[str]]:
    """
    Split research 'symbols' into (tickers, keywords).
    - Tickers: BTC/ETH/SOL/... (RWA is treated as a keyword, not a ticker)
    - Keywords: everything else (e.g., redstone, base, agentpy, rwa)
    """
    if not syms:
        return [], []
    tickers, keywords = [], []
    for s in syms:
        s2 = str(s or "").strip()
        if not s2:
            continue
        up = s2.upper()
        if _TICKER_RX.fullmatch(up) and up != "RWA":
            tickers.append(up)
        else:
            keywords.append(s2.lower())
    return sorted(set(tickers)), sorted(set(keywords))

# ======================= pairing + LLM briefs =======================

def _build_semantic_pairs(research_chunks, news_items, top_per_news=1, min_sim=0.18):
    cids, ctxts = [], []
    for c in research_chunks or []:
        if not c.get("text"): continue
        cids.append(str(c["chunk_id"]))
        ctxts.append((c["text"] or "")[:600])
    if not cids: return {}
    nids, ntxts = [], []
    for n in news_items or []:
        nid = n.get("news_id")
        if not nid: continue
        ntxt = " ".join([
            n.get("title") or "",
            " ".join(n.get("tags") or []),
            " ".join(n.get("symbols") or []),
        ])[:220]
        if not ntxt.strip(): continue
        nids.append(str(nid)); ntxts.append(ntxt)
    if not nids: return {}

    cvec = _embed(ctxts); nvec = _embed(ntxts)
    out: Dict[str, List[Tuple[str, float]]] = {}
    for i, nid in enumerate(nids):
        row = []
        for j, cid in enumerate(cids):
            sim = _cosine(nvec[i], cvec[j])
            if sim >= min_sim: row.append((cid, sim))
        row.sort(key=lambda t: t[1], reverse=True)
        out[nid] = row[:max(1, int(top_per_news))]
    return out


def _doc_id_from_location(loc: Dict[str, Any]) -> str:
    """
    Try to recover a stable doc_id from the location when metadata lacks it.
    Works for S3-backed KBs by using the object key basename (without extension).
    """
    if not isinstance(loc, dict):
        return ""
    s3 = loc.get("s3Location") or {}
    uri = (s3.get("uri") or "").strip()
    if not uri:
        return ""
    # uri like: s3://bucket/path/to/<doc_id>.json (or .txt, .pdf, etc.)
    try:
        path = urlparse(uri).path or ""
        base = os.path.basename(path)
        stem = base.rsplit(".", 1)[0] if "." in base else base
        return stem.strip()
    except Exception:
        return ""
    
# === sentiment & alias helpers (NEW) ===
ALIASES = {
    "agentpy": ["agentpy", "agent layer", "agentlayer", "magpie", "eigenpie", "egp"],
    "base": ["base", "coinbase base"],
    "rwa": ["rwa", "real world assets", "tokenized treasuries", "onchain treasuries", "tokenization"],
    "redstone": ["redstone"],
}

SOURCE_WEIGHTS = {
    # light trust weighting; unseen sources default to 1.0
    "coindesk": 1.10, "the block": 1.08, "forbes": 1.08,
    "cointelegraph": 1.00, "cryptoslate": 0.98, "bitcoin": 0.98,
}

POS_LEX = ("rally", "surge", "record", "all-time high", "approve", "launch", "partnership",
           "integration", "listing", "raises", "inflow", "growth", "adoption", "upgrade", "mainnet")
NEG_LEX = ("hack", "exploit", "bug", "lawsuit", "ban", "halt", "outage", "bearish", "decline",
           "plunge", "selloff", "fear", "outflow", "crackdown", "charges", "fine", "delay", "reject")

def _expand_aliases(qs: List[str], max_extra: int = 6) -> List[str]:
    """Add a few high-value aliases so we don’t miss news."""
    out, added = list(qs), 0
    lower = {q.lower() for q in qs}
    for k, vals in ALIASES.items():
        if k in lower:
            for v in vals:
                if v.lower() not in lower:
                    out.append(v); lower.add(v.lower()); added += 1
                    if added >= max_extra: break
        if added >= max_extra: break
    return out

def _recency_weight(dt_iso: str | None, half_life_days: float = 14.0) -> float:
    if not dt_iso: return 1.0
    try:
        if dt_iso.endswith("Z"): dt_iso = dt_iso[:-1] + "+00:00"
        age_days = max(0.0, (datetime.utcnow() - datetime.fromisoformat(dt_iso).replace(tzinfo=None)).days + 
                       (datetime.utcnow() - datetime.fromisoformat(dt_iso).replace(tzinfo=None)).seconds/86400.0)
        # exponential decay (half-life)
        return 0.5 ** (age_days / max(1e-6, half_life_days))
    except Exception:
        return 1.0

def _src_weight(src: str | None) -> float:
    if not src: return 1.0
    s = src.strip().lower()
    return SOURCE_WEIGHTS.get(s, 1.0)

def _polarity_from_text(t: str) -> Tuple[str, float]:
    """Tiny lexical fallback if we avoid an LLM per item."""
    tl = t.lower()
    pos = any(w in tl for w in POS_LEX)
    neg = any(w in tl for w in NEG_LEX)
    if pos and not neg: return ("bullish", 0.6)
    if neg and not pos: return ("bearish", 0.6)
    return ("neutral", 0.4)

def _has_any(s: str, terms: List[str]) -> bool:
    s = (s or "").lower()
    return any(t for t in (terms or []) if t and t.lower() in s)

# ======================= news retrieval via KB =======================

def _kb_news_filter(symbols: Optional[List[str]] = None,
                    date_from_iso: Optional[str] = None,
                    date_to_iso: Optional[str] = None,
                    tags_any: Optional[List[str]] = None) -> Optional[dict]:
    """
    Build a Bedrock KB filter:
      AND(
        date window,
        OR(symbols listContains any, tags listContains any)   # optional
      )
    """
    clauses = []

    or_clauses = []
    if symbols:
        or_clauses.append(_kb_or([_kb_list_contains("symbols", s) for s in symbols if s]))
    if tags_any:
        or_clauses.append(_kb_or([_kb_list_contains("tags", t) for t in tags_any if t]))

    if or_clauses:
        clauses.append(_kb_or(or_clauses))

    ep_from = _iso_to_epoch_seconds(date_from_iso) if date_from_iso else None
    ep_to   = _iso_to_epoch_seconds(date_to_iso)   if date_to_iso   else None
    if ep_from is not None:
        clauses.append(_kb_gte_num("as_of_epoch", ep_from))
    if ep_to is not None:
        clauses.append(_kb_lte_num("as_of_epoch", ep_to))

    return _kb_and(clauses)

@tool("kb_ingest_research")
def kb_ingest_research_tool(docs: List[Dict[str, Any]], wait: bool=False,
                            poll_seconds: int=5, timeout_seconds: int=900) -> Dict[str, Any]:
    """
    Uploads research docs to S3 (txt + .metadata.json) and starts a Bedrock KB ingestion.
    Each item:
      { "doc_id": str, "title": str, "text": str,
        "as_of": "YYYY-MM-DDTHH:MM:SSZ", "symbols":[...], "tags":[...], "drivers":[...] }
    """
    if not (RESEARCH_KB_ID and RESEARCH_KB_DS_ID and RESEARCH_KB_BUCKET):
        raise RuntimeError("Missing RESEARCH_KB_* env (KB_ID, DS_ID, BUCKET)")

    uploaded = 0
    for d in docs or []:
        did = d["doc_id"]
        txt_key   = f"{RESEARCH_KB_PREFIX}/{did}.txt"
        pdf_key   = f"{RESEARCH_KB_PREFIX}/{did}.pdf"
        txt_meta  = f"{txt_key}.metadata.json"
        pdf_meta  = f"{pdf_key}.metadata.json"
        

        # 1) text
        _s3.put_object(
            Bucket=RESEARCH_KB_BUCKET,
            Key=txt_key,
            Body=(d.get("text") or "").encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )

        # 2) sidecar (doc-level metadata copied onto all chunks)
        as_of = d.get("as_of")
        sidecar = _typed_sidecar({
             "doc_id": did,
             "title": d.get("title") or did.replace("_"," "),
             "as_of": as_of,
             "as_of_epoch": _iso_to_epoch_seconds_str(as_of),
             "symbols": d.get("symbols") or [],
             "tags": d.get("tags") or [],
             "drivers": d.get("drivers") or [],
         })
        
        sidecar_bytes = json.dumps(sidecar, ensure_ascii=False).encode("utf-8")
        _s3.put_object(Bucket=RESEARCH_KB_BUCKET, Key=txt_meta,
                       Body=sidecar_bytes, ContentType="application/json")
        from botocore.exceptions import ClientError

        try:
            _s3.head_object(Bucket=RESEARCH_KB_BUCKET, Key=pdf_key)
            _s3.put_object(Bucket=RESEARCH_KB_BUCKET, Key=pdf_meta,
                           Body=sidecar_bytes, ContentType="application/json")
        except ClientError:
            pass
        uploaded += 1

    # 3) trigger ingestion
    resp = _kbcp.start_ingestion_job(
        knowledgeBaseId=RESEARCH_KB_ID,
        dataSourceId=RESEARCH_KB_DS_ID,
        description=f"research batch: {uploaded} docs",
    )
    job_id = resp["ingestionJob"]["ingestionJobId"]
    if not wait:
        return {"uploaded": uploaded, "ingestion_job_id": job_id, "status": "STARTED"}

    # optional poll
    t0 = time.time(); status = None
    while time.time() - t0 < timeout_seconds:
        j = _kbcp.get_ingestion_job(
            knowledgeBaseId=RESEARCH_KB_ID,
            dataSourceId=RESEARCH_KB_DS_ID,
            ingestionJobId=job_id,
        )["ingestionJob"]
        status = j["status"]
        if status in ("COMPLETE","FAILED","ERROR","STOPPED"): break
        time.sleep(poll_seconds)
    return {"uploaded": uploaded, "ingestion_job_id": job_id, "status": status or "UNKNOWN"}


@tool("list_research_docs_via_aurora")
def list_research_docs_via_aurora_tool(limit: int = 200) -> Dict[str, Any]:
    """
    Fallback listing of research documents directly from Aurora (RESEARCH_TABLE).
    Reads distinct doc_id (and optional title) out of custom_metadata JSONB.
    """
    sql = f"""
    SELECT
      custom_metadata->>'doc_id' AS doc_id,
      max(coalesce(custom_metadata->>'title','')) AS title,
      count(*) AS hit_count
    FROM {RESEARCH_TABLE}
    WHERE custom_metadata ? 'doc_id'
    GROUP BY 1
    ORDER BY count(*) DESC
    LIMIT :p_lim
    """
    res = _exec_sql(sql, [{"name": "p_lim", "value": {"longValue": int(limit)}}])
    rows = res.get("records", []) or []
    docs: List[Dict[str, Any]] = []
    for r in rows:
        doc_id = (r[0].get("stringValue") or "").strip()
        title  = (r[1].get("stringValue") or "").strip() or doc_id
        hits   = int(r[2].get("longValue") or 0)
        if doc_id:
            docs.append({"doc_id": doc_id, "title": title, "hit_count": hits, "sample_excerpt": ""})
    return {"docs": docs}

@tool("retrieve_research_chunks")
def retrieve_research_chunks_tool(doc_id: Optional[str] = None, k: int = 40) -> Dict[str, Any]:
    """
    Retrieve up to k research chunks from the Research KB (Aurora).
    If the metadata filter by doc_id returns nothing (because doc_id isn't there yet),
    fall back to an unfiltered retrieve and match by S3 key basename.
    """
    if not RESEARCH_KB_ID:
        raise RuntimeError("RESEARCH_KB_ID is not set")

    k = max(1, min(int(k or 40), TOPK_LIMIT))

    # 1) Try metadata-filtered lookup (works when doc_id is already present)
    flt = _kb_string_equals("doc_id", doc_id) if doc_id else None
    matches = _kb_retrieve(RESEARCH_KB_ID, text=(doc_id or "research document"), top_k=k, flt=flt)

    # 2) Fallback: no doc_id yet in KB. Pull without filter and select by S3 basename.
    if not matches:
        probe_k = max(k, TOPK_LIMIT)  # widen a bit
        all_hits = _kb_retrieve(RESEARCH_KB_ID, text=(doc_id or "research document"), top_k=probe_k, flt=None)
        target = (doc_id or "").strip()
        if target:
            filtered = []
            for m in all_hits:
                did_from_loc = _doc_id_from_location(m.get("location") or {})
                if did_from_loc == target:
                    filtered.append(m)
            matches = filtered

    items: List[Dict[str, Any]] = []
    for m in matches:
        md = m.get("metadata") or {}
        # Prefer explicit doc_id; otherwise recover from S3 key; otherwise fall back to the requested doc_id
        did = (md.get("doc_id") or _doc_id_from_location(m.get("location") or {}) or doc_id or "").strip()

        page = md.get("x-amz-bedrock-kb-document-page-number")
        ord_ = md.get("ord", page)
        try:
            ord_i = int(float(ord_)) if ord_ is not None else None
        except Exception:
            ord_i = None

        text = (m.get("text") or "").strip()
        if not text:
            continue

        src = (((m.get("location") or {}).get("s3Location") or {}).get("uri") or "").strip()

        if did and ord_i is not None:
            cid = f"research#{did}#ord:{ord_i}"
        else:
            h = hashlib.blake2b(text[:300].encode("utf-8"), digest_size=8).hexdigest()
            cid = f"research#{did or 'na'}#h:{h}"

        items.append({
            "chunk_id": cid,
            "doc_id": did,
            "ord": ord_i,
            "text": text,
            "score": float(m.get("score") or 0.0),
            "symbols": md.get("symbols") or [],
            "tags":    md.get("tags") or [],
            "drivers": md.get("drivers") or [],
            "source_uri": src,
            "page_number": page,
        })

    items.sort(key=lambda x: x["score"], reverse=True)
    return {"doc_ref": (doc_id or ""), "chunks": items}

# ======================= news retrieval via KB =======================

def _kb_news_filter(symbols: Optional[List[str]] = None,
                    date_from_iso: Optional[str] = None,
                    date_to_iso: Optional[str] = None,
                    tags_any: Optional[List[str]] = None) -> Optional[dict]:
    """
    Build a Bedrock KB filter:
      AND(
        date window,
        OR(symbols listContains any, tags listContains any)   # optional
      )
    """
    clauses = []

    or_clauses = []
    if symbols:
        or_clauses.append(_kb_or([_kb_list_contains("symbols", s) for s in symbols if s]))
    if tags_any:
        or_clauses.append(_kb_or([_kb_list_contains("tags", t) for t in tags_any if t]))

    if or_clauses:
        clauses.append(_kb_or(or_clauses))

    ep_from = _iso_to_epoch_seconds(date_from_iso) if date_from_iso else None
    ep_to   = _iso_to_epoch_seconds(date_to_iso)   if date_to_iso   else None
    if ep_from is not None:
        clauses.append(_kb_gte_num("as_of_epoch", ep_from))
    if ep_to is not None:
        clauses.append(_kb_lte_num("as_of_epoch", ep_to))

    return _kb_and(clauses)

# ======================= research: classify missing only =======================

@tool("classify_research_chunks_if_needed")
def classify_research_chunks_if_needed_tool(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Audits chunks; ONLY classifies those missing symbols/tags/drivers.
    Returns {"labels": [...], "updated": <count updated>}.
    """
    if not chunks:
        return {"labels": [], "updated": 0}

    llm = _llm()
    to_cls = [c for c in chunks if _needs_cls({"symbols": c.get("symbols"), "tags": c.get("tags"), "drivers": c.get("drivers")})]

    if not to_cls:
        return {"labels": [], "updated": 0}

    labels = []
    sys = ("You label research excerpts. Return STRICT JSON with keys: "
           "symbols (UPPER tickers), tags (lowercase keywords), drivers (short phrases).")
    for c in to_cls:
        text = (c.get("text") or "")[:700]
        payload = {"excerpt": text}
        out = llm.invoke([("system", sys), ("user", json.dumps(payload, ensure_ascii=False))])
        content = getattr(out, "content", "") or "{}"
        try:
            data = json.loads(content)
        except Exception:
            data = {}
        syms = [s.strip().upper() for s in (data.get("symbols") or []) if isinstance(s, str) and s.strip()]
        tags = [t.strip().lower() for t in (data.get("tags") or []) if isinstance(t, str) and t.strip()]
        drv  = [d.strip() for d in (data.get("drivers") or []) if isinstance(d, str) and d.strip()]
        # tiny regex assist
        if text and not syms:
            rx = re.findall(r"\b(?:BTC|ETH|SOL|XRP|TON|DOGE|LTC|ADA|DOT|AVAX|BNB|OP|ARB|LINK)\b", text.upper())
            syms = sorted(set(rx))

        labels.append({
            "chunk_id": c.get("chunk_id"),
            "doc_id": c.get("doc_id"),
            "ord": c.get("ord"),
            "symbols": syms[:16],
            "tags": sorted(set(tags))[:24],
            "drivers": drv[:16],
            "source_uri": c.get("source_uri"),
            "page_number": c.get("page_number")
        })

    # persist only for those we classified
    updated = persist_research_metadata_tool.invoke({"labels": labels}).get("updated", 0)
    return {"labels": labels, "updated": int(updated)}

@tool("persist_research_metadata")
def persist_research_metadata_tool(labels: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge {'symbols','tags','drivers','classified_at'} into RESEARCH_TABLE.custom_metadata,
    but ONLY when (doc_id & ord) are present to safely locate the row.
    """
    if not labels:
        return {"updated": 0}
    updated = 0
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    sql_doc_ord = f"""
    UPDATE {RESEARCH_TABLE}
       SET custom_metadata =
           coalesce(custom_metadata, '{{}}'::jsonb)
           || jsonb_build_object(
                'symbols', (:p_symbols)::jsonb,
                'tags',    (:p_tags)::jsonb,
                'drivers', (:p_drivers)::jsonb
              )
           || jsonb_build_object('classified_at', to_jsonb(:p_ts::text))
     WHERE (custom_metadata->>'doc_id') = :p_doc_id
       AND ((custom_metadata->>'ord')::int) = (:p_ord::int)
    """

    sql_src_page = f"""
    UPDATE {RESEARCH_TABLE}
       SET custom_metadata =
           coalesce(custom_metadata, '{{}}'::jsonb)
           || jsonb_build_object(
                'symbols', (:p_symbols)::jsonb,
                'tags',    (:p_tags)::jsonb,
                'drivers', (:p_drivers)::jsonb
              )
           || jsonb_build_object('classified_at', to_jsonb(:p_ts::text))
     WHERE (custom_metadata->>'x-amz-bedrock-kb-source-uri') = :p_src
       AND ((custom_metadata->>'x-amz-bedrock-kb-document-page-number')::int) = (:p_page::int)
    """

    for l in labels:
        did, ord_ = l.get("doc_id"), l.get("ord")
        use_doc_ord = bool(did and ord_ is not None)

        # common params
        base = [
            {"name": "p_symbols", "value": {"stringValue": json.dumps(l.get("symbols") or [])}},
            {"name": "p_tags",    "value": {"stringValue": json.dumps(l.get("tags") or [])}},
            {"name": "p_drivers", "value": {"stringValue": json.dumps(l.get("drivers") or [])}},
            {"name": "p_ts",      "value": {"stringValue": ts}},
        ]

        try:
            n = 0
            if use_doc_ord:
                params = base + [
                    {"name": "p_doc_id", "value": {"stringValue": str(did)}},
                    {"name": "p_ord",    "value": {"stringValue": str(int(ord_))}},
                ]
                res = _exec_sql(sql_doc_ord, params)
                n = int(res.get("numberOfRecordsUpdated", 0) or 0)

            if n == 0 and (l.get("source_uri") and l.get("page_number") is not None):
                params2 = base + [
                    {"name": "p_src",  "value": {"stringValue": str(l.get("source_uri"))}},
                    {"name": "p_page", "value": {"stringValue": str(int(l.get("page_number"))) }},
                ]
                res2 = _exec_sql(sql_src_page, params2)
                n = int(res2.get("numberOfRecordsUpdated", 0) or 0)

            if n > 0:
                updated += 1
        except Exception as e:
            logger.warning(f"persist metadata failed for doc={did} ord={ord_}: {e}")

    return {"updated": updated}



@tool("query_news_vectors")  # back-compat name; now hits Bedrock KB (Aurora) directly
def query_news_vectors_tool(
    queries: List[str],
    top_k_per_query: int = 20,
    max_total: int = 200,
    metadata_filter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve news from the Bedrock Knowledge Base using numeric date filtering (as_of_epoch).
    De-duplicates across queries by (title,url) fingerprint.
    """
    if not queries:
        return []

    queries = [q.strip() for q in queries if isinstance(q, str) and q.strip()]
    if not queries:
        return []

    # Extract intended constraints from any passed filter tree
    syms, dt_from, dt_to = _extract_syms_dates_from_filter(metadata_filter) if isinstance(metadata_filter, dict) else (None, None, None)
    kb_filter = _kb_news_filter(symbols=syms, date_from_iso=dt_from, date_to_iso=dt_to)

    def _fp(title: str, url: str) -> str:
        t = " ".join((title or "").strip().lower().split())
        u = (url or "").strip().lower()
        return f"{t}|{u}"

    by_id: Dict[str, Dict[str, Any]] = {}
    by_fp: Dict[str, str] = {}

    for q in queries:
        # Pull from the NEWS KB with a single vector retrieve per query
        matches = _kb_retrieve(NEWS_KB_ID, text=q, top_k=int(top_k_per_query), flt=kb_filter)

        for r in matches:
            meta = r.get("metadata") or {}
            title = (meta.get("headline") or meta.get("title") or (r.get("text") or "").split("\n", 1)[0]).strip()
            url   = (meta.get("url") or "").strip()
            nid   = (meta.get("news_id") or _fp(title, url)).strip()

            # Score already normalized in _to_score
            score = float(r.get("score") or 0.0)

            item = by_id.get(nid)
            if (not item) or score > item["score"]:
                by_id[nid] = {
                    "news_id": nid,
                    "title": title,
                    "symbols": meta.get("symbols") or [],
                    "tags": meta.get("tags") or [],
                    "published_at": meta.get("as_of") or meta.get("published_at"),
                    "news_url": url,
                    "source_name": meta.get("source") or meta.get("source_name"),
                    "score": score,
                }

            if title or url:
                fp = _fp(title, url)
                chosen = by_fp.get(fp)
                if chosen is None or by_id[nid]["score"] > by_id[chosen]["score"]:
                    by_fp[fp] = nid

    keep_ids = set(by_fp.values()) if by_fp else set(by_id.keys())
    items = [v for nid, v in by_id.items() if nid in keep_ids]
    items.sort(key=lambda x: (x.get("score") or 0.0), reverse=True)
    return items[: int(max_total)]





@tool("synthesize_marketing_brief")
def synthesize_marketing_brief_tool(doc_meta: Dict[str, Any],
                                    research_chunks: List[Dict[str, Any]],
                                    news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create a per-paper marketing brief from research chunks + candidate news.
    Returns a JSON-like dict:
      {
        "summary": str,
        "highlights": [str],
        "recommendations": [str],
        "mappings": [{"news_id": str, "chunk_id": str, "rationale": str}]
      }
    Only uses the provided items; includes a semantic fallback when LLM output can't be parsed.
    """
    llm = _llm()
    allowed_news  = {str(n.get("news_id")) for n in news_items if n.get("news_id")}
    allowed_chunks= {str(c.get("chunk_id")) for c in research_chunks if c.get("chunk_id")}

    r_chunks = [{"id": c["chunk_id"], "text": (c.get("text") or "")[:600]} for c in (research_chunks or [])[:10]]
    n_items  = [{
        "news_id": n.get("news_id"),
        "title": n.get("title"),
        "tags": n.get("tags", []),
        "symbols": n.get("symbols", []),
        "date": str(n.get("published_at") or ""),
        "source": n.get("source_name"),
        "url": n.get("news_url"),
        "score": float(n.get("score", 0.0)),
    } for n in (news_items or [])[:20] if n.get("news_id")]

    candidates = _build_semantic_pairs(research_chunks, news_items, 1, 0.18)
    lm_pairs = [{"news_id": nid, "chunk_id": cid, "similarity": round(sim,3)}
                for nid, lst in candidates.items() for (cid, sim) in lst]

    sys = ("You are a precise analyst. ONLY use provided items. "
           "Output STRICT JSON: summary, highlights[], recommendations[], "
           "mappings[{news_id, chunk_id, rationale<=30w}].")
    payload = {
        "doc": {"doc_id": doc_meta.get("doc_id"), "title": doc_meta.get("title")},
        "research_chunks": r_chunks,
        "news_items": n_items,
        "match_candidates": lm_pairs,
    }
    out = llm.invoke([("system", sys), ("user", json.dumps(payload, ensure_ascii=False))])
    try:
        data = json.loads(getattr(out, "content", "") or "{}")
    except Exception:
        data = {}

    # safety cleanup + fallback
    summary = data.get("summary") or "No summary available."
    highlights = [str(h).strip() for h in (data.get("highlights") or []) if str(h).strip()][:10]
    recommendations = [str(r).strip() for r in (data.get("recommendations") or []) if str(r).strip()][:10]

    cleaned, seen = [], set()
    raw_m = data.get("mappings") or []
    for m in raw_m:
        if not isinstance(m, dict): continue
        nid, cid = str(m.get("news_id")), str(m.get("chunk_id"))
        if nid in allowed_news and cid in allowed_chunks and (nid, cid) not in seen:
            rationale = " ".join((m.get("rationale") or "").split()[:30]).strip()
            cleaned.append({"news_id": nid, "chunk_id": cid, "rationale": rationale})
            seen.add((nid, cid))

    if not cleaned:  # fallback to semantic pairs
        for nid, lst in candidates.items():
            if not lst: continue
            cid, sim = lst[0]
            if nid in allowed_news and cid in allowed_chunks:
                cleaned.append({"news_id": nid, "chunk_id": cid, "rationale": f"Semantic match (sim≈{sim:.2f})."})

    if (not highlights and not recommendations) or summary == "No summary available.":
        fallback = _news_only_brief(news_items)
        # prefer LLM pieces if present; fill gaps otherwise
        summary = summary if summary != "No summary available." else fallback["summary"]
        if not highlights: highlights = fallback["highlights"]

    return {"summary": summary, "highlights": highlights, "recommendations": recommendations, "mappings": cleaned[:12]}

# ======================= planning queries =======================

@tool("select_news_queries_from_chunks_or_llm")
def select_news_queries_from_chunks_or_llm_tool(chunks: List[Dict[str, Any]],
                                                min_q: int = 6, max_q: int = 10) -> Dict[str, Any]:
    """
    Prefer metadata (symbols/tags/drivers). Fallback adds doc title and 1–2
    short entity-rich LLM phrases per chunk to reach min_q.
    """
    # harvest metadata
    syms, tags, drvs = [], [], []
    for c in chunks or []:
        syms += [s for s in (c.get("symbols") or []) if s]
        tags += [t for t in (c.get("tags") or []) if t]
        drvs += [d for d in (c.get("drivers") or []) if d]

    base = list(dict.fromkeys([*(s.upper() for s in syms), *(t.lower() for t in tags), *drvs]))
    base = [q for q in base if isinstance(q, str) and q.strip()]

    # Deterministic extraction from content
    extras = []
    for c in (chunks or [])[:8]:
        txt = (c.get("text") or "")[:800]
        hdr = txt.split("\n", 1)[0][:80].strip()
        if hdr: extras.append(hdr)
        extras += _extract_terms(txt)

    seen, final = set(x.lower() for x in base), base[:]
    for q in extras:
        if len(final) >= max_q: break
        k = q.lower()
        if k and k not in seen and k not in {"crypto regulation","crypto news","market update"}:
            final.append(q); seen.add(k)

    if len(final) >= min_q:
        return {"queries": final[:max_q]}

    # LLM top-up last
    llm = _llm(temperature=0.2)
    sample = [{"id": c.get("chunk_id"), "text": (c.get("text") or "")[:400]} for c in (chunks or [])[:6]]
    sys = ("Return STRICT JSON {\"queries\":[...]} with 6–10 short, entity-rich phrases (<=12 words). "
           "Prefer tickers, protocol/product names, bill names. "
           "Do NOT output generic phrases.")
    out = llm.invoke([("system", sys), ("user", json.dumps({"chunks": sample}, ensure_ascii=False))])
    try:
        data = json.loads(getattr(out, "content", "") or "{}")
        add = [q.strip() for q in data.get("queries", []) if q and q.strip()]
        for q in add:
            k = q.lower()
            if len(final) >= max_q: break
            if k not in seen and k not in {"crypto regulation","crypto news","market update"}:
                final.append(q); seen.add(k)
    except Exception:
        pass
    final = _expand_aliases(final, max_extra=6)
    return {"queries": final[:max_q]}

# ======================= orchestration =======================

@tool("ensure_research_doc_ready")
def ensure_research_doc_ready_tool(doc_id: str, k: int = 60) -> Dict[str, Any]:
    """
    Preliminary step: fetch chunks, classify ONLY those missing metadata, persist, and return fresh chunks.
    """
    fetched = retrieve_research_chunks_tool.invoke({"doc_id": doc_id, "k": k})
    chunks = fetched.get("chunks", [])
    cls = classify_research_chunks_if_needed_tool.invoke({"chunks": chunks})

    # NEW: merge labels into chunks for the current run even if DB persist skipped
    labels_by_key = {}
    for l in cls.get("labels", []):
        key = (l.get("doc_id"), l.get("ord"), l.get("chunk_id"))
        labels_by_key[key] = l

    merged = []
    for c in chunks:
        key = (c.get("doc_id"), c.get("ord"), c.get("chunk_id"))
        lbl = labels_by_key.get(key)
        if lbl:
            c = {**c}
            # only fill if empty/missing (keeps your “fill only if missing” rule)
            if not c.get("symbols"): c["symbols"] = lbl.get("symbols") or []
            if not c.get("tags"):    c["tags"]    = lbl.get("tags") or []
            if not c.get("drivers"): c["drivers"] = lbl.get("drivers") or []
        merged.append(c)

    # If any DB rows were updated, optionally re-fetch; otherwise keep merged
    if cls.get("updated", 0) > 0:
        fetched = retrieve_research_chunks_tool.invoke({"doc_id": doc_id, "k": k})
    else:
        fetched["chunks"] = merged

    fetched["updated"] = cls.get("updated", 0)
    fetched["ready"] = True
    return fetched

@tool("score_news_for_asset")
def score_news_for_asset_tool(asset_terms: List[str],
                              news_items: List[Dict[str, Any]],
                              doc_drivers: Optional[List[str]] = None,
                              min_sim: float = 0.18,
                              max_keep: int = 30) -> Dict[str, Any]:
    """
    Enrich news with bullish/bearish scoring for an asset. Direct mentions get full weight;
    driver-only items get partial weight. Also uses semantic pairs to keep relevance high.
    Returns: { "stance": str, "net_score": float, "items": [ ... enriched news ... ] }
    """
    asset_terms = [t.strip().lower() for t in (asset_terms or []) if t and t.strip()]
    doc_drivers = [d.strip().lower() for d in (doc_drivers or []) if d and d.strip()]

    # 1) semantic filter (reuse existing embed pairing)
    pairs = _build_semantic_pairs(
        research_chunks=[{"chunk_id": f"tmp#{i}", "text": " ".join(asset_terms + doc_drivers)} for i in range(1)],
        news_items=news_items, top_per_news=1, min_sim=min_sim
    )
    keep = {nid for nid, lst in pairs.items() if lst}

    enriched = []
    net = 0.0
    for n in news_items:
        title = (n.get("title") or "").strip()
        tags  = [*(n.get("tags") or []), *(n.get("symbols") or [])]
        src   = n.get("source_name")
        date  = str(n.get("published_at") or "")
        direct = _has_any(title, asset_terms) or _has_any(" ".join(tags), asset_terms)

        # driver hit if any doc driver appears
        driver_hit = _has_any(title, doc_drivers) or _has_any(" ".join(tags), doc_drivers)

        # light lexical polarity (fast + conservative)
        pol, conf = _polarity_from_text(title)

        # weight knobs
        base = float(n.get("score") or 0.0)
        rec  = _recency_weight(date)
        srcw = _src_weight(src)
        simw = 1.15 if n.get("news_id") in keep else 0.90
        dw   = 1.0 if direct else (0.6 if driver_hit else 0.45)

        signed = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}[pol]
        weight = dw * rec * srcw * simw
        final_score = signed * conf * max(0.6, base) * weight

        enriched.append({**n,
                         "polarity": pol,
                         "confidence": round(conf, 3),
                         "direct": bool(direct),
                         "driver_related": bool(driver_hit),
                         "rank_score": round(abs(final_score), 5),
                         "signed_score": round(final_score, 5)})
        net += final_score

    # sort by (abs sentiment weight) then by original kb score
    enriched.sort(key=lambda x: (x.get("rank_score", 0.0), x.get("score", 0.0)), reverse=True)
    items = enriched[:max_keep]

    stance = "bullish" if net > 0.2 else ("bearish" if net < -0.2 else "neutral")
    return {"stance": stance, "net_score": round(net, 4), "items": items}

@tool("news_for_doc")
def news_for_doc_tool(doc_id: str,
                      title: Optional[str] = None,
                      days_back: int = 90,
                      top_k_per_query: int = 20,
                      max_total: int = 150) -> Dict[str, Any]:
    """
    Ensure doc is ready, derive queries, fetch news from KB, and return a per-doc brief.
    """
    prepared = ensure_research_doc_ready_tool.invoke({"doc_id": doc_id})
    chunks = prepared.get("chunks", [])

    # queries (prefer metadata)
    qs_res = select_news_queries_from_chunks_or_llm_tool.invoke({"chunks": chunks})
    qs = qs_res.get("queries") or []

    if not qs:
        # doc-specific, deterministic fallback:
        base = []
        if title: base.append(title.replace("_", " ")[:48])
        if not base and doc_id: base.append(doc_id.replace("_", " ")[:48])
        # add 1–2 short headers from the first chunks
        for c in chunks[:2]:
            h = (c.get("text") or "").split("\n", 1)[0][:80].strip()
            if h: base.append(h)
        # final guardrail keywords to keep it crypto-specific
        base += ["stablecoins", "onchain liquidity", "L2 scaling"]
        qs = [q for q in base if q][:8]

    # symbol filter to tighten results
    syms_all = [(s or "") for c in chunks for s in (c.get("symbols") or [])]
    tickers, keywords = _partition_symbols_for_news(syms_all)

    tags_from_chunks = sorted({(t or "").lower() for c in chunks for t in (c.get("tags") or []) if t})
    tags_any = (keywords + tags_from_chunks)[:12]  # cap to keep filter small

    # --- date window
    date_to = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    date_from = (datetime.utcnow() - timedelta(days=int(days_back))).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- hybrid filter: (symbols tickers OR tags keywords) AND date
    filt_primary = _kb_news_filter(
        symbols=tickers[:8] or None,
        tags_any=tags_any or None,
        date_from_iso=date_from,
        date_to_iso=date_to,
    )

    news = query_news_vectors_tool.invoke({
    "queries": qs,  # qs already includes metadata terms; good for vector scoring
    "top_k_per_query": top_k_per_query,
    "max_total": max_total,
    "metadata_filter": filt_primary
    })

     # --- NEW: sentiment scoring & re-ranking
    asset_terms = []
    # use doc id/title plus prominent tags from chunks as asset hints
    asset_terms += [doc_id.replace("_", " "), (title or doc_id).replace("_", " ")]
    # keep short proper nouns from queries (skip generic)
    asset_terms += [q for q in qs if q and len(q) <= 20]
    asset_terms = _expand_aliases([t for t in asset_terms if t], max_extra=6)

    doc_driver_list = sorted({r for c in chunks for r in (c.get("drivers") or [])})
    scored = score_news_for_asset_tool.invoke({
        "asset_terms": asset_terms,
        "news_items": news,
        "doc_drivers": doc_driver_list,
        "max_keep": 30
    })

    # replace news with re-ranked subset for downstream brief
    news = scored.get("items") or news[:30]

    sentiment_brief = synthesize_sentiment_brief_tool.invoke({
        "asset": {"doc_id": doc_id, "title": title or doc_id, "terms": asset_terms},
        "scored_news": scored
    })

    if not news:
        filt_date_only = _kb_news_filter(
            symbols=None,
            tags_any=None,
            date_from_iso=date_from,
            date_to_iso=date_to,
        )
        news = query_news_vectors_tool.invoke({
            "queries": qs,
            "top_k_per_query": top_k_per_query,
            "max_total": max_total,
            "metadata_filter": filt_date_only
        })

    brief = synthesize_marketing_brief_tool.invoke({
        "doc_meta": {"doc_id": doc_id, "title": title or doc_id},
        "research_chunks": chunks,
        "news_items": news
    })
    
    return {
        "doc_id": doc_id, "title": title or doc_id,
        "queries": qs, "news": news, "brief": brief,
        "sentiment": sentiment_brief,            # <-- NEW
        "chunks_used": len(chunks),
        "classified_updates": prepared.get("updated", 0)
    }

_DEFAULT_SEEDS = [
    "crypto research report", "stablecoins", "real world assets tokenization",
    "bitcoin etf", "layer 2 scaling", "defi derivatives", "regulation enforcement", "macro liquidity"
]

@tool("list_research_docs")
def list_research_docs_tool(seed: str = "crypto research report",
                            k: int = 120,
                            extra_seeds: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    List distinct research documents discoverable in the Research KB using one or more seed probes.
    Falls back to Aurora (RESEARCH_TABLE) if KB returns none.
    Returns:
      {"docs": [{"doc_id","hit_count","title","sample_excerpt"}]}
    """
    if not RESEARCH_KB_ID:
        raise RuntimeError("RESEARCH_KB_ID is not set")

    per_query = max(1, min(int(k or 30), TOPK_LIMIT))

    # build probes
    seeds: List[str] = [seed.strip()] if seed else []
    if extra_seeds and isinstance(extra_seeds, list):
        seeds += [s for s in extra_seeds if isinstance(s, str) and s.strip()]
    elif k and k > TOPK_LIMIT:
        seeds += _DEFAULT_SEEDS
    seen, final = set(), []
    for s in seeds:
        s2 = s.strip()
        if s2 and s2.lower() not in seen:
            seen.add(s2.lower()); final.append(s2)
    if not final:
        final = ["crypto research report"]

    by_doc: Dict[str, Dict[str, Any]] = {}

    for s in final:
        matches = _kb_retrieve(RESEARCH_KB_ID, text=s, top_k=per_query)
        for m in matches:
            md  = m.get("metadata") or {}
            did = (md.get("doc_id") or "").strip()
            if not did:
                # Recover from location (e.g., s3://bucket/path/<doc_id>.json)
                did = _doc_id_from_location(m.get("location") or {})
            if not did:
                continue

            entry = by_doc.get(did)
            if not entry:
                entry = by_doc[did] = {
                    "doc_id": did,
                    "hit_count": 0,
                    "title": (md.get("title") or did).strip(),
                    "sample_excerpt": (m.get("text") or "")[:260],
                }
            entry["hit_count"] += 1

    docs = sorted(by_doc.values(), key=lambda d: d["hit_count"], reverse=True)

    # Hard fallback: directly query Aurora if KB discovery is empty
    if not docs:
        return list_research_docs_via_aurora_tool.invoke({"limit": max(120, k)})

    return {"docs": docs}

@tool("aggregate_market_briefs")
def aggregate_market_briefs_tool(per_doc_briefs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    LLM summary across all per-doc briefs.
    Input item format: {"doc_id","title","brief":{summary,highlights[],recommendations[],"mappings":[]}}
    """
    llm = _llm()
    payload = []
    for b in per_doc_briefs or []:
        br = b.get("brief") or {}
        payload.append({
            "doc_id": b.get("doc_id"),
            "title": b.get("title"),
            "summary": br.get("summary"),
            "highlights": br.get("highlights") or [],
            "recommendations": br.get("recommendations") or [],
            "news_count": len(b.get("news") or []),
        })
    sys = ("You are compiling a weekly market brief. From the per-document summaries, "
           "produce STRICT JSON with keys: overall_summary, key_themes[], action_items[]. "
           "Keep it crisp and non-redundant.")
    out = llm.invoke([("system", sys), ("user", json.dumps({"docs": payload}, ensure_ascii=False))])
    try:
        return json.loads(getattr(out, "content", "") or "{}")
    except Exception:
        # minimal fallback
        themes = []
        for p in payload:
            if p.get("summary"):
                themes.append(p["summary"][:120])
        return {"overall_summary": "Aggregate brief generated from per-document summaries.",
                "key_themes": themes[:10], "action_items": []}

@tool("run_market_agent_for_all_docs")
def run_market_agent_for_all_docs_tool(limit_docs: int = 12,
                                       days_back: int = 90) -> Dict[str, Any]:
    """
    End-to-end: iterate all research papers (up to limit_docs),
    ensure metadata is ready, fetch news, build per-doc brief, and aggregate.
    """
    docs = list_research_docs_tool.invoke({"k": max(limit_docs, TOPK_LIMIT*2)})
    print("fetched docs:", len(docs.get("docs") or []) , "docs: ", docs)
    picked = (docs.get("docs") or [])[:limit_docs]
    per_doc = []
    for d in picked:
        doc_id, title = d.get("doc_id"), d.get("title")
        try:
            res = news_for_doc_tool.invoke({"doc_id": doc_id, "title": title, "days_back": days_back})
            per_doc.append(res)
        except Exception as e:
            logger.warning(f"doc {doc_id} failed: {e}")
    final = aggregate_market_briefs_tool.invoke({"per_doc_briefs": per_doc})
    return {"count_docs": len(per_doc), "per_doc": per_doc, "final": final}

# ======================= Athena helper (unchanged) =======================

@tool("athena_news_latest")
def athena_news_latest_tool(days_back: int = 120, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch the most recent crypto news rows from Athena (table: news_agent.cryptoapi_news).
    
    Args:
        days_back: Look-back window in days from now (UTC).
        limit: Maximum number of rows to return.

    Returns:
        List of dicts with keys:
          - news_id (str)
          - title (str)
          - news_url (str)
          - source_name (str)
          - published_at (timestamp/str)
          - sentiment (str/float)
          - symbols (List[str])      # extracted from currencies_arr.symbol
          - tags (List[str])         # from api_payload_obj.tags or empty array
    """
    sql = f"""
    SELECT
      news_id, title, news_url, source_name, published_at, sentiment,
      transform(currencies_arr, x -> x.symbol) AS symbols,
      coalesce(api_payload_obj.tags, ARRAY[]) AS tags
    FROM news_agent.cryptoapi_news
    WHERE published_at >= current_timestamp - INTERVAL '{int(days_back)}' DAY
    ORDER BY published_at DESC
    LIMIT {int(limit)}
    """
    qid = athena_query(sql)
    cols = ["news_id","title","news_url","source_name","published_at","sentiment","symbols","tags"]
    return [dict(zip(cols, r)) for r in athena_rows(qid)]


@tool("synthesize_sentiment_brief")
def synthesize_sentiment_brief_tool(asset: Dict[str, Any],
                                    scored_news: Dict[str, Any],
                                    top_k: int = 10) -> Dict[str, Any]:
    """
    Produce a bullish/bearish/neutral brief focused on *why*, grouped by drivers.
    """
    llm = _llm()
    stance = scored_news.get("stance")
    net    = scored_news.get("net_score")
    items  = scored_news.get("items") or []

    feed = [{
        "title": n.get("title"),
        "date": str(n.get("published_at") or ""),
        "source": n.get("source_name"),
        "polarity": n.get("polarity"),
        "direct": n.get("direct"),
        "driver_related": n.get("driver_related")
    } for n in items[:top_k]]

    sys = ("You are a crypto market analyst. Summarize market stance for the asset below. "
           "Weigh direct headlines more than driver-only headlines, but include both. "
           "Output STRICT JSON with keys: stance, net_score, summary, bullish_drivers[], "
           "bearish_drivers[], key_headlines[]. Keep it concise and actionable.")
    user = {
        "asset": asset,
        "stance": stance,
        "net_score": net,
        "headlines": feed
    }
    out = llm.invoke([("system", sys), ("user", json.dumps(user, ensure_ascii=False))])
    try:
        return json.loads(getattr(out, "content", "") or "{}")
    except Exception:
        # safe fallback
        return {
            "stance": stance, "net_score": net,
            "summary": f"Overall stance {stance} (net={net}).",
            "bullish_drivers": [], "bearish_drivers": [],
            "key_headlines": [h["title"] for h in feed]
        }