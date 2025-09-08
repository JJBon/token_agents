# tools/market_hybrid_tools.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import re
import math
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse

from langchain_core.tools import tool
from langchain_aws import ChatBedrockConverse
from tools.athena_client import query as athena_query, rows as athena_rows

from vectors.s3vectors_client import S3Vectors
from vectors.embeddings import embed_texts

# =============================================================================
# Configuration
# =============================================================================

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# --- S3Vectors (ANN indexes) ---
VEC_BUCKET         = os.getenv("VEC_BUCKET")
VEC_INDEX_NEWS     = os.getenv("VEC_INDEX_NEWS")                 # news index
VEC_INDEX_RESEARCH = os.getenv("VEC_INDEX_RESEARCH")# research index

_s3v_news     = S3Vectors(region=AWS_REGION, bucket=VEC_BUCKET, index=VEC_INDEX_NEWS)
_s3v_research = S3Vectors(region=AWS_REGION, bucket=VEC_BUCKET, index=VEC_INDEX_RESEARCH)

TOPK_LIMIT = 30  # service cap

# =============================================================================
# Small utils
# =============================================================================

def _collect_text(content):
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, dict) and "text" in b:
                parts.append(b["text"])
            else:
                parts.append(str(b))
        return "".join(parts)
    return str(content or "")

def _parse_relaxed_json(s: str) -> dict:
    txt = _collect_text(s).strip()
    if txt.startswith("```"):
        txt = re.sub(r"^```[a-zA-Z]*\n?", "", txt)
        txt = re.sub(r"\n```$", "", txt)
    start, end = txt.find("{"), txt.rfind("}")
    candidate = txt[start:end+1] if (start != -1 and end != -1 and end > start) else txt
    try:
        return json.loads(candidate)
    except Exception:
        pass
    try:
        mid = json.loads(candidate)
        if isinstance(mid, str):
            return json.loads(mid)
    except Exception:
        pass
    candidate2 = re.sub(r",\s*([}\]])", r"\1", candidate)
    candidate2 = candidate2.replace("“", '"').replace("”", '"').replace("’", "'")
    return json.loads(candidate2)

def _canon_url(u: str) -> str:
    try:
        p = urlparse(u or "")
        netloc = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/").lower()
        return f"{netloc}{path}"
    except Exception:
        return (u or "").strip().lower()

def _fingerprint(title: str, url: str) -> Tuple[str, str]:
    t = " ".join((title or "").strip().lower().split())
    c = _canon_url(url or "")
    return (t, c)

def _iter_matches(resp):
    if isinstance(resp, dict):
        if "matches" in resp: return resp["matches"]
        if "results" in resp: return resp["results"]
    return resp

def _to_score(m: Dict[str, Any]) -> float:
    if "score" in m:
        try:
            return float(m["score"])
        except Exception:
            pass
    if "distance" in m:
        try:
            d = float(m["distance"])
            return 1.0 / (1.0 + max(0.0, d))
        except Exception:
            return 0.0
    return 0.0

def _call_s3_query(client: S3Vectors, vec, top_k: int, flt: Optional[Dict[str, Any]] = None):
    """Try common signatures: (vec, top_k, filt) → (vec, top_k) → (vec,) → ()."""
    variants = []
    if flt is not None:
        variants.append((vec, top_k, flt))
    variants.extend([(vec, top_k), (vec,), ()])
    for args in variants:
        try:
            return client.query(*args)
        except TypeError:
            continue
    raise RuntimeError("S3Vectors.query signature not supported by this client.")

# =============================================================================
# Embeddings + research key parsing
# =============================================================================

def _embed(texts: List[str]) -> List[List[float]]:
    vecs = embed_texts(texts, region=AWS_REGION) or []
    return list(vecs) if isinstance(vecs, list) else [list(vecs)]

# research key: research#<doc_id>#m:e5-small#v1#h:<16hex>
_KEY_RX = re.compile(r"^research#(?P<doc>[^#]+)#m:[^#]+#v\d+#h:(?P<h>[0-9a-f]{16})$")

def _chunk_id_from_key_or_meta(key: str, md: Dict[str, Any]) -> str:
    m = _KEY_RX.match(key or "")
    if m:
        return f"research#{m.group('doc')}#h:{m.group('h')}"
    if md.get("doc_id") and md.get("ord") is not None:
        return f"research#{md['doc_id']}#ord:{int(float(md['ord']))}"
    return key or md.get("id") or ""

# =============================================================================
# Tools
# =============================================================================

@tool("retrieve_research_chunks")
def retrieve_research_chunks_tool(
    doc_id: Optional[str] = None,
    s3_uri: Optional[str] = None,
    title_hint: Optional[str] = None,
    k: int = 10,
) -> Dict[str, Any]:
    """
    Retrieve up to k research chunks from the S3 Vectors *research* index.
    - Try doc_id filter first (safe).
    - Fallback to unfiltered query if empty.
    - If s3_uri provided, prefer matches from that source when available.
    Returns: {"doc_ref": <string>, "chunks": [{"chunk_id","text","score"}]}
    """
    k = max(1, min(int(k or 10), TOPK_LIMIT))

    # Seed for embedding
    probes: List[str] = []
    if doc_id: probes.append(str(doc_id))
    if s3_uri:
        probes.append(s3_uri)
        base = os.path.basename(s3_uri.rstrip("/"))
        if base: probes.append(base)
    if title_hint and title_hint.strip():
        probes.append(title_hint.strip())
    if not probes:
        probes = ["research document"]

    vec = _embed([probes[0]])[0]

    # Safe filter: only doc_id (avoid 'type' and other keys some backends reject)
    flt: Optional[Dict[str, Any]] = {"doc_id": str(doc_id)} if doc_id else None

    # First try (maybe filtered)
    try:
        raw = _call_s3_query(_s3v_research, vec, k, flt) if flt else _call_s3_query(_s3v_research, vec, k)
    except Exception as e:
        # If backend complains about filters, retry unfiltered
        if "invalid query filter" in str(e).lower():
            raw = _call_s3_query(_s3v_research, vec, k)
        else:
            raise

    matches = _iter_matches(raw) or []

    # If nothing came back, retry UNFILTERED with a slightly larger fetch
    if not matches:
        fetch_k = min(TOPK_LIMIT, max(k * 3, k))
        raw = _call_s3_query(_s3v_research, vec, fetch_k)
        matches = _iter_matches(raw) or []

    # Prefer items whose metadata.source_s3 mentions our file basename (if any)
    base = os.path.basename((s3_uri or "").rstrip("/")).lower() if s3_uri else ""
    def _src_ok(md: Dict[str, Any]) -> bool:
        if not base:
            return False
        src = (
            md.get("source_s3")
            or md.get("s3_uri")
            or md.get("source")
            or md.get("bedrock-kb-source-uri")
            or ""
        )
        return base in str(src).lower()

    preferred, others = [], []
    for m in matches:
        if not isinstance(m, dict):
            continue
        key  = m.get("key") or m.get("id")
        md   = m.get("metadata") or {}
        text = (md.get("excerpt") or md.get("text") or "").strip()
        if not text:
            continue

        item = {
            "chunk_id": _chunk_id_from_key_or_meta(key, md),
            "text": text,
            "score": float(_to_score(m)),
            "_prefer": _src_ok(md),
        }
        (preferred if item["_prefer"] else others).append(item)

    # If we found any from the same source, use those; otherwise use best overall
    items = preferred if preferred else others
    # De-dupe by chunk_id, keep best score
    best: Dict[str, Dict[str, Any]] = {}
    for it in items:
        cid = it["chunk_id"]
        if (cid not in best) or (it["score"] > best[cid]["score"]):
            best[cid] = it
    out_items = list(best.values())
    out_items.sort(key=lambda x: x["score"], reverse=True)
    for it in out_items:
        it.pop("_prefer", None)

    return {"doc_ref": (s3_uri or title_hint or doc_id or ""), "chunks": out_items[:k]}

@tool("query_news_vectors")
def query_news_vectors_tool(
    queries: List[str],
    top_k_per_query: int = 30,
    max_total: int = 200,
    metadata_filter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve news via S3Vectors ANN index.
    Aggregates per news_id (best score), annotates with match_queries/match_count.
    """
    if not queries:
        return []

    flt = metadata_filter or {"doc_type": "news"}  # news ingests use doc_type
    qlist = [q.strip() for q in queries if q and q.strip()]
    if not qlist:
        return []

    qvecs = embed_texts(qlist, region=AWS_REGION) or []
    if not isinstance(qvecs, list):
        qvecs = list(qvecs)
    limit = min(len(qlist), len(qvecs))
    if limit == 0:
        return []

    by_id: Dict[str, Dict[str, Any]] = {}
    by_fp: Dict[Tuple[str, str], str] = {}

    top_k_per_query = max(1, min(int(top_k_per_query or 30), TOPK_LIMIT))

    for i in range(limit):
        q = qlist[i]
        vec = qvecs[i]

        raw = _call_s3_query(_s3v_news, vec, top_k_per_query, flt)
        for m in (_iter_matches(raw) or []):
            if not isinstance(m, dict):
                continue

            meta = m.get("metadata") or {}
            key = m.get("key") or m.get("id") or meta.get("news_id")

            news_id = (meta.get("news_id") or key or "").strip()
            title = (meta.get("headline") or meta.get("title") or "").strip()
            news_url = (meta.get("url") or meta.get("news_url") or "").strip()
            source_name = (meta.get("source") or meta.get("source_name") or "").strip()

            published_at = meta.get("published_at") or meta.get("date") or meta.get("as_of")
            if isinstance(published_at, (int, float)):
                published_at = datetime.utcfromtimestamp(published_at).isoformat()
            elif published_at:
                published_at = str(published_at)
            else:
                published_at = None

            if not news_id:
                if not title and not news_url:
                    continue
                news_id = f"{title[:40]}|{news_url}"

            score = float(_to_score(m))

            entry = by_id.get(news_id)
            if not entry or score > entry["score"]:
                by_id[news_id] = {
                    "news_id": news_id,
                    "title": title,
                    "tags": meta.get("tags") or [],
                    "symbols": meta.get("symbols") or [],
                    "published_at": published_at,
                    "news_url": news_url,
                    "source_name": source_name,
                    "score": score,
                    "match_queries": {q},
                }
            else:
                entry["match_queries"].add(q)

            if title or news_url:
                fp = _fingerprint(title, news_url)
                chosen = by_fp.get(fp)
                if chosen is None or by_id[news_id]["score"] > by_id[chosen]["score"]:
                    by_fp[fp] = news_id

    keep_ids = set(by_fp.values()) if by_fp else set(by_id.keys())
    items: List[Dict[str, Any]] = []
    for nid, v in by_id.items():
        if nid not in keep_ids:
            continue
        mq = sorted(v.pop("match_queries"))
        v["match_queries"] = mq
        v["match_count"] = len(mq)
        v["_rank"] = (v["score"], v["match_count"])
        items.append(v)

    items.sort(key=lambda it: it["_rank"], reverse=True)
    for it in items:
        it.pop("_rank", None)

    return items[:max_total]

# =============================================================================
# Semantic pairing (NO hard-coded topics)
# =============================================================================

def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a))
    nb = math.sqrt(sum(y*y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)

def _build_semantic_pairs(
    research_chunks: List[Dict[str, Any]],
    news_items: List[Dict[str, Any]],
    top_per_news: int = 1,
    min_sim: float = 0.18,
) -> Dict[str, List[Tuple[str, float]]]:
    """
    Returns mapping: news_id -> [(best_chunk_id, similarity), ...] (size <= top_per_news)
    """
    chunk_ids: List[str] = []
    chunk_texts: List[str] = []
    for c in (research_chunks or []):
        cid = c.get("chunk_id")
        txt = (c.get("text") or "")[:600]
        if cid is None or not txt:
            continue
        chunk_ids.append(str(cid))
        chunk_texts.append(txt)

    if not chunk_ids:
        return {}

    news_ids: List[str] = []
    news_texts: List[str] = []
    for n in (news_items or []):
        nid = n.get("news_id")
        if not nid:
            continue
        title = n.get("title") or ""
        tags  = " ".join(n.get("tags") or [])
        symbols = " ".join((n.get("symbols") or []))
        news_txt = " ".join([title, tags, symbols])[:200]
        if not news_txt.strip():
            continue
        news_ids.append(str(nid))
        news_texts.append(news_txt)

    if not news_ids:
        return {}

    chunk_vecs = _embed(chunk_texts)
    news_vecs  = _embed(news_texts)

    out: Dict[str, List[Tuple[str, float]]] = {}
    for i, nid in enumerate(news_ids):
        row: List[Tuple[str, float]] = []
        for j, cid in enumerate(chunk_ids):
            sim = _cosine(news_vecs[i], chunk_vecs[j])
            if sim >= min_sim:
                row.append((cid, sim))
        row.sort(key=lambda t: t[1], reverse=True)
        out[nid] = row[:max(1, int(top_per_news))]
    return out

# =============================================================================
# LLM synthesis
# =============================================================================

@tool("select_news_queries")
def select_news_queries_tool(chunks: List[Dict[str, Any]], min_q: int = 5, max_q: int = 8) -> Dict[str, Any]:
    """Return 5–8 short query strings (<= 12 words) from research chunks."""
    sample = [{"id": c.get("chunk_id"), "text": (c.get("text") or "")[:600]} for c in (chunks or [])[:10]]

    llm = ChatBedrockConverse(model="anthropic.claude-3-haiku-20240307-v1:0")
    sys = (
        "You read research excerpts and output ONLY a JSON object "
        'with key "queries": an array of 5–8 short search phrases (<=12 words), '
        "focused on concrete entities/claims suitable to match news titles."
    )
    msg = [("system", sys), ("user", json.dumps({"chunks": sample}, ensure_ascii=False))]

    out = llm.invoke(msg)
    content = getattr(out, "content", "") or ""
    try:
        data = json.loads(content)
        qs = [q.strip() for q in data.get("queries", []) if isinstance(q, str) and q.strip()]
        return {"queries": qs[:max_q]}
    except Exception:
        fallback: List[str] = []
        for c in sample:
            txt = c.get("text") or ""
            line = txt.split(".")[0][:80].strip()
            if line:
                fallback.append(line)
            if len(fallback) >= max_q:
                break
        return {"queries": fallback}

@tool("synthesize_marketing_brief")
def synthesize_marketing_brief_tool(
    doc_meta: Dict[str, Any],
    research_chunks: List[Dict[str, Any]],
    news_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Returns JSON: {summary, highlights[], recommendations[], mappings[{news_id, chunk_id, rationale}]}
    """
    llm = ChatBedrockConverse(model="anthropic.claude-3-haiku-20240307-v1:0")

    allowed_news_ids  = {str(n.get("news_id")) for n in (news_items or []) if n.get("news_id")}
    allowed_chunk_ids = {str(c.get("chunk_id")) for c in (research_chunks or []) if c.get("chunk_id") is not None}

    r_chunks = [
        {"id": c.get("chunk_id"), "text": (c.get("text") or "")[:600]}
        for c in (research_chunks or [])[:10]
        if c.get("chunk_id") is not None
    ]
    n_items = [
        {
            "news_id": n.get("news_id"),
            "title": n.get("title"),
            "tags": n.get("tags", []),
            "symbols": n.get("symbols", []),
            "date": str(n.get("published_at") or n.get("date") or n.get("as_of") or ""),
            "source": n.get("source_name"),
            "url": n.get("news_url"),
            "score": float(n.get("score", 0.0)),
        }
        for n in (news_items or [])[:20]
        if n.get("news_id")
    ]

    print("news items to be consumed: ", n_items)
    print("research items to be consumed: ", r_chunks)

    candidates = _build_semantic_pairs(research_chunks, news_items, top_per_news=1, min_sim=0.18)
    lm_pairs = [{"news_id": nid, "chunk_id": cid, "similarity": round(sim, 3)}
                for nid, lst in candidates.items() for (cid, sim) in lst]

    sys = (
    "You are a precise analyst. ONLY use the provided news_items and research_chunks.\n"
    "- Do not introduce entities not present in research_chunks or news_items.\n"
    "- If news_items is empty, return short 'summary' and EMPTY arrays for 'highlights', 'recommendations', 'mappings'.\n"
    "- Output STRICT JSON: summary, highlights[], recommendations[], mappings[{news_id, chunk_id, rationale<=30w}].\n"
    "- IMPORTANT: mappings must come only from match_candidates."
    )
    payload = {
        "doc": {"doc_id": doc_meta.get("doc_id"), "title": doc_meta.get("title")},
        "research_chunks": r_chunks,
        "news_items": n_items,
        "match_candidates": lm_pairs,
    }

    out = llm.invoke([("system", sys), ("user", json.dumps(payload, ensure_ascii=False))])
    content = getattr(out, "content", "")

    try:
        data = _parse_relaxed_json(content)

        if not allowed_news_ids:
            return {"summary": "No relevant news were found for this research document in the current run.",
                    "highlights": [], "recommendations": [], "mappings": []}

        summary = data.get("summary", "")
        highlights = [str(h).strip() for h in (data.get("highlights") or []) if str(h).strip()][:10]
        recommendations = [str(r).strip() for r in (data.get("recommendations") or []) if str(r).strip()][:10]

        allowed_pairs = {(nid, cid) for nid, lst in candidates.items() for (cid, _) in lst}
        cleaned, seen_news = [], set()

        raw_m = data.get("mappings") or []
        if raw_m:
            for m in raw_m:
                if not isinstance(m, dict): 
                    continue
                nid_raw = m.get("news_id")
                cid_raw = m.get("chunk_id")
                nid = str(nid_raw) if nid_raw is not None else ""
                cid = str(cid_raw) if cid_raw is not None else ""
                if nid in allowed_news_ids and cid in allowed_chunk_ids and (nid, cid) in allowed_pairs and nid not in seen_news:
                    rationale = " ".join((m.get("rationale") or "").split()[:30]).strip()
                    cleaned.append({"news_id": nid, "chunk_id": cid_raw, "rationale": rationale})
                    seen_news.add(nid)
                if len(cleaned) >= 12:
                    break

        if len(cleaned) < min(12, len(candidates)):
            for nid, lst in candidates.items():
                if not lst or nid in seen_news:
                    continue
                cid, sim = lst[0]
                if nid in allowed_news_ids and cid in allowed_chunk_ids:
                    rationale = f"Semantic match (sim≈{sim:.2f}) between headline and this chunk."
                    cleaned.append({"news_id": nid, "chunk_id": cid, "rationale": rationale})
                    seen_news.add(nid)
                if len(cleaned) >= 12:
                    break

        return {"summary": summary, "highlights": highlights, "recommendations": recommendations, "mappings": cleaned}

    except Exception:
        if not news_items:
            return {"summary": "No relevant news were found for this research document in the current run.",
                    "highlights": [], "recommendations": [], "mappings": []}
        # fallback: semantic-only mappings
        candidates = _build_semantic_pairs(research_chunks, news_items, top_per_news=1, min_sim=0.18)
        cleaned, seen = [], set()
        allowed_news = {str(n.get("news_id")) for n in (news_items or [])}
        allowed_chunks = {str(c.get("chunk_id")) for c in (research_chunks or []) if c.get("chunk_id") is not None}
        for nid, lst in candidates.items():
            if not lst or nid in seen:
                continue
            cid, sim = lst[0]
            if str(nid) in allowed_news and str(cid) in allowed_chunks:
                cleaned.append({"news_id": str(nid), "chunk_id": cid, "rationale": f"Semantic match (sim≈{sim:.2f})."})
                seen.add(nid)
            if len(cleaned) >= 12:
                break

        return {"summary": "Related news and research were provided; using semantic matching due to LLM parse failure.",
                "highlights": [], "recommendations": [], "mappings": cleaned}

@tool("select_news_queries_from_athena")
def select_news_queries_from_athena_tool(
    research_chunks: list,
    athena_news: list,
    min_q: int = 6,
    max_q: int = 12,
) -> dict:
    """
    Read research chunks + Athena latest news (title,tags). Return 6–12 search phrases.
    """
    rc = [{"id": c.get("chunk_id"), "text": (c.get("text") or "")[:500]} for c in (research_chunks or [])[:10]]
    nw = []
    for n in (athena_news or [])[:200]:
        nw.append({
            "title": n.get("title") or "",
            "tags": n.get("tags") or [],
            "symbols": n.get("symbols") or [],
            "date": str(n.get("published_at") or ""),
            "source": n.get("source_name") or "",
        })

    llm = ChatBedrockConverse(model="anthropic.claude-3-haiku-20240307-v1:0")
    sys = (
        "You are a careful planner. Read research excerpts and recent crypto headlines/tags. "
        "Choose 6–12 short, high-signal search phrases (<=12 words) to retrieve relevant articles from a semantic news index. "
        "Prefer exact or lightly-trimmed headlines; include distinctive entities (protocols, tokens, regulations). "
        "Avoid generic market recaps, hype, airdrops, price-only headlines, and off-topic tech news. "
        "Return STRICT JSON: {\"queries\": [\"...\"]} with no extra text."
    )
    user = {"research_chunks": rc, "athena_news": nw}
    out = llm.invoke([("system", sys), ("user", json.dumps(user, ensure_ascii=False))])
    content = getattr(out, "content", "") or ""
    try:
        data = json.loads(content)
        qs, seen = [], set()
        for q in data.get("queries", []):
            if not isinstance(q, str): 
                continue
            q2 = q.strip()
            if not q2:
                continue
            if len(q2.split()) > 12:
                q2 = " ".join(q2.split()[:12])
            if q2.lower() in seen:
                continue
            seen.add(q2.lower()); qs.append(q2)
        return {"queries": qs[:max_q]}
    except Exception:
        titles, seen = [], set()
        for n in nw:
            t = (n.get("title") or "").strip()
            if t and t.lower() not in seen:
                seen.add(t.lower()); titles.append(t)
            if len(titles) >= max_q: 
                break
        return {"queries": titles}

@tool("athena_news_latest")
def athena_news_latest_tool(days_back: int = 120, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch most recent crypto news rows from Athena (no title/tag filters).
    """
    sql = f"""
    SELECT
      news_id,
      title,
      news_url,
      source_name,
      published_at,
      sentiment,
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

_DOC_FROM_CHUNK_RX = re.compile(r"^research#(?P<doc>[^#]+)#")

def _doc_from_chunk_id(cid: str) -> Optional[str]:
    m = _DOC_FROM_CHUNK_RX.match(str(cid or ""))
    return m.group("doc") if m else None

_DEFAULT_DOC_SEEDS = [
    "crypto research report",
    "stablecoins payments rails",
    "real world assets tokenization",
    "bitcoin etf institutional adoption",
    "layer 2 scaling",
    "defi derivatives staking",
    "regulation policy enforcement",
    "macro liquidity cycle"
]

@tool("list_research_docs")
def list_research_docs_tool(seed: str = "crypto research report",
                            k: int = 120,
                            extra_seeds: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Query the research index with one or more seeds and aggregate by doc_id.
    Returns: {"docs": [{"doc_id","hit_count","title","sample_excerpt"}]}
    """
    # per-query clamp to the service limit
    per_query = max(1, min(int(k or 30), TOPK_LIMIT))  # TOPK_LIMIT=30

    seeds: List[str] = [seed.strip()] if seed else []
    # If caller asked for more than 30, diversify probes to approximate >30 results
    if extra_seeds and isinstance(extra_seeds, list):
        seeds += [s for s in extra_seeds if isinstance(s, str) and s.strip()]
    elif k and k > TOPK_LIMIT:
        # use built-ins when caller implicitly wants more breadth
        seeds += _DEFAULT_DOC_SEEDS

    # final clean-up
    seen_s, final_seeds = set(), []
    for s in seeds:
        s2 = s.strip()
        if s2 and s2.lower() not in seen_s:
            seen_s.add(s2.lower()); final_seeds.append(s2)

    by_doc: Dict[str, Dict[str, Any]] = {}

    for s in final_seeds or ["crypto research report"]:
        vec = _embed([s])[0]
        raw = _call_s3_query(_s3v_research, vec, per_query)  # NEVER >30
        matches = _iter_matches(raw) or []
        for m in matches:
            meta = (m.get("metadata") or {})
            did  = (meta.get("doc_id") or "").strip()
            if not did:
                cid = _chunk_id_from_key_or_meta(m.get("key") or m.get("id") or "", meta)
                # derive doc_id from chunk id if needed
                if cid.startswith("research#"):
                    did = cid.split("#", 2)[1]
                if not did:
                    continue
            entry = by_doc.get(did)
            if not entry:
                entry = by_doc[did] = {
                    "doc_id": did,
                    "hit_count": 0,
                    "title": (meta.get("title") or did).strip(),
                    "sample_excerpt": (meta.get("excerpt") or meta.get("text") or "")[:260],
                }
            entry["hit_count"] += 1

    docs = sorted(by_doc.values(), key=lambda d: d["hit_count"], reverse=True)
    return {"docs": docs}