# news_tools.py
from __future__ import annotations
import os, re, json, time, asyncio, logging, uuid, hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone


import boto3
import requests
from pydantic import BaseModel, Field, ValidationError
from langchain_core.tools import tool
from langchain_aws import ChatBedrockConverse

from tools.news_tools.storage_glue import (
    ensure_iceberg_tables as _ensure_iceberg_tables_raw,
    existing_ids as _existing_ids_raw,
    write_run_to_s3_jsonl,
    stage_to_iceberg,
    sha256,
)

# (Optional—kept for future S3 Vectors usage)
from vectors.s3vectors_client import S3Vectors  # noqa: F401
from vectors.embeddings import embed_texts      # noqa: F401

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AWS_REGION      = os.getenv("AWS_REGION", "us-east-1")
NEWS_KB_ID      = os.getenv("NEWS_KB_ID")        # required
NEWS_KB_DS_ID   = os.getenv("NEWS_KB_DS_ID")     # required
NEWS_KB_BUCKET  = os.getenv("NEWS_KB_BUCKET")    # required
NEWS_KB_PREFIX  = (os.getenv("NEWS_KB_PREFIX") or "news/").strip("/")

_s3    = boto3.client("s3", region_name=AWS_REGION)
_kb_cp = boto3.client("bedrock-agent", region_name=AWS_REGION)
_kb_rt = boto3.client("bedrock-agent-runtime", region_name=AWS_REGION)

BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

def _require_env() -> None:
    missing = [k for k, v in {
        "NEWS_KB_ID": NEWS_KB_ID,
        "NEWS_KB_DS_ID": NEWS_KB_DS_ID,
        "NEWS_KB_BUCKET": NEWS_KB_BUCKET,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")
    
def _mk_attrs(meta: dict) -> dict:
    # same structure you already use in sidecars
    # {"metadataAttributes": {"as_of":{"value":{"type":"STRING","stringValue":"..."}} ...}}
    return build_kb_sidecar(meta)["metadataAttributes"]

def kb_direct_ingest_news(rows: list[dict]) -> list[dict]:
    """Directly ingest into the KB; returns list of per-doc results."""
    results = []
    batch = []
    for r in rows:
        text = _best_fulltext(r)[:240_000]
        news_id = r["news_id"]
        as_of_iso = r.get("published_at_iso")
        as_of_epoch = _iso_to_epoch(as_of_iso)

        raw_meta = _clean_meta({
            "news_id": news_id,
            "url": r.get("news_url"),
            "headline": r.get("title"),
            "source": r.get("source_name"),
            "as_of": as_of_iso,
            "as_of_epoch": as_of_epoch,
            "symbols": sorted({c.get("symbol") for c in (r.get("currencies") or []) if c.get("symbol")}),
            "tags": r.get("tags") or _derive_tags(r),
            "sentiment": r.get("sentiment"),
        })

        batch.append({
            "documentId": news_id,                     # <- YOUR stable ID
            "content": {"text": text},                 # or {"byteContent": ...}
            "metadataAttributes": _mk_attrs(raw_meta), # same schema you already build
        })

        if len(batch) == 25:                           # API limit per call
            resp = _kb_rt.ingest_knowledge_base_documents(
                knowledgeBaseId=NEWS_KB_ID, documents=batch
            )
            results.extend(resp.get("documentResults", []))
            batch.clear()

    if batch:
        resp = _kb_rt.ingest_knowledge_base_documents(
            knowledgeBaseId=NEWS_KB_ID, documents=batch
        )
        results.extend(resp.get("documentResults", []))
    return results

def _s3_key_for_doc(news_id: str) -> str:
    return f"{NEWS_KB_PREFIX}/{news_id}.txt"

def _s3_key_for_meta(news_id: str) -> str:
    return f"{NEWS_KB_PREFIX}/{news_id}.txt.metadata.json"

def _best_fulltext(row: dict) -> str:
    payload = row.get("api_payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    body = (row.get("full_text") or payload.get("text") or "").strip()
    if not body:
        body = ((row.get("title") or "") + "\n" + (payload.get("snippet") or "")).strip()
    title = (row.get("title") or "").strip()
    combo = f"{title}\n\n{body}" if title and body else (title or body or "")
    return combo.strip()

def build_kb_sidecar(meta: dict) -> dict:
    """
    Bedrock S3 sidecar schema:
      {
        "metadataAttributes": {
          "<key>": {
            "value": {
              "type": "STRING|NUMBER|BOOLEAN|STRING_LIST",
              "stringValue" | "numberValue" | "booleanValue" | "stringListValue": ...
            },
            "includeForEmbedding": true|false   # optional
          },
          ...
        }
      }
    """
    def to_attr(v):
        if isinstance(v, bool):
            return {"value": {"type": "BOOLEAN", "booleanValue": v}, "includeForEmbedding": True}
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return {"value": {"type": "NUMBER", "numberValue": float(v)}, "includeForEmbedding": True}
        if isinstance(v, (list, tuple)):
            lst = [str(x) for x in v if str(x)]
            # Bedrock requires at least 1 item if you provide stringListValue
            if not lst:
                return None
            return {"value": {"type": "STRING_LIST", "stringListValue": lst}, "includeForEmbedding": True}
        return {"value": {"type": "STRING", "stringValue": str(v)}, "includeForEmbedding": True}
    out = {"metadataAttributes": {}}
    for k, v in (meta or {}).items():
        if v in (None, "", [], {}):
            continue
        attr = to_attr(v)
        if attr:
            out["metadataAttributes"][k] = attr
    return out

def _kb_filter(symbols: List[str] | None = None,
               date_from_iso: str | None = None,
               date_to_iso:   str | None = None) -> dict | None:
    """
    Build Bedrock retrieve() filter for S3 KB:
      symbols      -> listContains
      as_of_epoch  -> numeric comparisons
    """
    clauses: List[dict] = []

    if symbols:
        sym_or = [{"listContains": {"key": "symbols", "value": s}} for s in symbols if s]
        if len(sym_or) == 1:
            clauses.append(sym_or[0])
        elif sym_or:
            clauses.append({"orAll": sym_or})

    # NEW: convert ISO window to numeric epoch and compare on as_of_epoch
    if date_from_iso:
        ep_from = _iso_to_epoch(date_from_iso)
        if ep_from is not None:
            clauses.append({"greaterThanOrEquals": {"key": "as_of_epoch", "value": ep_from}})
    if date_to_iso:
        ep_to = _iso_to_epoch(date_to_iso)
        if ep_to is not None:
            clauses.append({"lessThanOrEquals": {"key": "as_of_epoch", "value": ep_to}})

    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"andAll": clauses}

# ---------------- Types ----------------
from tools.news_tools.models import (
    TokenMention, EnsureOut, FetchOut, DedupeOut, ScrapeOut,
    ExtractOneOut, IndexVectorsOut, PersistBronzeOut, PersistIcebergOut
)

# --------------- Utils -----------------
from datetime import timezone
from email.utils import parsedate_to_datetime

def _normalize_rfc2822_to_iso(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return raw

def _clean_meta(meta: dict) -> dict:
    """Drop null/empty values and empty arrays from metadata."""
    cleaned = {}
    for k, v in meta.items():
        if isinstance(v, list):
            vv = [x for x in v if x not in (None, "", [], {})]
            if vv:
                cleaned[k] = vv
        elif v not in (None, "", [], {}):
            cleaned[k] = v
    return cleaned

def _chunk(s: str, max_len: int = 1400) -> List[str]:
    s = (s or "").strip()
    if len(s) <= max_len:
        return [s]
    chunks, i = [], 0
    while i < len(s):
        j = min(i + max_len, len(s))
        k = s.rfind(".", i, j)
        cut = k + 1 if k != -1 and k > i + 400 else j
        chunks.append(s[i:cut].strip()); i = cut
    return [c for c in chunks if c]

_COMMON_ASSET_DICT: Dict[str, str] = {
    "BTC":"Bitcoin","ETH":"Ethereum","SOL":"Solana","XRP":"XRP","DOGE":"Dogecoin",
    "ADA":"Cardano","DOT":"Polkadot","MATIC":"Polygon","AVAX":"Avalanche","BNB":"BNB",
    "SUI":"Sui","ARB":"Arbitrum","OP":"Optimism","LINK":"Chainlink","LTC":"Litecoin","TON":"Toncoin",
}
_NAME_TO_SYMBOL = {v.lower(): k for k, v in _COMMON_ASSET_DICT.items()}
_TICKER_RE = re.compile(r"\$(?P<sym>[A-Z]{2,10})\b")

def _regex_symbol_name_hints(text: str) -> List[TokenMention]:
    hits: List[TokenMention] = []
    for m in _TICKER_RE.finditer(text or ""):
        sym = m.group("sym").upper()
        if sym in _COMMON_ASSET_DICT:
            hits.append(TokenMention(name=_COMMON_ASSET_DICT[sym], symbol=sym, confidence=0.55))
        else:
            hits.append(TokenMention(name=sym, symbol=sym, confidence=0.4))
    lowered = (text or "").lower()
    for name_lower, sym in _NAME_TO_SYMBOL.items():
        if name_lower in lowered:
            hits.append(TokenMention(name=_COMMON_ASSET_DICT[sym], symbol=sym, confidence=0.6))
    uniq: Dict[Tuple[str, Optional[str]], TokenMention] = {}
    for h in hits:
        key = (h.name, h.symbol)
        if key not in uniq or h.confidence > uniq[key].confidence:
            uniq[key] = h
    return list(uniq.values())

def _iso_to_epoch(iso_str: Optional[str]) -> Optional[int]:
    """Return seconds since epoch (int) for an ISO/RFC2822-like string; None if parse fails."""
    if not iso_str:
        return None
    s = iso_str.strip()
    # ISO8601 first
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception:
        pass
    # RFC 2822 fallback
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None



# --------------- Tools -----------------
@tool("ensure_iceberg_tables")
def ensure_iceberg_tables_tool() -> EnsureOut:
    """Idempotently create Iceberg tables used by the agent."""
    _ensure_iceberg_tables_raw()
    return EnsureOut(ok=True)

@tool("fetch_news_api")
def fetch_news_api_tool(api_url: str, timeout_s: int = 15) -> FetchOut:
    """Fetch CryptoNews-style API and normalize primary fields."""
    r = requests.get(api_url, timeout=timeout_s, verify=False)
    r.raise_for_status()
    payload = r.json()
    items: List[Dict[str, Any]] = []
    for it in (payload.get("data") or []):
        if not isinstance(it, dict):
            continue
        row = {**it}
        row.setdefault("currencies", [])
        row["news_id"] = sha256(row.get("news_url") or row.get("title") or "")
        row["sentiment"] = it.get("sentiment")
        row["published_at_iso"] = _normalize_rfc2822_to_iso(row.get("date"))
        items.append(row)
    return FetchOut(items=items)

@tool("dedupe_news_ids")
def dedupe_news_ids_tool(items: List[Dict[str, Any]]) -> DedupeOut:
    """Return items that are not yet present in Iceberg (by news_id)."""
    existing = set(_existing_ids_raw([x["news_id"] for x in items]))
    to_process = [x for x in items if x["news_id"] not in existing]
    return DedupeOut(to_process=to_process, dedup_skipped=len(items) - len(to_process))

@tool("scrape_article_text")
def scrape_article_text_tool(url: str, timeout_s: int = 15) -> ScrapeOut:
    """Requests+readability scrape with AMP/JSON-LD fallback.
    Never raises; returns empty text so caller can fallback."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from readability import Document
    from bs4 import BeautifulSoup

    if not (isinstance(url, str) and url.startswith("http")):
        return ScrapeOut(text="")

    sess = requests.Session()
    retry = Retry(
        total=2, connect=2, read=2, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"), raise_on_status=False
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://",  HTTPAdapter(max_retries=retry))

    try:
        r = sess.get(url, timeout=(8, 10), verify=False, allow_redirects=True, headers=BROWSER_HEADERS)
    except requests.RequestException as e:
        logger.info(f"scrape error for {url}: {e}")
        return ScrapeOut(text="")
    if r.status_code >= 400:
        logger.info(f"{url} returned {r.status_code}; fallback path.")
        return ScrapeOut(text="")

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
            objs = data if isinstance(data, list) else [data]
            for o in objs:
                if isinstance(o, dict) and o.get("@type") in ("Article", "NewsArticle"):
                    body = o.get("articleBody")
                    if body and len(body) > 800:
                        return ScrapeOut(text=body[:120_000])
        except Exception:
            pass

    # AMP
    link = soup.find("link", rel=lambda v: v and "amphtml" in v.lower())
    amp_url = link.get("href") if link and link.has_attr("href") else None
    if amp_url:
        try:
            ra = sess.get(amp_url, timeout=(8, 10), verify=False, allow_redirects=True, headers=BROWSER_HEADERS)
            if ra.status_code < 400:
                doca = Document(ra.text)
                from bs4 import BeautifulSoup as BS2
                soupa = BS2(doca.summary(html_partial=True), "html.parser")
                texta = "\n".join(t.get_text(strip=True) for t in soupa.select("p, h1, h2, h3, li") if t.get_text(strip=True))
                if len(texta) > 800:
                    return ScrapeOut(text=texta[:120_000])
        except Exception:
            pass

    # Readability
    try:
        doc = Document(html)
        soup2 = BeautifulSoup(doc.summary(html_partial=True), "html.parser")
        text = "\n".join(
            t.get_text(strip=True)
            for t in soup2.select("article p, .article p, .post p, p, li")
            if t.get_text(strip=True)
        )
        if len(text) < 600:
            raw = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
            if len(raw) > len(text):
                text = raw
        return ScrapeOut(text=text[:120_000])
    except Exception as e:
        logger.info(f"readability failed for {url}: {e}")
        return ScrapeOut(text="")

# --------- LLM extractor tool (structured) ----------
def _make_llm() -> ChatBedrockConverse:
    br = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    return ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=0.3,
        client=br,
    )
_LLM = _make_llm().with_structured_output(ExtractOneOut)

@tool("llm_extract_mentions")
def llm_extract_mentions_tool(title: str = "", source: str = "", url: str = "", body: str = "") -> ExtractOneOut:
    """Extract {name, symbol, confidence} via LLM (structured)."""
    from langchain_core.prompts import ChatPromptTemplate
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         "Extract crypto assets mentioned. Return JSON with a key 'tokens'. "
         "Each item must have fields: name, symbol, confidence (0..1)."),
        ("human",
         "Extract coins/tokens/chains mentioned in the article text. "
         "Return up to 8 distinct, high-confidence items.\n\n"
         "Article Title: {title}\nSource: {source}\nURL: {url}\n\n"
         "Text (truncated):\n{body}\n"),
    ])
    chain = prompt | _LLM
    try:
        return chain.invoke({"title": title, "source": source, "url": url, "body": body[:180_000]})
    except Exception as e:
        logger.info(f"LLM extract failed for {url or title}: {e}")

def _attrs_from_meta(meta: dict) -> list[dict]:
    """Convert your meta dict to direct-ingest inlineAttributes array."""
    out = []
    for k, v in (meta or {}).items():
        if v in (None, "", [], {}): 
            continue
        if isinstance(v, bool):
            out.append({"key": k, "value": {"type": "BOOLEAN", "booleanValue": v}})
        elif isinstance(v, (int, float)):
            out.append({"key": k, "value": {"type": "NUMBER", "numberValue": float(v)}})
        elif isinstance(v, (list, tuple)):
            out.append({"key": k, "value": {"type": "STRING_LIST", "stringListValue": [str(x) for x in v if str(x)]}})
        else:
            out.append({"key": k, "value": {"type": "STRING", "stringValue": str(v)}})
    return out

EMBED_MODEL = os.getenv("EMBED_MODEL", "e5-small")

def _norm_for_hash(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

def _chunk_key(news_id: str, chunk_text: str) -> str:
    h = hashlib.blake2b(_norm_for_hash(chunk_text).encode("utf-8"), digest_size=8).hexdigest()
    return f"news#{news_id}#m:{EMBED_MODEL}#v1#h:{h}"

# ============= KB: ingest & retrieve =============
@tool("kb_ingest_news")
def kb_ingest_news_tool(rows: List[dict],
                        wait: bool = False,
                        poll_seconds: int = 5,
                        timeout_seconds: int = 900) -> dict:
    """
    Write news docs + Bedrock-compliant sidecar JSON to S3 (for the KB data source),
    then StartIngestionJob. Optionally wait for completion.
    Returns: {uploaded, ingestion_job_id, status}
    """
    _require_env()
    _require_env()
    if not rows:
        return {"uploaded": 0, "status": "NOOP"}

    uploaded = 0
    for r in rows:
        news_id = r.get("news_id") or sha256((r.get("news_url") or r.get("title") or str(uuid.uuid4())))
        text = _best_fulltext(r)

        symbols = sorted({c.get("symbol") for c in (r.get("currencies") or []) if isinstance(c, dict) and c.get("symbol")})
        as_of_iso   = r.get("published_at_iso")
        as_of_epoch = _iso_to_epoch(as_of_iso)

        raw_meta = _clean_meta({
            "news_id":    news_id,
            "url":        r.get("news_url"),
            "headline":   r.get("title"),
            "source":     r.get("source_name"),
            "as_of":      as_of_iso,          # display string
            "as_of_epoch":as_of_epoch,        # numeric for filtering
            "symbols":    symbols,            # string list
            "tags":       r.get("tags") or _derive_tags(r),
            "sentiment":  r.get("sentiment"),
        })
        sidecar = build_kb_sidecar(raw_meta)

        # (1) upload the TEXT doc
        _s3.put_object(
            Bucket=NEWS_KB_BUCKET,
            Key=_s3_key_for_doc(news_id),            # e.g., news/<id>.txt
            Body=(text or "").encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        # (2) upload the SIDECAR JSON
        sidecar_bytes = json.dumps(sidecar, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(sidecar_bytes) > 10_000:
            sidecar_bytes = sidecar_bytes[:9_900]  # belt & suspenders; or drop optional fields
        _s3.put_object(
            Bucket=NEWS_KB_BUCKET,
            Key=_s3_key_for_meta(news_id),          # e.g., news/<id>.txt.metadata.json
            Body=json.dumps(sidecar, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
)
        uploaded += 1

    # 3) Start ingestion job
    resp = _kb_cp.start_ingestion_job(
        knowledgeBaseId=NEWS_KB_ID,
        dataSourceId=NEWS_KB_DS_ID,
        description=f"news batch: {uploaded} items"
    )
    job_id = resp["ingestionJob"]["ingestionJobId"]

    if not wait:
        return {"uploaded": uploaded, "ingestion_job_id": job_id, "status": "STARTED"}

    # Optional poll
    t0 = time.time()
    status = None
    while time.time() - t0 < timeout_seconds:
        j = _kb_cp.get_ingestion_job(
            knowledgeBaseId=NEWS_KB_ID,
            dataSourceId=NEWS_KB_DS_ID,
            ingestionJobId=job_id
        )["ingestionJob"]
        status = j["status"]
        if status in ("COMPLETE", "FAILED", "ERROR", "STOPPED"):
            break
        time.sleep(poll_seconds)

    return {"uploaded": uploaded, "ingestion_job_id": job_id, "status": status or "UNKNOWN"}

@tool("kb_retrieve_news")
def kb_retrieve_news_tool(query: str,
                          top_k: int = 8,
                          symbols: List[str] | None = None,
                          date_from_iso: str | None = None,
                          date_to_iso: str | None = None) -> dict:
    """
    Query the Bedrock KB; returns [{text, s3_uri, score?, metadata?}].
    You can filter by symbols (list of tickers) and/or date window.
    """
    _require_env()
    vf = _kb_filter(symbols=symbols, date_from_iso=date_from_iso, date_to_iso=date_to_iso)
    rcfg = {
        "vectorSearchConfiguration": {
            "numberOfResults": int(top_k),
            **({"filter": vf} if vf else {}),
        }
    }

    out = []
    resp = _kb_rt.retrieve(
        knowledgeBaseId=NEWS_KB_ID,
        retrievalConfiguration=rcfg,
        retrievalQuery={"text": query},
    )
    for r in resp.get("retrievalResults", []):
        loc = r.get("location", {}) or {}
        s3loc = loc.get("s3Location", {}) or {}
        content = r.get("content", {}) or {}
        item = {
            "text": content.get("text"),
            "s3_uri": s3loc.get("uri"),
        }
        if "score" in r:
            item["score"] = r["score"]
        if "metadata" in r:
            item["metadata"] = r["metadata"]
        out.append(item)
    return {"results": out}

# ===== Persist to your data lake =====
def _norm_tag(s: str) -> str:
    return (s or "").strip().lower()

def _derive_tags(r: dict) -> list[str]:
    tags = set()
    for k in ("tags", "categories", "topics"):
        for t in (r.get(k) or []):
            if isinstance(t, str) and t.strip():
                tags.add(_norm_tag(t))
    for c in (r.get("currencies") or []):
        sym  = (c.get("symbol") or "").strip()
        name = (c.get("name")   or "").strip()
        if sym:  tags.add(sym.upper())
        if name: tags.add(_norm_tag(name))
    for w in (r.get("title") or "").split():
        if len(w) > 3:
            tags.add(_norm_tag(w))
    out = [t for t in tags if t]
    out.sort()
    return out[:32]

@tool("persist_bronze")
def persist_bronze_tool(rows: List[Dict[str, Any]], extractor_temperature: float = 0.3) -> PersistBronzeOut:
    """Write bronze JSONL for news + extraction metadata."""
    if not rows:
        return PersistBronzeOut(count=0)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    bronze_news_rows, bronze_reason_rows = [], []
    for r in rows:
        r_tags = r.get("tags") or _derive_tags(r)
        pub_iso = r.get("published_at_iso")
        pub_epoch = _iso_to_epoch(pub_iso)
        bronze_news_rows.append({
            "news_id": r["news_id"],
            "news_url": r.get("news_url"),
            "title": r.get("title"),
            "source_name": r.get("source_name"),
            "published_at": r.get("published_at_iso"),
            "published_at_epoch": pub_epoch, 
            "sentiment": r.get("sentiment"),
            "api_payload": {**r, "tags": r_tags},
            "currencies": r.get("currencies", []),
            "first_seen_at": now_iso,
            "last_seen_at": now_iso,
        })
        bronze_reason_rows.append({
            "news_id": r["news_id"],
            "extractor_model": "anthropic.claude-3-haiku-20240307-v1:0",
            "temperature": extractor_temperature,
            "prompt_version": "v1",
            "evidence": r.get("_evidence", []),
            "created_at": now_iso,
        })
    write_run_to_s3_jsonl(bronze_news_rows, bronze_reason_rows)
    return PersistBronzeOut(count=len(rows))

@tool("persist_iceberg")
def persist_iceberg_tool(rows: List[Dict[str, Any]], extractor_temperature: float = 0.3) -> PersistIcebergOut:
    """Upsert rows into Iceberg via staged JSON arrays (typed)."""
    if not rows:
        return PersistIcebergOut(count=0)

    def _norm_currencies(lst):
        out = []
        for it in (lst or []):
            if not isinstance(it, dict):
                name = str(it)
                out.append({"name": name, "symbol": name, "confidence": None})
                continue
            name = it.get("name") or it.get("title") or it.get("coin") or it.get("symbol")
            sym  = it.get("symbol") or it.get("code") or it.get("ticker")
            conf = it.get("confidence")
            try:
                conf = float(conf) if conf is not None else None
            except Exception:
                conf = None
            out.append({"name": name, "symbol": sym, "confidence": conf})
        return [x for x in out if (x.get("name") or x.get("symbol"))]

    iceberg_rows, meta_rows = [], []
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for r in rows:
        r_tags = r.get("tags") or _derive_tags(r)
        pub_iso = r.get("published_at_iso")
        pub_epoch = _iso_to_epoch(pub_iso)

        payload_obj = {
            "news_url":  r.get("news_url"),
            "title":     r.get("title"),
            "date_iso":  r.get("published_at_iso"),
            "date_epoch": pub_epoch,  
            "source":    r.get("source_name"),
            "sentiment": r.get("sentiment"),
            "tags":      [t for t in r_tags if isinstance(t, str)],
        }

        payload_json    = json.dumps(payload_obj, ensure_ascii=False)
        currencies_json = json.dumps(_norm_currencies(r.get("currencies", [])), ensure_ascii=False)

        iceberg_rows.append({
            "news_id":      r["news_id"],
            "news_url":     r.get("news_url"),
            "title":        r.get("title"),
            "source_name":  r.get("source_name"),
            "published_at": r.get("published_at_iso"),
            "sentiment":    r.get("sentiment"),
            "api_payload":  payload_json,
            "currencies":   currencies_json,
        })

        meta_rows.append({
            "news_id": r["news_id"],
            "extractor_model": "anthropic.claude-3-haiku-20240307-v1:0",
            "temperature": extractor_temperature,
            "prompt_version": "v1",
            "evidence": json.dumps(r.get("_evidence", []), ensure_ascii=False),
        })

    stage_to_iceberg(iceberg_rows, meta_rows)
    return PersistIcebergOut(count=len(rows))

# Utility used by graph to merge regex hints + LLM tokens
def merge_hints_and_tokens(text: str, tokens: List[TokenMention]) -> List[Dict[str, Any]]:
    hints = _regex_symbol_name_hints(text)
    merged: Dict[Tuple[str, Optional[str]], TokenMention] = {}
    for h in hints + tokens:
        key = (h.name.strip(), (h.symbol or "").strip() or None)
        if key not in merged or h.confidence > merged[key].confidence:
            merged[key] = h
    return [{"name": t.name, "symbol": t.symbol, "confidence": float(t.confidence)} for t in merged.values()]


@tool("kb_direct_ingest_news_tool")
def kb_direct_ingest_news_tool(rows: List[dict], batch_size: int = 25) -> dict:
    """
    Directly ingest documents into the Bedrock Knowledge Base (no S3 sync).
    Returns per-document results for precise error reporting.

    Input rows should contain at least:
      - news_id (str)  [if absent, will be derived]
      - title, source_name, news_url, published_at_iso (optional)
      - full_text (optional) or anything _best_fulltext(...) can assemble
      - currencies (list[dict{name,symbol,confidence}]) (optional)
      - tags (list[str]) (optional)
      - sentiment (str) (optional)

    Output:
      {
        "results": [
          {"documentId": "...", "status": "SUCCESS|FAILED", "statusReason": "..."}
        ],
        "ok": <int>,
        "failed": <int>,
        "took_ms": <int>
      }
    """
    _require_env()
    if not rows:
        return {"results": [], "ok": 0, "failed": 0, "took_ms": 0}

    t0 = time.time()
    results: List[Dict[str, Any]] = []

    def _mk_doc(r: dict) -> dict:
        # 1) doc id
        news_id = r.get("news_id") or sha256(
            (r.get("news_url") or r.get("title") or str(uuid.uuid4()))
        )

        # 2) content (keep reasonable length)
        text = (r.get("full_text") or _best_fulltext(r) or "")[:240_000]

        # 3) metadata (same schema as your sidecars)
        as_of_iso = r.get("published_at_iso")
        as_of_epoch = _iso_to_epoch(as_of_iso)

        currencies = r.get("currencies") or []
        symbols = sorted({
            (c.get("symbol") or "").strip()
            for c in currencies
            if isinstance(c, dict) and c.get("symbol")
        })

        raw_meta = _clean_meta({
            "news_id":   news_id,
            "url":       r.get("news_url"),
            "headline":  r.get("title"),
            "source":    r.get("source_name"),
            "as_of":     as_of_iso,
            "as_of_epoch": as_of_epoch,
            "symbols":   symbols,
            "tags":      r.get("tags") or _derive_tags(r),
            "sentiment": r.get("sentiment"),
        })

        # Build Bedrock metadataAttributes
        meta_attrs = build_kb_sidecar(raw_meta).get("metadataAttributes", {})

        # OPTIONAL: mark a couple of fields for embedding signal
        for k in ("headline", "symbols"):
            if k in meta_attrs:
                meta_attrs[k]["includeForEmbedding"] = True

        return {
            "documentId": news_id,
            "content": {"text": text},
            "metadataAttributes": meta_attrs,
        }

    # assemble docs
    docs: List[dict] = [_mk_doc(r) for r in rows]

    # 4) send in batches (API limit: 25 docs per call)
    i = 0
    while i < len(docs):
        batch = docs[i:i + int(batch_size or 25)]
        try:
            resp = _kb_rt.ingest_knowledge_base_documents(
                knowledgeBaseId=NEWS_KB_ID,
                documents=batch,
            )
            # Normalize response
            for d in (resp.get("documentResults") or []):
                results.append({
                    "documentId": d.get("documentId"),
                    "status": d.get("status") or d.get("documentStatus"),
                    "statusReason": d.get("statusReason") or d.get("reason"),
                })
        except Exception as e:
            # If the batch call itself fails, mark all in the batch as failed with same reason
            for d in batch:
                results.append({
                    "documentId": d.get("documentId"),
                    "status": "FAILED",
                    "statusReason": f"{type(e).__name__}: {e}",
                })
        i += len(batch)

    ok = sum(1 for r in results if (r.get("status") or "").upper() == "SUCCESS")
    failed = sum(1 for r in results if (r.get("status") or "").upper() != "SUCCESS")

    return {
        "results": results,
        "ok": ok,
        "failed": failed,
        "took_ms": int((time.time() - t0) * 1000),
    }

@tool("kb_direct_ingest_news_tool")
def kb_direct_ingest_news_tool(rows: List[dict],
                               data_source_id: Optional[str] = None,
                               batch_size: int = 25) -> dict:
    """
    Directly ingest docs into a KB CUSTOM data source (no S3 sync).
    Returns per-document statuses.
    """
    _require_env()
    ds_id = data_source_id or NEWS_KB_DS_ID  # must be a CUSTOM data source
    if not rows:
        return {"results": [], "ok": 0, "failed": 0}

    docs = []
    for r in rows:
        news_id = r.get("news_id") or sha256((r.get("news_url") or r.get("title") or str(uuid.uuid4())))

        text = (r.get("full_text") or _best_fulltext(r) or "")[:240_000]

        as_of_iso   = r.get("published_at_iso")
        as_of_epoch = _iso_to_epoch(as_of_iso)
        currencies  = r.get("currencies") or []
        symbols     = sorted({(c.get("symbol") or "").strip() for c in currencies if isinstance(c, dict) and c.get("symbol")})

        raw_meta = _clean_meta({
            "news_id":   news_id,
            "url":       r.get("news_url"),
            "headline":  r.get("title"),
            "source":    r.get("source_name"),
            "as_of":     as_of_iso,
            "as_of_epoch": as_of_epoch,
            "symbols":   symbols,
            "tags":      r.get("tags") or _derive_tags(r),
            "sentiment": r.get("sentiment"),
        })

        docs.append({
            "metadata": {
                "type": "IN_LINE_ATTRIBUTE",
                "inlineAttributes": _attrs_from_meta(raw_meta),
            },
            "content": {
                "dataSourceType": "CUSTOM",
                "custom": {
                    "customDocumentIdentifier": {"id": news_id},
                    "sourceType": "IN_LINE",
                    "inlineContent": {
                        "type": "TEXT",
                        "textContent": {"data": text or " "}
                    }
                }
            }
        })

    results = []
    i = 0
    t0 = time.time()
    while i < len(docs):
        batch = docs[i:i+batch_size]
        try:
            resp = _kb_cp.ingest_knowledge_base_documents(  # <-- CONTROL PLANE
                knowledgeBaseId=NEWS_KB_ID,
                dataSourceId=ds_id,
                documents=batch,
            )
            for d in resp.get("documentDetails", []):
                ident = d.get("identifier", {}).get("custom", {}) or {}
                results.append({
                    "documentId": ident.get("id"),
                    "status": d.get("status"),
                    "statusReason": d.get("statusReason", "")
                })
        except Exception as e:
            for b in batch:
                bid = b["content"]["custom"]["customDocumentIdentifier"]["id"]
                results.append({"documentId": bid, "status": "FAILED", "statusReason": f"{type(e).__name__}: {e}"})
        i += len(batch)

    ok = sum(1 for r in results if (r.get("status") or "").upper() in {"INDEXED","PENDING","IN_PROGRESS"})
    failed = sum(1 for r in results if (r.get("status") or "").upper() in {"FAILED","METADATA_UPDATE_FAILED"})
    return {"results": results, "ok": ok, "failed": failed, "took_ms": int((time.time()-t0)*1000)}