# graph.py — robust pipeline with Glue/Athena persistence, dedupe, and typed tools
import asyncio
import json
import re
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import boto3
from pydantic import BaseModel, Field, ValidationError
from langchain_aws import ChatBedrockConverse
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool  # <-- make tools available

# allow TLS-off calls to API
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import requests

# Glue/Athena storage helpers
from storage_glue import (
    ensure_iceberg_tables as _ensure_iceberg_tables_raw,
    existing_ids as _existing_ids_raw,
    write_run_to_s3_jsonl,
    stage_to_iceberg,
    sha256,
)

# --- scraping stack
try:
    from playwright.async_api import async_playwright
    _PW = True
except Exception:
    _PW = False

try:
    import requests
    from bs4 import BeautifulSoup
    from readability import Document
    _REQS = True
except Exception:
    _REQS = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------------------------------
# Structured extraction models
# ------------------------------
class TokenMention(BaseModel):
    name: str
    symbol: Optional[str] = None
    confidence: float = Field(ge=0, le=1)
    

class ExtractionResult(BaseModel):
    tokens: List[TokenMention] = Field(default_factory=list)

# ------------------------------
# Utilities
# ------------------------------
_COMMON_ASSET_DICT: Dict[str, str] = {
    "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana", "XRP": "XRP",
    "DOGE": "Dogecoin", "ADA": "Cardano", "DOT": "Polkadot", "MATIC": "Polygon",
    "AVAX": "Avalanche", "BNB": "BNB", "SUI": "Sui", "ARB": "Arbitrum",
    "OP": "Optimism", "LINK": "Chainlink", "LTC": "Litecoin", "TON": "Toncoin",
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

# ------------------------------
# Scraping
# ------------------------------

# add near scraping helpers
def _extract_amp_and_ld(html: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (amp_url, article_body_from_ldjson) if discovered."""
    from bs4 import BeautifulSoup
    import json
    amp_url = None
    article_body = None
    soup = BeautifulSoup(html, "html.parser")

    # 1) AMP link
    link = soup.find("link", rel=lambda v: v and "amphtml" in v.lower())
    if link and link.get("href"):
        amp_url = link["href"]

    # 2) JSON-LD Article object
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
            # JSON-LD may be an array or object
            objs = data if isinstance(data, list) else [data]
            for o in objs:
                if isinstance(o, dict) and (o.get("@type") in ("Article", "NewsArticle")):
                    body = o.get("articleBody")
                    if body and len(body) > 400:  # keep only meaningful bodies
                        article_body = body
                        return amp_url, article_body
        except Exception:
            continue
    return amp_url, article_body

async def _scrape_with_playwright(url: str, timeout_s: int = 15) -> str:
    if not _PW:
        raise RuntimeError("Playwright not available")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )

        # Block heavy resources
        await context.route("**/*", lambda route: route.abort()
                            if route.request.resource_type in {"image","font","media"} else route.continue_())

        page = await context.new_page()
        page.set_default_navigation_timeout(timeout_s * 1000)
        try:
            await page.goto(url, wait_until="domcontentloaded")

            # auto-accept simple cookie banners
            for sel in ['button[aria-label*="accept"]', 'button:has-text("Accept")', 'button:has-text("I agree")']:
                try:
                    b = page.locator(sel)
                    if await b.count() > 0:
                        await b.first.click(timeout=1000)
                        break
                except Exception:
                    pass

            # prefer article/main text
            try:
                await page.wait_for_selector("article, main", timeout=3000)
                # innerText preserves line breaks better than textContent for some sites
                article_text = await page.locator("article, main").inner_text(timeout=2000)
                if article_text and len(article_text) > 800:
                    return article_text[:120_000]
            except Exception:
                pass

            # fallback: readability on rendered HTML
            html = await page.content()
            from readability import Document
            doc = Document(html)
            content_html = doc.summary(html_partial=True)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content_html, "html.parser")
            text = "\n".join(t.get_text(strip=True) for t in soup.select("p, h1, h2, h3, li") if t.get_text(strip=True))

            # ultimate fallback: whole page innerText
            if len(text) < 600:
                body_text = await page.evaluate("document.body.innerText")
                if body_text and len(body_text) > len(text):
                    text = body_text

            return text[:120_000]
        finally:
            await context.close()
            await browser.close()

def _scrape_with_requests(url: str, timeout_s: int = 15) -> str:
    if not _REQS:
        raise RuntimeError("requests/readability not available")
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    sess = requests.Session()
    retry = Retry(total=2, connect=2, read=2, backoff_factor=0.4,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "HEAD"), raise_on_status=False)
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://", HTTPAdapter(max_retries=retry))

    # 1) primary fetch
    r = sess.get(url, timeout=(8, 10), verify=False)
    r.raise_for_status()
    html = r.text

    # JSON-LD articleBody wins
    amp_url, ld_body = _extract_amp_and_ld(html)
    if ld_body and len(ld_body) > 800:
        return ld_body[:120_000]

    # 2) AMP fetch (often cleaner)
    if amp_url:
        try:
            ra = sess.get(amp_url, timeout=(8, 10), verify=False)
            ra.raise_for_status()
            amp_html = ra.text
            from readability import Document
            doca = Document(amp_html)
            content_html = doca.summary(html_partial=True)
            from bs4 import BeautifulSoup
            soupa = BeautifulSoup(content_html, "html.parser")
            texta = "\n".join(t.get_text(strip=True) for t in soupa.select("p, h1, h2, h3, li") if t.get_text(strip=True))
            if len(texta) > 800:
                return texta[:120_000]
        except Exception:
            pass

    # 3) Readability on original
    from readability import Document
    doc = Document(html)
    content_html = doc.summary(html_partial=True)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(content_html, "html.parser")
    text = "\n".join(t.get_text(strip=True) for t in soup.select("article p, .article p, .post p, p, li") if t.get_text(strip=True))

    # If still too short, fallback to raw visible text
    if len(text) < 600:
        raw = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
        if len(raw) > len(text):
            text = raw

    return text[:120_000]

async def scrape_article_text(url: str, timeout_s: int = 15) -> str:
    try:
        if _PW:
            return await _scrape_with_playwright(url, timeout_s)
    except Exception as e:
        logger.warning(f"Playwright failed for {url}: {e}")
    try:
        return _scrape_with_requests(url, timeout_s)
    except Exception as e:
        logger.warning(f"requests/readability failed for {url}: {e}")
        return ""

# ------------------------------
# Tools (typed) for SQL/storage + date normalization
# ------------------------------
class EnsureTablesOut(BaseModel):
    ok: bool

@tool("ensure_iceberg_tables")
def ensure_iceberg_tables_tool() -> EnsureTablesOut:
    """Idempotently create Iceberg tables used by the agent."""
    _ensure_iceberg_tables_raw()
    return EnsureTablesOut(ok=True)

class ExistingIdsIn(BaseModel):
    ids: List[str]

class ExistingIdsOut(BaseModel):
    existing: List[str]

@tool("athena_existing_ids")
def athena_existing_ids_tool(ids: List[str]) -> ExistingIdsOut:
    """Return the subset of ids that already exist in the Iceberg news table."""
    return ExistingIdsOut(existing=list(_existing_ids_raw(ids)))

class NormalizePublishedAtIn(BaseModel):
    raw: Optional[str] = None

class NormalizePublishedAtOut(BaseModel):
    iso8601: Optional[str] = None

@tool("normalize_published_at")
def normalize_published_at_tool(raw: Optional[str]) -> NormalizePublishedAtOut:
    """Normalize API 'date' like 'Tue, 12 Aug 2025 09:49:06 -0400' to ISO8601 `YYYY-MM-DDTHH:MM:SSZ`."""
    if not raw:
        return NormalizePublishedAtOut(iso8601=None)
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return NormalizePublishedAtOut(iso8601=dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        # last resort: leave as-is; Athena will try to cast but may NULL it
        return NormalizePublishedAtOut(iso8601=raw)

# ------------------------------
# LLM extractor
# ------------------------------
def _make_llm_extractor(temperature: float = 0.3, region: str = "us-east-1"):
    bedrock = boto3.client("bedrock-runtime", region_name=region)
    llm = ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=temperature,  # slight randomness for non-deterministic buckets
        client=bedrock,
    )
    return llm.with_structured_output(ExtractionResult)

def _coerce_tokens_maybe(obj: Any) -> List[TokenMention]:
    """
    Guardrail: if the model returns ['BTC','ETH',...] instead of structured objects,
    coerce them into TokenMention with default confidence.
    """
    try:
        if isinstance(obj, ExtractionResult):
            return obj.tokens
        if isinstance(obj, dict) and "tokens" in obj:
            return [TokenMention(**t) for t in obj["tokens"]]  # may raise & go to except
        if isinstance(obj, list):
            out = []
            for x in obj:
                if isinstance(x, dict):
                    out.append(TokenMention(**x))
                elif isinstance(x, str):
                    # best-effort name; symbol unknown
                    out.append(TokenMention(name=x, symbol=None, confidence=0.7))
            return out
    except ValidationError:
        pass
    return []

# ------------------------------
# Core pipeline
# ------------------------------
async def enrich_news_api(
    api_url: str,
    *,
    max_articles: int = 50,
    timeout_s: int = 15,
    extractor_temperature: float = 0.3,
) -> Dict[str, Any]:
    if not _REQS:
        raise RuntimeError("requests is required for API fetch")

    # Ensure Glue/Athena Iceberg tables (via tool to validate IO)
    ensure_iceberg_tables_tool.invoke({})

    # Fetch CryptoNews API (TLS verify disabled as requested)
    resp = requests.get(api_url, timeout=20, verify=False)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError("Unexpected API response shape")

    items: List[Dict[str, Any]] = []
    for it in payload.get("data", []):
        if isinstance(it, dict):
            row = {**it}
            row.setdefault("currencies", [])
            row["news_id"] = sha256(row.get("news_url") or row.get("title") or "")

            # ✅ Add sentiment from API if present
            row["sentiment"] = it.get("sentiment")

            # normalize publish date now, so SQL never sees RFC-2822
            norm = normalize_published_at_tool.invoke({"raw": row.get("date")})
            row["published_at_iso"] = (
                getattr(norm, "iso8601", None)
                if hasattr(norm, "iso8601")
                else norm.get("iso8601")
            )

            items.append(row)

    # Deduplicate against Iceberg (typed tool)
    already_out = athena_existing_ids_tool.invoke({"ids": [x["news_id"] for x in items]})
    print("already_out ", already_out)
    already = set(getattr(already_out, "existing", already_out.existing))
    to_process = [x for x in items if x["news_id"] not in already]
    if (skipped := len(items) - len(to_process)) > 0:
        logger.info(f"Skipping {skipped} already-processed items")

    extractor = _make_llm_extractor(temperature=extractor_temperature)
    prompt = ChatPromptTemplate.from_messages([
        ("system", "Extract crypto assets mentioned. Return JSON with a 'tokens' list of {{name, symbol, confidence}}."),
        ("human",
         "Extract coins/tokens/chains mentioned in the article text. "
         "Return up to 8 distinct, high-confidence items.\n\n"
         "Article Title: {title}\nSource: {source}\nURL: {url}\n\n"
         "Text (truncated):\n{body}\n"),
    ])
    chain = prompt | extractor

    sem = asyncio.Semaphore(5)

    async def process(item: Dict[str, Any]) -> Dict[str, Any]:
        url = item.get("news_url", "")
        text = ""
        if isinstance(url, str) and url.startswith("http"):
            text = await scrape_article_text(url, timeout_s=timeout_s)
        if not text:
            text = f"{item.get('title','')}\n\n{item.get('text','')}"

        hints = _regex_symbol_name_hints(text)
        tokens: List[TokenMention] = []
        evidence = []  # placeholder

        try:
            res = chain.invoke({
                "title": item.get("title", ""),
                "source": item.get("source_name", ""),
                "url": url,
                "body": text[:60_000],
            })
            # res is ExtractionResult due to with_structured_output, but defend anyway:
            tokens = _coerce_tokens_maybe(res)
        except Exception as e:
            logger.info(f"LLM extraction failed for {url or item.get('title','(no url)')}: {e}")

        merged: Dict[Tuple[str, Optional[str]], TokenMention] = {}
        for h in hints + tokens:
            key = (h.name.strip(), (h.symbol or "").strip() or None)
            if key not in merged or h.confidence > merged[key].confidence:
                merged[key] = h

        out = {**item}
        out["full_text"] = text[:240_000]
        out["currencies"] = [
            {"name": tm.name, "symbol": tm.symbol, "confidence": round(float(tm.confidence), 3)}
            for tm in merged.values()
        ]
        out["_evidence"] = evidence
        return out

    enriched_new = await asyncio.gather(*[process(it) for it in to_process[:max_articles]])

    # Stitch final return: previously-seen items pass through unchanged
    by_id = {x["news_id"]: x for x in enriched_new}
    enriched_all = [by_id.get(it["news_id"], it) for it in items]

    # --- Persist results ---
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    bronze_news_rows, bronze_reason_rows = [], []
    for r in enriched_new:
        bronze_news_rows.append({
            "news_id": r["news_id"],
            "news_url": r.get("news_url"),
            "title": r.get("title"),
            "source_name": r.get("source_name"),
            "published_at": r.get("published_at_iso"), 
            "sentiment": r.get("sentiment"),  # normalized
            "api_payload": r,                 # full enriched record
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

    if bronze_news_rows:
        n_s3, r_s3 = write_run_to_s3_jsonl(bronze_news_rows, bronze_reason_rows)
        logger.info(f"Bronze written: {n_s3} , {r_s3}")

    # Iceberg upsert (MERGE) via staged JSON arrays
    iceberg_rows, meta_rows = [], []
    for r in enriched_new:
        iceberg_rows.append({
            "news_id": r["news_id"],
            "news_url": r.get("news_url"),
            "title": r.get("title"),
            "source_name": r.get("source_name"),
            "published_at": r.get("published_at_iso"),
            "sentiment": r.get("sentiment"),  
            "api_payload": json.dumps(r, ensure_ascii=False),
            "currencies": json.dumps(r.get("currencies", []), ensure_ascii=False),
        })
        meta_rows.append({
            "news_id": r["news_id"],
            "extractor_model": "anthropic.claude-3-haiku-20240307-v1:0",
            "temperature": extractor_temperature,
            "prompt_version": "v1",
            "evidence": json.dumps(r.get("_evidence", []), ensure_ascii=False),
        })

    if iceberg_rows:
        stage_to_iceberg(iceberg_rows, meta_rows)
        logger.info(f"Upserted {len(iceberg_rows)} rows into Iceberg")

    return {
        "data": enriched_all,
        "total_pages": payload.get("total_pages"),
        "total_items": payload.get("total_items"),
    }

# ------------------------------
# CLI entry
# ------------------------------
async def _main():
    url = os.environ.get("CRYPTONEWS_URL")
    if not url:
        print("Set CRYPTONEWS_URL to a fully-formed endpoint.")
        return
    result = await enrich_news_api(url)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(_main())
