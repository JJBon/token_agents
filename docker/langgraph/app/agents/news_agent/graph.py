# graph.py  —  robust pipeline (no LLM controller)
# Keeps the LLM for token extraction, removes tool-routing/JSON parsing failures.

import asyncio
import json
import re
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
import os

import boto3
from pydantic import BaseModel, Field

from langchain_aws import ChatBedrockConverse
from langchain_core.prompts import ChatPromptTemplate

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

def _fetch_news_api(url: str) -> Dict[str, Any]:
    """Fetch CryptoNews API JSON from a fully-formed endpoint; returns its parsed dict."""
    if not _REQS:
        raise RuntimeError("requests is required for fetch_news_api")

    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    sess = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://", HTTPAdapter(max_retries=retry))

    resp = sess.get(url, timeout=(8, 12), verify=False)  # TLS verification disabled
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError("Unexpected response shape; expected an object with 'data' array")

    fixed = []
    for item in payload.get("data", []):
        if isinstance(item, dict):
            item = {**item}
            item.setdefault("currencies", [])
            # Keep API's own snippet around as a fallback signal for LLM when scraping fails
            item.setdefault("_api_text", item.get("text", "") or "")
            fixed.append(item)
    return {"data": fixed, "total_pages": payload.get("total_pages"), "total_items": payload.get("total_items")}

async def _scrape_with_playwright(url: str, timeout_s: int = 15) -> str:
    if not _PW:
        raise RuntimeError("Playwright not available")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
            locale="en-US",
        )
        page = await context.new_page()
        page.set_default_navigation_timeout(timeout_s * 1000)
        try:
            await page.goto(url, wait_until="domcontentloaded")
            # settle a bit but don't hang forever
            try:
                await page.wait_for_load_state("networkidle", timeout=timeout_s * 1000)
            except Exception:
                pass
            html = await page.content()
            doc = Document(html)
            content_html = doc.summary(html_partial=True)
            soup = BeautifulSoup(content_html, "html.parser")
            text = "\n".join(
                t.get_text(strip=True) for t in soup.select("p, h1, h2, h3, li") if t.get_text(strip=True)
            )
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
    retry = Retry(
        total=2, connect=2, read=2, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://", HTTPAdapter(max_retries=retry))

    resp = sess.get(url, timeout=(8, 10), verify=False)  # TLS off + connect/read tuple
    resp.raise_for_status()
    html = resp.text

    doc = Document(html)
    content_html = doc.summary(html_partial=True)
    soup = BeautifulSoup(content_html, "html.parser")
    text = "\n".join(
        t.get_text(strip=True) for t in soup.select("p, h1, h2, h3, li") if t.get_text(strip=True)
    )
    return text[:120_000]

async def scrape_article_text(url: str, timeout_s: int = 15) -> str:
    """Try Playwright first, then requests; return '' on failure."""
    try:
        if _PW:
            return await _scrape_with_playwright(url, timeout_s)
    except Exception as e:
        logger.warning(f"Playwright failed for {url}: {e}")
    try:
        return _scrape_with_requests(url, timeout_s)
    except Exception as e:
        logger.warning(f"requests/readability failed for {url}: {e}")
        return ""  # we'll fall back to API-provided title/text fields

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
# Core pipeline (no controller)
# ------------------------------
def _make_llm_extractor(temperature: float = 0.3, region: str = "us-east-1"):
    bedrock = boto3.client("bedrock-runtime", region_name=region)
    llm = ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=temperature,  # small > 0 for slight non-determinism
        client=bedrock,
    )
    return llm.with_structured_output(ExtractionResult)

async def enrich_news_api(
    api_url: str,
    *,
    max_articles: int = 50,
    timeout_s: int = 15,
    extractor_temperature: float = 0.3,
) -> Dict[str, Any]:
    if not _REQS:
        raise RuntimeError("requests is required for API fetch")

    # 1) Fetch CryptoNews API (do NOT try to “scrape” this URL)
    api_payload = _fetch_news_api(api_url)
    items_in = api_payload["data"]

    # 2) Build extractor LLM & prompt chain
    extractor = _make_llm_extractor(temperature=extractor_temperature)
    prompt = ChatPromptTemplate.from_messages([
        ("system", "Extract crypto assets mentioned. Return JSON with a 'tokens' list."),
        ("human",
         "Extract coins/tokens/chains mentioned in the article text. "
         "Return up to 8 distinct, high-confidence items.\n\n"
         "Article Title: {title}\nSource: {source}\nURL: {url}\n\n"
         "Text (truncated):\n{body}\n"),
    ])
    chain = prompt | extractor

    # 3) Enrich each article concurrently
    sem = asyncio.Semaphore(4)

    async def process(item: Dict[str, Any]) -> Dict[str, Any]:
        url = item.get("news_url", "")
        if not (isinstance(url, str) and url.startswith("http")):
            url = ""  # don't scrape non-article/invalid URLs

        # scrape page text (best effort)
        text = ""
        if url:
            async with sem:
                text = await scrape_article_text(url, timeout_s=timeout_s)

        # fallback body for extraction if scraping failed
        if not text.strip():
            text = f"{item.get('title','')}\n\n{item.get('text','') or item.get('_api_text','')}"

        # seed with heuristics
        hints = _regex_symbol_name_hints(text)

        # LLM extraction (run in a thread so it doesn't block the event loop)
        tokens: List[TokenMention] = []
        try:
            res: ExtractionResult = await asyncio.to_thread(
                chain.invoke,
                {
                    "title": item.get("title", ""),
                    "source": item.get("source_name", ""),
                    "url": url,
                    "body": text[:20_000],
                },
            )
            tokens = res.tokens
        except Exception as e:
            logger.info(f"LLM extraction failed for {url or item.get('title','(no url)')}: {e}")

        # merge & normalize
        merged: Dict[Tuple[str, Optional[str]], TokenMention] = {}
        for h in hints + tokens:
            key = (h.name.strip(), (h.symbol or "").strip() or None)
            if key not in merged or h.confidence > merged[key].confidence:
                merged[key] = h

        out = {**item}
        out["currencies"] = [
            {"name": tm.name, "symbol": tm.symbol, "confidence": round(float(tm.confidence), 3)}
            for tm in merged.values()
        ]
        return out

    tasks = [process(it) for it in items_in[: max_articles]]
    enriched = await asyncio.gather(*tasks)

    # Return SAME shape as the API, with enriched items
    return {
        "data": enriched,
        "total_pages": api_payload.get("total_pages"),
        "total_items": api_payload.get("total_items"),
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
