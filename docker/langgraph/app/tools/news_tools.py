import json
import os
import time
from typing import Any, Dict, List, Optional
import re
import requests

# Load configuration from environment
API_PLAN = os.getenv("CRYPTOPANIC_API_PLAN", "developer")  # e.g., "public" or a paid plan
API_VERSION = os.getenv("CRYPTOPANIC_API_VERSION", "v2")
AUTH_TOKEN = os.getenv("CRYPTOPANIC_API_KEY", "")
BASE_URL = f"https://cryptopanic.com/api/{API_PLAN}/{API_VERSION}/posts/"

# Supported query parameters per official docs
VALID_FILTERS = {"rising", "hot", "bullish", "bearish", "important", "saved", "lol"}
VALID_KINDS = {"news", "media"}


def fetch_crypto_news(
    auth_token: str = AUTH_TOKEN,
    *,
    q: Optional[str] = None,    
    filter: Optional[str] = None,
    currencies: Optional[List[str]] = None,
    regions: Optional[List[str]] = None,
    kind: Optional[str] = None,
    following: Optional[bool] = None,
    public: bool = True,
    with_content: bool = True,
    page: int = 1,
    timeout: int = 10,
) -> str:
    """
    Fetch crypto news articles with optional filters.

    Returns a JSON string:
      {
        "status": "OK" | "ERROR",
        "articles": [ { "title": ..., "url": ... }, ... ],
        "error": "...",           # only if status == "ERROR"
        "retry_after": "secs"     # only if rate limited
      }
    """
    params: Dict[str, Any] = {"auth_token": auth_token, "public": str(public).lower(), "page": page, "with_content": str(with_content).lower()}
    if q:
        params["q"] = q
    if filter:
        if filter not in VALID_FILTERS:
            raise ValueError(f"Invalid filter: {filter}")
        params["filter"] = filter
    if currencies:
        # API accepts repeated 'currencies' params: currencies=BTC&currencies=ETH
        params["currencies"] = currencies
    if regions:
        params["regions"] = regions
    if kind:
        if kind not in VALID_KINDS:
            raise ValueError(f"Invalid kind: {kind}")
        params["kind"] = kind
    if following is not None:
        params["following"] = str(following).lower()

    retries = 3
    backoff = 1
    for attempt in range(retries):
        resp = requests.get(BASE_URL, params=params, timeout=timeout)
        print("response, ", resp.json())
        # Handle rate limiting per docs
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if attempt < retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            error_payload = {
                "status": "ERROR",
                "error": "Rate limit exceeded",
                "status_code": resp.status_code,
                **({"retry_after": retry_after} if retry_after else {}),
                "articles": [],
            }
            return json.dumps(error_payload)

        try:
            resp.raise_for_status()
            data = resp.json()
            # Official v2 returns 'results'; legacy v1 also uses 'results'
            raw = data.get("results", [])
            articles = [
                {"title": item.get("title", ""), "url": item.get("url", "")}
                for item in raw
                if item.get("title") and item.get("url")
            ]
            return json.dumps({"status": "OK", "articles": articles})
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            return json.dumps({"status": "ERROR", "error": str(e), "articles": []})


def analyze_trending_tokens(
    limit: int = 5,
    news_per_token: int = 2,
    fetch_kwargs: Dict[str, Any] = None,
) -> List[Dict[str, Any]]:
    """
    Identify the top `limit` tokens mentioned in the latest feed,
    then fetch up to `news_per_token` headlines for each.
    Accepts `fetch_kwargs` dict to forward into fetch_crypto_news.
    """
    # default fetch options
    opts = {"public": True, "timeout": 10}
    if fetch_kwargs:
        opts.update(fetch_kwargs)

    # 1) Get the general feed
    general_raw = fetch_crypto_news(**opts)
    payload = json.loads(general_raw)
    if payload.get("status") != "OK":
        return []

    # 2) Build token → mentions map
    token_map: Dict[str, Dict[str, Any]] = {}
    for art in payload["articles"]:
        title = art["title"]
        for match in re.finditer(
            r"\b(bitcoin|btc|ethereum|eth|dogecoin|doge|solana|sol|ripple|xrp|cardano|ada|polkadot|dot)\b",
            title,
            re.IGNORECASE,
        ):
            tok = match.group(1).lower()
            token_map.setdefault(tok, {"relevant_news": []})
            token_map[tok]["relevant_news"].append(art["url"])

    # 3) Pick top `limit` tokens
    top_tokens = sorted(
        token_map.items(),
        key=lambda x: len(x[1]["relevant_news"]),
        reverse=True,
    )[:limit]

    results = []
    for token, info in top_tokens:
        # 4) Fetch detailed news for each token
        detail_raw = fetch_crypto_news(q=token, **opts)  # pass token as a 'q' search
        detail = json.loads(detail_raw)

        if detail.get("status") == "OK" and detail.get("articles"):
            sel = detail["articles"][:news_per_token]
            titles = [a["title"] for a in sel]
            urls   = [a["url"]   for a in sel]

            info["insight"]       = " | ".join(titles)
            info["relevant_news"] = urls
        else:
            info["insight"]       = "No significant news found"
            info["relevant_news"] = []

        results.append({"token": token, **info})

    return results


# Tools creation (unchanged)
from langchain_core.tools import StructuredTool

fetch_crypto_news_tool = StructuredTool.from_function(
    func=fetch_crypto_news,
    name="fetch_crypto_news",
    description="Fetch crypto news articles with optional filters; returns JSON with 'status' and 'articles'.",
)

crypto_news_trends_tool = StructuredTool.from_function(
    func=analyze_trending_tokens,
    name="crypto_news_trends",
    description="Fetch latest crypto news, identify top trending tokens, and summarize insights.",
)
