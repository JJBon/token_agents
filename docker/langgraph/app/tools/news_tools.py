import json
import os
import re
import time
from typing import Dict, List, Any

import requests
from langchain_core.tools import StructuredTool


NEWS_API_URL = "https://cryptopanic.com/api/v1/posts/"
TOKEN_PATTERN = re.compile(
    r"\b(bitcoin|btc|ethereum|eth|dogecoin|doge|solana|sol|ripple|xrp|cardano|ada|polkadot|dot)\b",
    re.IGNORECASE,
)


def fetch_crypto_news(query: str = "crypto") -> str:
    """Fetch crypto news articles for a given query.

    Returns JSON string with keys: status and articles list of {title, url}.
    """
    params = {
        "auth_token": os.getenv("CRYPTOPANIC_API_KEY", ""),
        "public": True,
        "q": query,
        "kind": "news",
    }
    retries = 3
    backoff = 1
    for attempt in range(retries):
        try:
            resp = requests.get(NEWS_API_URL, params=params, timeout=10)
            if resp.status_code == 429:
                if attempt < retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                error_details = {
                    "status": "ERROR",
                    "error": f"Rate limit exceeded: {resp.text or 'Too Many Requests'}",
                    "status_code": resp.status_code,
                    "articles": [],
                }
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    error_details["retry_after"] = retry_after
                return json.dumps(error_details)
            resp.raise_for_status()
            data = resp.json()
            articles = [
                {"title": item.get("title", ""), "url": item.get("url", "")}
                for item in data.get("results", [])
                if item.get("title") and item.get("url")
            ]
            return json.dumps({"status": "OK", "articles": articles})
        except Exception as e:  # pragma: no cover - network failure
            if attempt < retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            return json.dumps({"status": "ERROR", "error": str(e), "articles": []})


def _extract_tokens(text: str) -> List[str]:
    return [m.group(1).lower() for m in TOKEN_PATTERN.finditer(text or "")]


def analyze_trending_tokens(limit: int = 5) -> List[Dict[str, Any]]:
    """Retrieve crypto news and summarize insights for top trending tokens.

    Only the ``limit`` most-mentioned tokens in the general crypto news feed are
    queried for detailed insights to avoid unnecessary API calls.

    Returns a list of dictionaries of the form::
        {
            "token": "bitcoin",
            "insight": "Bitcoin hits all time high",
            "relevant_news": ["http://example.com/btc"]
        }
    """
    general_raw = fetch_crypto_news("crypto news")
    payload = json.loads(general_raw)
    if payload.get("status") != "OK":
        return []

    articles = payload.get("articles", [])
    token_map: Dict[str, Dict[str, Any]] = {}

    for art in articles:
        tokens = _extract_tokens(art.get("title", ""))
        for t in tokens:
            entry = token_map.setdefault(t, {"relevant_news": []})
            entry["relevant_news"].append(art.get("url", ""))

    # Select the top ``limit`` tokens based on frequency of appearance
    top_tokens = sorted(
        token_map.items(),
        key=lambda item: len(item[1]["relevant_news"]),
        reverse=True,
    )[:limit]

    result: List[Dict[str, Any]] = []
    for token, info in top_tokens:
        detail = json.loads(fetch_crypto_news(token))
        if detail.get("status") == "OK":
            token_articles = detail.get("articles", [])
            titles = [a.get("title", "") for a in token_articles]
            if titles:
                info["insight"] = "; ".join(titles)
                info["relevant_news"] = [a.get("url", "") for a in token_articles]
            else:
                info["insight"] = "No significant news found"
        else:
            info["insight"] = "No significant news found"
        result.append({"token": token, **info})

    return result


fetch_crypto_news_tool = StructuredTool.from_function(
    func=fetch_crypto_news,
    name="fetch_crypto_news",
    description="Fetch crypto news articles. Returns JSON with keys status and articles.",
)

crypto_news_trends_tool = StructuredTool.from_function(
    func=analyze_trending_tokens,
    name="crypto_news_trends",
    description="Fetch latest crypto news, identify top trending tokens (default top 5), and summarize insights.",
)