import json
import os
import re
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
    try:
        params = {
            "auth_token": os.getenv("CRYPTOPANIC_API_KEY", ""),
            "public": True,
            "q": query,
            "kind": "news",
        }
        resp = requests.get(NEWS_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        articles = [
            {"title": item.get("title", ""), "url": item.get("url", "")}
            for item in data.get("results", [])
            if item.get("title") and item.get("url")
        ]
        return json.dumps({"status": "OK", "articles": articles})
    except Exception as e:  # pragma: no cover - network failure
        return json.dumps({"status": "ERROR", "error": str(e), "articles": []})


def _extract_tokens(text: str) -> List[str]:
    return [m.group(1).lower() for m in TOKEN_PATTERN.finditer(text or "")]


def analyze_trending_tokens() -> List[Dict[str, Any]]:
    """Retrieve crypto news and summarize insights per token.

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

    for token in list(token_map.keys()):
        detail = json.loads(fetch_crypto_news(token))
        if detail.get("status") == "OK":
            token_articles = detail.get("articles", [])
            titles = [a.get("title", "") for a in token_articles]
            if titles:
                token_map[token]["insight"] = "; ".join(titles)
                token_map[token]["relevant_news"] = [a.get("url", "") for a in token_articles]
            else:
                token_map[token]["insight"] = "No significant news found"
        else:
            token_map[token]["insight"] = "No significant news found"

    result = [
        {"token": token, **info}
        for token, info in token_map.items()
    ]
    return result


fetch_crypto_news_tool = StructuredTool.from_function(
    func=fetch_crypto_news,
    name="fetch_crypto_news",
    description="Fetch crypto news articles. Returns JSON with keys status and articles.",
)

crypto_news_trends_tool = StructuredTool.from_function(
    func=analyze_trending_tokens,
    name="crypto_news_trends",
    description="Fetch latest crypto news, identify trending tokens, and summarize insights.",
)
