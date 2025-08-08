import re
import json
from typing import Any, Dict, List
from playwright.sync_api import sync_playwright
from langchain_core.tools import StructuredTool


# Token regex (same as before)
TOKEN_PATTERN = re.compile(
    r"\b(bitcoin|btc|ethereum|eth|dogecoin|doge|solana|sol|ripple|xrp|cardano|ada|polkadot|dot)\b",
    re.IGNORECASE,
)

def fetch_crypto_news_playwright(
    base_url: str,
    query: str = "crypto",
    headless: bool = True,
    timeout_ms: int = 30_000,
) -> List[Dict[str, str]]:
    """
    Uses Playwright to navigate to `base_url`, run a search for `query`,
    and return a list of {"title": ..., "url": ...}.
    """
    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)  # launch a real browser :contentReference[oaicite:5]{index=5}
        page = browser.new_page()
        page.goto(base_url, timeout=timeout_ms)
        # Adapt selectors to the target site; for CryptoPanic:
        #   - Search input: 'input[name="q"]'
        #   - Submit: 'button[type="submit"]'
        page.fill('input[name="q"]', query)
        page.click('button[type="submit"]')
        page.wait_for_selector(".post")  # wait for results container :contentReference[oaicite:6]{index=6}
        posts = page.query_selector_all(".post")
        for post in posts:
            title_el = post.query_selector(".post-title a")
            if title_el:
                title = title_el.inner_text().strip()
                url   = title_el.get_attribute("href")
                if title and url:
                    results.append({"title": title, "url": url})
        browser.close()
    return results


def analyze_trending_tokens_playwright(
    base_url: str,
    limit: int = 5,
    news_per_token: int = 2,
    **fetch_opts: Any,
) -> List[Dict[str, Any]]:
    """
    1. Scrapes general crypto news via Playwright.
    2. Counts token mentions and picks top `limit`.
    3. For each token, scrapes up to `news_per_token` detailed posts.
    """
    # 1) General scrape
    general = fetch_crypto_news_playwright(base_url, **fetch_opts)
    token_map: Dict[str, Dict[str, Any]] = {}
    for art in general:
        for m in TOKEN_PATTERN.finditer(art["title"]):
            tok = m.group(1).lower()
            token_map.setdefault(tok, {"relevant_news": []})
            token_map[tok]["relevant_news"].append(art["url"])
    # 2) Top tokens
    top = sorted(token_map.items(), key=lambda x: len(x[1]["relevant_news"]), reverse=True)[:limit]
    results = []
    # 3) Fetch per-token details
    for token, info in top:
        detailed = fetch_crypto_news_playwright(base_url, query=token, **fetch_opts)
        sel = detailed[:news_per_token]
        titles = [d["title"] for d in sel]
        urls   = [d["url"]   for d in sel]
        info["insight"]       = " | ".join(titles) if titles else "No significant news"
        info["relevant_news"] = urls
        results.append({"token": token, **info})
    return results


# Wrap Playwright functions as StructuredTools
fetch_tool = StructuredTool.from_function(
    func=fetch_crypto_news_playwright,
    name="fetch_crypto_news_playwright",
    description="Scrapes crypto news from a website via Playwright."
)
trends_tool = StructuredTool.from_function(
    func=analyze_trending_tokens_playwright,
    name="crypto_news_trends_playwright",
    description="Identifies top tokens and scrapes detailed news via Playwright."
)