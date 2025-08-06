import json
from tools import news_tools


def fake_fetch_crypto_news(query: str) -> str:
    if query == "crypto news":
        articles = [
            {"title": "Bitcoin rallies to new high", "url": "https://example.com/btc1"},
            {"title": "Ethereum upgrade announced", "url": "https://example.com/eth1"},
            {"title": "Other market news", "url": "https://example.com/other"},
        ]
        return json.dumps({"status": "OK", "articles": articles})
    if query.lower() == "bitcoin":
        articles = [{"title": "Bitcoin hits $60k", "url": "https://example.com/btc2"}]
        return json.dumps({"status": "OK", "articles": articles})
    if query.lower() == "ethereum":
        articles = [{"title": "Ethereum gas fees drop", "url": "https://example.com/eth2"}]
        return json.dumps({"status": "OK", "articles": articles})
    return json.dumps({"status": "OK", "articles": []})


def test_analyze_trending_tokens(monkeypatch):
    monkeypatch.setattr(news_tools, "fetch_crypto_news", fake_fetch_crypto_news)
    result = news_tools.analyze_trending_tokens()
    tokens = {item["token"] for item in result}
    assert "bitcoin" in tokens
    assert "ethereum" in tokens
    btc = next(item for item in result if item["token"] == "bitcoin")
    assert "Bitcoin hits $60k" in btc["insight"]
    assert "https://example.com/btc2" in btc["relevant_news"]
