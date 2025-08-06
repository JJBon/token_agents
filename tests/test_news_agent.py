import json
from tools import news_tools
from agents.news_agent import graph as news_graph
import asyncio
from langchain_core.language_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda


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


def fake_fetch_crypto_news_many(query: str) -> str:
    if query == "crypto news":
        articles = [
            {"title": "Bitcoin news", "url": "https://example.com/btc1"},
            {"title": "Ethereum and Bitcoin news", "url": "https://example.com/ethbtc"},
            {"title": "Dogecoin joins Bitcoin and Ethereum", "url": "https://example.com/doge1"},
            {"title": "Solana beats Dogecoin and Bitcoin", "url": "https://example.com/sol1"},
            {"title": "Ripple vs Bitcoin and Ethereum and Dogecoin", "url": "https://example.com/xrp1"},
            {"title": "Cardano vs Bitcoin, Ethereum, Dogecoin, Solana, Ripple", "url": "https://example.com/ada1"},
        ]
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


def test_analyze_trending_tokens_limit(monkeypatch):
    monkeypatch.setattr(news_tools, "fetch_crypto_news", fake_fetch_crypto_news_many)
    result = news_tools.analyze_trending_tokens(limit=5)
    tokens = {item["token"] for item in result}
    assert len(tokens) == 5
    assert "bitcoin" in tokens
    assert "cardano" not in tokens


def test_news_agent_graph(monkeypatch):
    monkeypatch.setattr(news_tools, "fetch_crypto_news", fake_fetch_crypto_news)
    class ToolCallingFake(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    responses = [
        AIMessage(content="", tool_calls=[{"name": "crypto_news_trends", "args": {}, "id": "tool-0"}]),
        AIMessage(content="done"),
    ]
    llm = ToolCallingFake(responses=responses)
    graph = news_graph.build_graph(llm=llm, tools=[news_tools.crypto_news_trends_tool])
    pipeline = RunnableLambda(news_graph._to_state) | graph | RunnableLambda(news_graph._from_state)
    result = asyncio.run(
        pipeline.ainvoke({"user_request": "latest crypto news"}, config={"configurable": {"thread_id": "t1"}})
    )
    tokens = {item["token"] for item in result.tokens}
    assert "bitcoin" in tokens
    btc = next(item for item in result.tokens if item["token"] == "bitcoin")
    assert "Bitcoin hits $60k" in btc["insight"]
