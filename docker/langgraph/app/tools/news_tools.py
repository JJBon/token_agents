import os
import json
import requests
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool


class FetchNewsInput(BaseModel):
    query: str = Field(..., description="Search topic for crypto news")


def fetch_crypto_news(query: str) -> str:
    """Fetch recent crypto-related news articles for a given topic."""
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        return json.dumps({"status": "ERROR", "error": "NEWS_API_KEY not set"})

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": f"{query} AND crypto",
        "language": "en",
        "sortBy": "publishedAt",
        "apiKey": api_key,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        articles = [
            {"title": a.get("title"), "url": a.get("url")}
            for a in data.get("articles", [])[:5]
        ]
        return json.dumps({"status": "OK", "articles": articles}, indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "error": str(e)})


fetch_crypto_news_tool = StructuredTool.from_function(
    func=fetch_crypto_news,
    name="fetch_crypto_news",
    description="Fetch recent crypto-related news articles.",
    args_schema=FetchNewsInput,
)
