from scrapegraphai.graphs import SmartScraperGraph
from pydantic import BaseModel, Field
from typing import List

# Define schema for output
class NewsItem(BaseModel):
    title: str
    link: str
    description: str

class NewsList(BaseModel):
    news: List[NewsItem]

def fetch_news_scrapegraph(query: str, source_url: str = None):
    prompt = (
        "Extract the top news article titles, links, and summaries about: "
        f"{query}"
    )
    graph = SmartScraperGraph(
        prompt=prompt,
        source=source_url or f"https://google.com/search?q={query}",
        config={"llm": {"api_key": "YOUR_KEY", "model": "gpt-4o-mini"}},
    )
    result = graph.run()
    return result

# Example use:
if __name__ == "__main__":
    out = fetch_news_scrapegraph("latest cryptocurrency news")
    print(out)
