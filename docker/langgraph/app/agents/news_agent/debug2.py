import boto3
import json
from bs4 import BeautifulSoup
from scrapegraphai.nodes.fetch_node import FetchNode

# -----------------------------
# Extract real articles from HTML using BS4
# -----------------------------
def extract_cryptopanic_articles(html: str, base_url="https://cryptopanic.com"):
    soup = BeautifulSoup(html, "html.parser")
    articles = []

    for a_tag in soup.select("a.news-cell.nc-title"):
        title_tag = a_tag.select_one("span.title-text span")
        if title_tag:
            title = title_tag.get_text(strip=True)
            href = a_tag.get("href")
            if href and "/news/" in href:
                full_url = base_url + href
                articles.append({"title": title, "url": full_url})

    return articles[:5]  # Limit to 5 articles

# -----------------------------
# Use FetchNode to get raw HTML
# -----------------------------
fetch_node = FetchNode(
    input="url",
    output=["doc"],
    node_config={"verbose": True, "headless": True},
)

# Execute FetchNode only
result, _ = fetch_node.execute({
    "url": "https://cryptopanic.com"
})

html = result  # ← this is the raw HTML string
articles = extract_cryptopanic_articles(html)

final_result = {"content": articles}
print(json.dumps(final_result, indent=2))
# -----------------------------
# Output result as expected JSON
# -----------------------------
final_result = {"content": articles}
print(json.dumps(final_result, indent=2))
