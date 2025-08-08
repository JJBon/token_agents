 #!/usr/bin/env python3
import os
import json
from news_tools import fetch_crypto_news, analyze_trending_tokens

def pretty_print(title, obj):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, indent=2, ensure_ascii=False))

def main():
    # 1. Make sure your API key is set
    key = os.getenv("CRYPTOPANIC_API_KEY")
    if not key:
        print("⚠️  Please export your CRYPTOPANIC_API_KEY before running.")
        return

    # 2. Test fetch_crypto_news with various args
    #    – Basic crypto feed
    raw1 = fetch_crypto_news(public=True, timeout=5)
    pretty_print("General feed (public)", json.loads(raw1))

    #    – Search for “bitcoin”
    raw2 = fetch_crypto_news(q="bitcoin", public=True, timeout=5)
    pretty_print("Search q=bitcoin", json.loads(raw2))

    #    – Filter “hot” media only
    raw3 = fetch_crypto_news(q="ethereum", filter="hot", kind="media", public=True, timeout=5)
    pretty_print("Filter=hot, kind=media, q=ethereum", json.loads(raw3))

    # 3. Test analyze_trending_tokens
    #    – Default top 5 × 2 articles
    trends1 = analyze_trending_tokens()
    pretty_print("Top 5 tokens × 2 articles (defaults)", trends1)

    #    – Top 3 tokens × 1 article each, only public news
    trends2 = analyze_trending_tokens(limit=3, news_per_token=1, fetch_kwargs={"public": True})
    pretty_print("Top 3 × 1article", trends2)

if __name__ == "__main__":
    main()
