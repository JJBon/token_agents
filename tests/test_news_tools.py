import json
from tools.news_tools import fetch_crypto_news_tool


def test_fetch_crypto_news_tool(mocker):
    mocker.patch.dict("tools.news_tools.os.environ", {"NEWS_API_KEY": "test"})
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "articles": [{"title": "Mock headline", "url": "http://example.com"}]
    }
    mock_response.raise_for_status.return_value = None
    mocker.patch("tools.news_tools.requests.get", return_value=mock_response)
    result = fetch_crypto_news_tool.invoke({"query": "bitcoin"})
    data = json.loads(result)
    assert data["articles"][0]["title"] == "Mock headline"
