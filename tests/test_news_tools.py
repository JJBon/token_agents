import json
from tools.news_tools import fetch_crypto_news_tool


def test_fetch_crypto_news_tool(mocker):
    mocker.patch.dict("tools.news_tools.os.environ", {"NEWS_API_KEY": "test"})
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "results": [{"title": "Mock headline", "url": "http://example.com"}]
    }
    mock_response.raise_for_status.return_value = None
    mock_response.status_code = 200
    mocker.patch("tools.news_tools.requests.get", return_value=mock_response)
    result = fetch_crypto_news_tool.invoke({"query": "bitcoin"})
    data = json.loads(result)
    assert data["articles"][0]["title"] == "Mock headline"


def test_fetch_crypto_news_tool_rate_limit(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 429
    mock_response.headers = {}
    mock_response.text = "Too Many Requests"
    mocker.patch("tools.news_tools.requests.get", return_value=mock_response)
    mocker.patch("tools.news_tools.time.sleep", return_value=None)
    result = fetch_crypto_news_tool.invoke({"query": "bitcoin"})
    data = json.loads(result)
    assert data["status"] == "ERROR"
    assert data.get("status_code") == 429
    assert "rate limit" in data["error"].lower()
