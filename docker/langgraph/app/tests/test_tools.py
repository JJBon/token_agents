import pytest
from unittest.mock import AsyncMock
from tools import dbt_tools

@pytest.mark.asyncio
async def test_fetch_metrics_tool(mocker):
    mocker.patch("tools.dbt_tools.fetch_metrics_tool.invoke",
                 AsyncMock(return_value={"metrics": ["mock_metric"]}))
    response = await dbt_tools.fetch_metrics_tool.invoke()
    assert "mock_metric" in response["metrics"]
