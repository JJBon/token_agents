import pytest
from unittest.mock import AsyncMock

pytest.skip("dbt tools tests require DBT environment", allow_module_level=True)

from docker.langgraph.app.tools.query_tools import dbt_tools

@pytest.mark.asyncio
async def test_fetch_metrics_tool(mocker):
    mocker.patch("tools.dbt_tools.fetch_metrics_tool.invoke", AsyncMock(return_value={"metrics": ["mock_metric"]}))
    response = await dbt_tools.fetch_metrics_tool.invoke()
    assert "mock_metric" in response["metrics"]
