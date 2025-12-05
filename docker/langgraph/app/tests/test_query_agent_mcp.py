import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from agents.query_agent.graph import build_graph, QueryGraphArgs, QueryGraphResult
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import StructuredTool
import json

from io import BytesIO

def _mock_fetch_metrics() -> str:
    return '{"metrics": ["m1","m2"]}'

mock_tool = StructuredTool.from_function(
    func=_mock_fetch_metrics,
    name="fetch_metrics",
    description="Mock fetch metrics tool"
)

class MockBedrockClient:
    def converse(self, *args, **kwargs):
        return {
            "output": {
                "role": "assistant",
                "content": "I'll help you find available metrics."
            }
        }

    def invoke(self, *args, **kwargs):
        return {
            "body": BytesIO(json.dumps(self.converse()).encode())
        }

class MockChatBedrockConverse:
    def __init__(self, *args, **kwargs):
        self.client = MockBedrockClient()

    async def ainvoke(self, messages, **kwargs):
        response = self.client.converse()
        return AIMessage(content=response["output"]["content"])

    async def agenerate(self, messages, **kwargs):
        response = self.client.converse()
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(content=response["output"]["content"]),
                    text=response["output"]["content"]
                )
            ]
        )

    def bind_tools(self, tools):
        return self

    def _generate(self, messages, **kwargs):
        response = self.client.converse()
        return AIMessage(content=response["output"]["content"])

    async def _agenerate(self, messages, **kwargs):
        response = self.client.converse()
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(content=response["output"]["content"]),
                    text=response["output"]["content"]
                )
            ]
        )

@pytest.mark.asyncio
async def test_build_graph_with_mcp():
    mock_mcp_instance = AsyncMock()

    # ✅ Return an actual LangChain tool, not a dict
    from langchain_core.tools import StructuredTool
    def _mock_fetch_metrics() -> str:
        return '{"metrics": ["m1","m2"]}'
    mock_tool = StructuredTool.from_function(
        func=_mock_fetch_metrics,
        name="fetch_metrics",
        description="Mock fetch metrics tool"
    )
    mock_mcp_instance.get_tools.return_value = [mock_tool]

    with patch('agents.query_agent.graph.ChatBedrockConverse', MockChatBedrockConverse), \
         patch('agents.query_agent.graph.MultiServerMCPClient', return_value=mock_mcp_instance), \
         patch('boto3.client', return_value=MockBedrockClient()):

        graph = await build_graph({
            "model": "anthropic.claude-3-haiku-20240307-v1:0",
            "provider": "anthropic",
            "temperature": 0.0,
            "llm_ctor": MockChatBedrockConverse,  # if you adopted Option B
        })

        result = await graph.ainvoke(
            {"messages": [HumanMessage(content="Show me available metrics")]},
            config={"configurable": {"thread_id": "test-thread"}}
        )
        assert "messages" in result and len(result["messages"]) > 0

@pytest.mark.asyncio
async def test_query_graph_args_conversion():
    args = QueryGraphArgs(user_request="Show metrics", retries=1)
    state = {"messages": [{"role": "user", "content": "Show metrics"}]}
    
    from agents.query_agent.graph import _to_state
    converted = _to_state(args)
    assert converted["messages"][0].content == state["messages"][0]["content"]

@pytest.mark.asyncio
async def test_query_graph_result_conversion():
    state = {
        "messages": [
            AIMessage(content="Data: test data\nInsight: test insight")
        ]
    }
    
    from agents.query_agent.graph import _from_state
    result = _from_state(state)
    
    assert isinstance(result, QueryGraphResult)
    assert result.data == "test data"
    assert result.insight == "test insight"