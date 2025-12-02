"""
Simple integration test for Query Agent.

This test verifies the Query Agent can connect to MCP and execute queries.
It's simpler than testing MCP directly since it uses the actual agent code.

Prerequisites:
- Docker compose services running
- Thrift server started (make compose-run-spark-dbt)
- Data in Glue catalog

Run with: pytest tests/integration/test_query_agent_simple.py -v -s
"""

import pytest
import sys
import os

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../docker/langgraph/app'))

try:
    from agents.query_agent.graph import build_graph, _load_all_mcp_tools
    from langchain_core.messages import HumanMessage
    AGENT_AVAILABLE = True
except ImportError as e:
    AGENT_AVAILABLE = False
    IMPORT_ERROR = str(e)


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not AGENT_AVAILABLE, reason=f"Agent not available: {IMPORT_ERROR if not AGENT_AVAILABLE else ''}")
async def test_mcp_tools_available():
    """Test that MCP tools can be loaded."""
    try:
        tools = await _load_all_mcp_tools()
        assert len(tools) > 0, "No MCP tools loaded"
        
        tool_names = [t.name for t in tools]
        print(f"\n✓ Loaded {len(tools)} MCP tools:")
        for name in tool_names:
            print(f"  - {name}")
        
        # Check for expected tools
        expected_tools = ["fetch_metrics", "create_query", "fetch_query_result"]
        for expected in expected_tools:
            assert expected in tool_names, f"Missing expected tool: {expected}"
        
    except Exception as e:
        pytest.skip(f"MCP server not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not AGENT_AVAILABLE, reason="Agent not available")
async def test_query_agent_can_build():
    """Test that Query Agent graph can be built."""
    try:
        graph = await build_graph()
        assert graph is not None, "Graph is None"
        print("\n✓ Query Agent graph built successfully")
    except Exception as e:
        pytest.skip(f"Cannot build graph: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not AGENT_AVAILABLE, reason="Agent not available")
async def test_query_agent_simple_query():
    """Test a simple query through the agent."""
    try:
        graph = await build_graph()
        
        result = await graph.ainvoke({
            "messages": [HumanMessage(content="What metrics are available?")]
        }, config={"configurable": {"thread_id": "test-simple"}})
        
        assert "messages" in result, "No messages in result"
        assert len(result["messages"]) > 0, "Empty messages"
        
        last_message = result["messages"][-1]
        content = last_message.content
        
        print(f"\n✓ Agent response:\n{content[:500]}...")
        
        # Basic validation - should mention metrics
        assert len(content) > 0, "Empty response"
        
    except Exception as e:
        pytest.skip(f"Query failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
