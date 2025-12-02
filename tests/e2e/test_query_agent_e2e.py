"""
End-to-end tests for Query Agent.

These tests verify the complete flow from user query through the agent
to the dbt semantic layer and back with results.

Prerequisites:
- All docker compose services running
- Data loaded in Glue catalog
- LiteLLM proxy configured with AWS credentials

Run with: pytest tests/e2e/test_query_agent_e2e.py -v -s
"""

import pytest
import asyncio
import sys
import os

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../docker/langgraph/app'))

from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage, AIMessage


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_available_metrics():
    """Test asking for available metrics."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="What metrics are available?")]
    }, config={"configurable": {"thread_id": "e2e-metrics"}})
    
    assert "messages" in result
    assert len(result["messages"]) > 0
    
    last_message = result["messages"][-1]
    assert isinstance(last_message, AIMessage)
    
    content = last_message.content.lower()
    
    # Should mention metrics
    assert any(word in content for word in ["metric", "available", "measure"])
    
    print(f"\n✓ Response:\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_bitcoin_price():
    """Test querying Bitcoin's average price."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="What is Bitcoin's average price?")]
    }, config={"configurable": {"thread_id": "e2e-bitcoin"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Verify response contains expected elements
    assert "bitcoin" in content, "Response doesn't mention Bitcoin"
    assert any(word in content for word in ["price", "average", "usd", "$"]), \
        "Response doesn't mention price"
    
    # Should contain a number
    assert any(char.isdigit() for char in content), \
        "Response doesn't contain any numbers"
    
    print(f"\n✓ Bitcoin price response:\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_top_coins():
    """Test querying top cryptocurrencies by market cap."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(
            content="Show me the top 5 cryptocurrencies by market cap"
        )]
    }, config={"configurable": {"thread_id": "e2e-top-coins"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Should mention multiple coins
    coin_mentions = sum([
        "bitcoin" in content,
        "ethereum" in content,
        "btc" in content,
        "eth" in content
    ])
    assert coin_mentions >= 1, "Response doesn't mention any major cryptocurrencies"
    
    # Should have data (numbers or table)
    assert any(char.isdigit() for char in content), \
        "Response doesn't contain any data"
    
    # Check for table format
    has_table = "|" in last_message.content
    
    print(f"\n✓ Top coins response (table={has_table}):\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_time_series():
    """Test querying time series data."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(
            content="Show Bitcoin price trends for the last 7 days"
        )]
    }, config={"configurable": {"thread_id": "e2e-timeseries"}})
    
    last_message = result["messages"][-1]
    content = last_message.content
    
    # Should contain time references
    assert any(word in content.lower() for word in [
        "day", "date", "time", "trend", "week"
    ]), "Response doesn't mention time period"
    
    # Should have multiple data points (table or list)
    has_table = "|" in content
    has_multiple_lines = content.count("\n") > 3
    
    assert has_table or has_multiple_lines, \
        "Response doesn't appear to have multiple data points"
    
    print(f"\n✓ Time series response:\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_comparison():
    """Test comparing multiple cryptocurrencies."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(
            content="Compare Bitcoin and Ethereum prices"
        )]
    }, config={"configurable": {"thread_id": "e2e-compare"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Should mention both coins
    assert "bitcoin" in content or "btc" in content, \
        "Response doesn't mention Bitcoin"
    assert "ethereum" in content or "eth" in content, \
        "Response doesn't mention Ethereum"
    
    # Should have comparative data
    assert any(char.isdigit() for char in content), \
        "Response doesn't contain any numbers"
    
    print(f"\n✓ Comparison response:\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_conversation_context():
    """Test multi-turn conversation with context."""
    graph = await build_graph()
    thread_id = "e2e-context"
    
    # First query
    result1 = await graph.ainvoke({
        "messages": [HumanMessage(content="What is Bitcoin's price?")]
    }, config={"configurable": {"thread_id": thread_id}})
    
    # Follow-up query (should use context)
    result2 = await graph.ainvoke({
        "messages": result1["messages"] + [
            HumanMessage(content="How does that compare to Ethereum?")
        ]
    }, config={"configurable": {"thread_id": thread_id}})
    
    last_message = result2["messages"][-1]
    content = last_message.content.lower()
    
    # Should understand "that" refers to Bitcoin
    assert "ethereum" in content or "eth" in content, \
        "Response doesn't mention Ethereum"
    
    # Should have comparative context
    assert any(word in content for word in ["compare", "versus", "vs", "than"]), \
        "Response doesn't show comparison"
    
    print(f"\n✓ Context-aware response:\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_error_handling():
    """Test handling of invalid queries."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(
            content="Show me data for NonExistentCoin"
        )]
    }, config={"configurable": {"thread_id": "e2e-error"}})
    
    last_message = result["messages"][-1]
    content = last_message.content.lower()
    
    # Should handle gracefully (not crash)
    assert len(content) > 0, "Empty response"
    
    # Should indicate issue
    error_indicators = ["not found", "no data", "unavailable", "doesn't exist", "cannot find"]
    has_error_message = any(indicator in content for indicator in error_indicators)
    
    print(f"\n✓ Error handling response:\n{last_message.content[:500]}...")
    if has_error_message:
        print("  (Correctly indicated data not available)")


@pytest.mark.e2e
@pytest.mark.asyncio
@pytest.mark.slow
async def test_query_agent_complex_query():
    """Test complex multi-part query."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="""
            Show me the top 3 cryptocurrencies by market cap,
            their average prices over the last 7 days,
            and calculate the percentage change.
        """)]
    }, config={"configurable": {"thread_id": "e2e-complex"}})
    
    last_message = result["messages"][-1]
    content = last_message.content
    
    # Should have structured data
    has_table = "|" in content
    has_numbers = any(char.isdigit() for char in content)
    
    assert has_numbers, "Response doesn't contain any data"
    
    print(f"\n✓ Complex query response (table={has_table}):\n{last_message.content[:500]}...")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_query_agent_tool_usage():
    """Verify agent uses MCP tools correctly."""
    graph = await build_graph()
    
    result = await graph.ainvoke({
        "messages": [HumanMessage(content="List available metrics")]
    }, config={"configurable": {"thread_id": "e2e-tools"}})
    
    # Check if tool calls were made
    tool_calls_made = False
    for message in result["messages"]:
        if hasattr(message, "tool_calls") and message.tool_calls:
            tool_calls_made = True
            print(f"\n✓ Tool calls made: {[tc['name'] for tc in message.tool_calls]}")
            break
    
    # Should have made at least one tool call
    assert tool_calls_made or len(result["messages"]) > 2, \
        "Agent doesn't appear to have used tools"
    
    last_message = result["messages"][-1]
    print(f"\n✓ Final response:\n{last_message.content[:300]}...")


if __name__ == "__main__":
    # Run with: python -m pytest tests/e2e/test_query_agent_e2e.py -v -s
    pytest.main([__file__, "-v", "-s", "--tb=short"])
