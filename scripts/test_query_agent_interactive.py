#!/usr/bin/env python
"""
Interactive testing script for Query Agent.

This script allows you to test the Query Agent with various questions
and see the responses in real-time.

Usage:
    python scripts/test_query_agent_interactive.py
    
Or from docker:
    docker-compose -f docker/spark/docker-compose.yml exec langgraph-backend \
        python /app/scripts/test_query_agent_interactive.py
"""

import asyncio
import sys
import os
from datetime import datetime

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../docker/langgraph/app'))

from agents.query_agent.graph import build_graph
from langchain_core.messages import HumanMessage


async def test_query(question: str, thread_id: str = "interactive-test"):
    """Test a single query and print results."""
    print(f"\n{'='*70}")
    print(f"Question: {question}")
    print(f"{'='*70}\n")
    
    try:
        graph = await build_graph()
        
        start_time = datetime.now()
        result = await graph.ainvoke({
            "messages": [HumanMessage(content=question)]
        }, config={"configurable": {"thread_id": thread_id}})
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        last_message = result["messages"][-1]
        
        print(f"Response (took {elapsed:.2f}s):")
        print(f"{'-'*70}")
        print(last_message.content)
        print(f"{'-'*70}\n")
        
        # Show tool calls if any
        for msg in result["messages"]:
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                print(f"Tools used: {[tc['name'] for tc in msg.tool_calls]}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        return None


async def run_test_suite():
    """Run a suite of test queries."""
    print("\n" + "="*70)
    print("QUERY AGENT INTERACTIVE TEST SUITE")
    print("="*70)
    
    test_queries = [
        # Basic queries
        ("What metrics are available?", "Test 1: List metrics"),
        ("What is Bitcoin's average price?", "Test 2: Simple metric query"),
        
        # Aggregation queries
        ("Show me the top 5 cryptocurrencies by market cap", "Test 3: Top N query"),
        ("What are the average prices for all cryptocurrencies?", "Test 4: Aggregate query"),
        
        # Time series queries
        ("Show Bitcoin price trends for the last 7 days", "Test 5: Time series"),
        ("Compare prices over the last month", "Test 6: Monthly trends"),
        
        # Comparison queries
        ("Compare Bitcoin and Ethereum prices", "Test 7: Comparison"),
        ("Which coin has the highest market cap?", "Test 8: Superlative"),
        
        # Complex queries
        ("Show the top 3 coins by volume and their price changes", "Test 9: Multi-metric"),
        ("Calculate the average market cap for coins with price > $1000", "Test 10: Filtered aggregate"),
    ]
    
    results = []
    for i, (query, description) in enumerate(test_queries, 1):
        print(f"\n{'#'*70}")
        print(f"# {description}")
        print(f"{'#'*70}")
        
        result = await test_query(query, thread_id=f"test-{i}")
        results.append((description, result is not None))
        
        # Rate limiting
        if i < len(test_queries):
            print("Waiting 2 seconds before next query...")
            await asyncio.sleep(2)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for description, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {description}")
    
    print(f"\nTotal: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    print("="*70 + "\n")


async def interactive_mode():
    """Interactive mode - ask questions one by one."""
    print("\n" + "="*70)
    print("QUERY AGENT INTERACTIVE MODE")
    print("="*70)
    print("\nType your questions (or 'quit' to exit):\n")
    
    thread_id = f"interactive-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    while True:
        try:
            question = input("\nYour question: ").strip()
            
            if not question:
                continue
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!\n")
                break
            
            await test_query(question, thread_id=thread_id)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted. Goodbye!\n")
            break
        except EOFError:
            print("\n\nGoodbye!\n")
            break


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Query Agent interactively")
    parser.add_argument(
        "--mode",
        choices=["suite", "interactive"],
        default="suite",
        help="Test mode: 'suite' runs predefined tests, 'interactive' allows custom questions"
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Single query to test (skips mode selection)"
    )
    
    args = parser.parse_args()
    
    if args.query:
        # Single query mode
        await test_query(args.query)
    elif args.mode == "interactive":
        # Interactive mode
        await interactive_mode()
    else:
        # Test suite mode
        await run_test_suite()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted. Goodbye!\n")
        sys.exit(0)
