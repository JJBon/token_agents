"""
Integration tests for MCP → dbt → Spark flow.

These tests verify that the MCP server can successfully communicate with
the dbt semantic layer and execute queries against Spark/Glue tables.

Prerequisites:
- Docker compose services running (spark-master, dbt-mcp)
- Data loaded in Glue catalog
- dbt models compiled

Run with: pytest tests/integration/test_mcp_dbt_integration.py -v
"""

import pytest
import requests
import json
import time

MCP_URL = "http://localhost:8001/mcp"
TIMEOUT = 30  # seconds


def wait_for_mcp_server(max_attempts=10):
    """Wait for MCP server to be ready."""
    for i in range(max_attempts):
        try:
            response = requests.get("http://localhost:8001/health", timeout=5)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
    return False


def call_mcp_tool(tool_name: str, arguments: dict = None, timeout=TIMEOUT):
    """Helper to call MCP tools via JSON-RPC."""
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
        },
        "id": 1
    }
    response = requests.post(MCP_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


@pytest.fixture(scope="module", autouse=True)
def check_services():
    """Ensure required services are running."""
    if not wait_for_mcp_server():
        pytest.skip("MCP server not available")


@pytest.mark.integration
class TestMCPTools:
    """Test MCP tool functionality."""
    
    def test_fetch_metrics(self):
        """Test fetching available metrics from dbt semantic layer."""
        result = call_mcp_tool("fetch_metrics")
        
        assert "result" in result, "Missing result in response"
        assert "content" in result["result"], "Missing content in result"
        
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        assert "metrics" in content, "No metrics found"
        assert len(content["metrics"]) > 0, "Metrics list is empty"
        
        # Verify metric structure
        first_metric = content["metrics"][0]
        assert "name" in first_metric
        assert "type" in first_metric
        
        print(f"\n✓ Found {len(content['metrics'])} metrics")
        for metric in content["metrics"][:3]:
            print(f"  - {metric['name']} ({metric['type']})")
    
    def test_create_query_simple(self):
        """Test query validation with simple metric."""
        result = call_mcp_tool("create_query", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "metric_time__day"}],
            "limit": 10
        })
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        assert content.get("status") == "OK", f"Query validation failed: {content}"
        assert "sql" in content, "No SQL generated"
        
        print(f"\n✓ Generated SQL:\n{content['sql'][:200]}...")
    
    def test_create_query_with_order(self):
        """Test query with ORDER BY clause."""
        result = call_mcp_tool("create_query", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "name"}],
            "order_by": ["average_price_usd desc"],
            "limit": 5
        })
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        assert content.get("status") == "OK"
        assert "ORDER BY" in content["sql"].upper()
        
        print(f"\n✓ Query with ORDER BY validated")
    
    def test_fetch_query_result_simple(self):
        """Test executing a simple query."""
        result = call_mcp_tool("fetch_query_result", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "metric_time__day"}],
            "order_by": ["metric_time__day desc"],
            "limit": 5
        }, timeout=60)  # Longer timeout for query execution
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        assert content.get("status") == "OK", f"Query failed: {content}"
        assert "rows" in content, "No rows returned"
        assert "table" in content, "No markdown table generated"
        
        print(f"\n✓ Query returned {len(content['rows'])} rows")
        print(f"Markdown table preview:\n{content['table'][:200]}...")
    
    def test_fetch_query_result_with_filter(self):
        """Test query with WHERE condition."""
        result = call_mcp_tool("fetch_query_result", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "name"}],
            "where": {
                "conditions": [
                    {
                        "dimension": "name",
                        "operator": "=",
                        "value": "Bitcoin"
                    }
                ],
                "logic": "AND"
            },
            "limit": 1
        }, timeout=60)
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        assert content.get("status") == "OK"
        assert len(content["rows"]) > 0, "No rows returned for Bitcoin"
        
        # Verify Bitcoin is in results
        bitcoin_found = any(
            row.get("name", "").lower() == "bitcoin" 
            for row in content["rows"]
        )
        assert bitcoin_found, "Bitcoin not found in filtered results"
        
        print(f"\n✓ Filter query returned Bitcoin data")
    
    def test_search_dimension_values(self):
        """Test searching dimension values."""
        result = call_mcp_tool("search_dimension_values", {
            "dimension": "name",
            "query": "bit",
            "max_results": 10
        })
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        # Should find Bitcoin
        values = content.get("values", [])
        assert len(values) > 0, "No dimension values found"
        
        bitcoin_found = any("bitcoin" in v.lower() for v in values)
        assert bitcoin_found, "Bitcoin not found in dimension search"
        
        print(f"\n✓ Found {len(values)} values matching 'bit'")


@pytest.mark.integration
class TestMCPErrorHandling:
    """Test error handling in MCP tools."""
    
    def test_invalid_metric(self):
        """Test query with non-existent metric."""
        result = call_mcp_tool("create_query", {
            "metrics": ["nonexistent_metric"],
            "limit": 10
        })
        
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        # Should return error
        assert content.get("status") == "ERROR" or "error" in content
        
        print(f"\n✓ Invalid metric handled correctly")
    
    def test_invalid_dimension(self):
        """Test query with non-existent dimension."""
        result = call_mcp_tool("create_query", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "nonexistent_dimension"}],
            "limit": 10
        })
        
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        
        # Should return error
        assert content.get("status") == "ERROR" or "error" in content
        
        print(f"\n✓ Invalid dimension handled correctly")


@pytest.mark.integration
@pytest.mark.slow
class TestMCPPerformance:
    """Test MCP performance characteristics."""
    
    def test_query_latency(self):
        """Measure query execution latency."""
        import time
        
        start = time.time()
        result = call_mcp_tool("fetch_query_result", {
            "metrics": ["average_price_usd"],
            "group_by": [{"name": "name"}],
            "limit": 10
        }, timeout=60)
        latency = time.time() - start
        
        assert "result" in result
        content_text = result["result"]["content"][0]["text"]
        content = json.loads(content_text)
        assert content.get("status") == "OK"
        
        print(f"\n✓ Query latency: {latency:.2f}s")
        
        # Warn if too slow
        if latency > 30:
            print(f"⚠️  Query took longer than expected: {latency:.2f}s")
    
    def test_concurrent_queries(self):
        """Test handling multiple concurrent queries."""
        import concurrent.futures
        
        def run_query(i):
            return call_mcp_tool("fetch_query_result", {
                "metrics": ["average_price_usd"],
                "group_by": [{"name": "name"}],
                "limit": 5
            }, timeout=60)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_query, i) for i in range(3)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        assert len(results) == 3
        for result in results:
            content_text = result["result"]["content"][0]["text"]
            content = json.loads(content_text)
            assert content.get("status") == "OK"
        
        print(f"\n✓ Handled 3 concurrent queries successfully")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
