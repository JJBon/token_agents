# mcp_config.py
import os

def default_mcp_servers():
    """
    Configure MCP servers for MultiServerMCPClient.
    For Docker-in-Docker, prefer 127.0.0.1 or the service name.
    """
    servers = {}

    # DBT semantic MCP server exposed by your Uvicorn app:
    # If you didn't set a custom route, the path is usually "/"
    mcp_url = os.getenv("MCP_DBT_URL", "http://127.0.0.1:8001")  # 0.0.0.0 is "listen on all"; clients should use 127.0.0.1
    mcp_path = os.getenv("MCP_DBT_PATH", "/mcp")                    # change to "/mcp" if your server mounts there
    transport = os.getenv("MCP_DBT_TRANSPORT", "streamable_http")           # "http" (FastMCP v2) or "streamable_http" (official)

    headers = {}
    if os.getenv("MCP_DBT_BEARER"):
        headers["Authorization"] = f"Bearer {os.getenv('MCP_DBT_BEARER')}"

    servers["dbt-semantic"] = {
        "url": mcp_url.rstrip("/") + mcp_path,
        "transport": transport,         # "http" or "streamable_http"
        "headers": headers or None,
    }

    return servers
