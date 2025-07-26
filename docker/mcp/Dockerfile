FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Required system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-core and metricflow
COPY ./docker/mcp/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire app (or just the MCP script and dbt project)
# Copy dbt files and create necessary directories
COPY ./docker/mcp/app /app
COPY ./docker/spark/spark-utils/adapter_backed_client.py  /usr/local/lib/python3.11/site-packages/dbt_metricflow/cli/dbt_connectors/adapter_backed_client.py



# Run the JSON-RPC MCP server
ENTRYPOINT ["python", "/app/dbt_semantic_layer_mcp_server.py"]
