# Dockerfile for LangGraph + Gradio UI + Bedrock Client
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install OS dependencies
RUN apt-get update && \
    apt-get install -y build-essential git curl docker.io && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./docker/langgraph/requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./docker/spark/spark-utils/adapter_backed_client.py  /usr/local/lib/python3.11/site-packages/dbt_metricflow/cli/dbt_connectors/adapter_backed_client.py


# Copy application code
COPY ./docker/langgraph/app/ .

# Set environment (you can override with Docker Compose)
ENV AWS_REGION=us-east-1
ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]