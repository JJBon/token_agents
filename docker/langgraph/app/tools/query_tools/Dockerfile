# Use Ubuntu base image
FROM ubuntu:22.04

# Set working directory
WORKDIR /app

# Avoid interactive prompts during package install
ENV DEBIAN_FRONTEND=noninteractive

# Install OS dependencies (Python, pip, build tools, Docker CLI, etc.)
RUN apt-get update && \
    apt-get install -y \
        python3.11 \
        python3.11-venv \
        python3-pip \
        build-essential \
        git \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Python 3.11 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \ 
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Copy Python dependencies
COPY ./docker/langgraph/app/tools/query_tools/requirements.txt requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Replace adapter_backed_client.py with custom version
COPY ./docker/spark/spark-utils/adapter_backed_client.py \
     /usr/local/lib/python3.11/dist-packages/dbt_metricflow/cli/dbt_connectors/adapter_backed_client.py

# Copy application code
COPY ./docker/langgraph/app/ .

# Set environment variables (can be overridden at runtime)
ENV AWS_REGION=us-east-1
ENV PYTHONUNBUFFERED=1

# Run the app
CMD ["python", "app.py"]
