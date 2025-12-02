#!/bin/bash
# Run tests inside the langgraph-backend container where all dependencies are installed

set -e

echo "=========================================="
echo "Running Tests in Docker Container"
echo "=========================================="
echo ""

# Check if services are running
if ! docker-compose -f docker/spark/docker-compose.yml ps | grep -q "langgraph-backend"; then
    echo "❌ langgraph-backend container is not running"
    echo "Start services with: docker-compose -f docker/spark/docker-compose.yml up -d"
    exit 1
fi

# Default to integration tests if no argument provided
TEST_PATH="${1:-tests/integration/test_query_agent_simple.py}"

echo "Running: pytest $TEST_PATH -v -s"
echo ""

# Run pytest inside the container
docker-compose -f docker/spark/docker-compose.yml exec -T langgraph-backend \
    pytest "$TEST_PATH" -v -s

echo ""
echo "=========================================="
echo "Tests Complete"
echo "=========================================="
