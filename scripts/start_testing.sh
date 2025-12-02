#!/bin/bash
# Quick start script for testing Query Agent
# Usage: ./scripts/start_testing.sh

set -e

echo "=========================================="
echo "Query Agent Testing Quick Start"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ docker-compose not found${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 1: Starting Docker services...${NC}"
docker-compose -f docker/spark/docker-compose.yml up -d

echo ""
echo -e "${YELLOW}Step 2: Waiting for services to be ready (60 seconds)...${NC}"
sleep 60

echo ""
echo -e "${YELLOW}Step 3: Checking service health...${NC}"

# Check Spark
if docker-compose -f docker/spark/docker-compose.yml exec -T spark-master pgrep -f "org.apache.spark" > /dev/null; then
    echo -e "${GREEN}✓ Spark Master is running${NC}"
else
    echo -e "${RED}✗ Spark Master is not running${NC}"
fi

# Check MCP server
if curl -s http://localhost:8001/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ MCP server is responding${NC}"
else
    echo -e "${YELLOW}⚠ MCP server not responding (may still be starting)${NC}"
fi

# Check LiteLLM
if curl -s http://localhost:4000/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ LiteLLM is responding${NC}"
else
    echo -e "${YELLOW}⚠ LiteLLM not responding (may still be starting)${NC}"
fi

echo ""
echo -e "${YELLOW}Step 4: Starting Spark Thrift Server...${NC}"
docker-compose -f docker/spark/docker-compose.yml exec -T spark-master \
    /spark_utils/start-thrift-server.sh || echo -e "${YELLOW}⚠ Thrift server may already be running${NC}"

echo ""
echo "=========================================="
echo -e "${GREEN}Services are ready!${NC}"
echo "=========================================="
echo ""
echo "What would you like to do?"
echo ""
echo "1. Run integration tests (MCP + dbt + Spark)"
echo "2. Run E2E tests (full Query Agent)"
echo "3. Run interactive test script"
echo "4. View service logs"
echo "5. Stop all services"
echo "6. Exit"
echo ""

read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        echo ""
        echo -e "${YELLOW}Running integration tests...${NC}"
        pytest tests/integration/test_mcp_dbt_integration.py -v -s
        ;;
    2)
        echo ""
        echo -e "${YELLOW}Running E2E tests...${NC}"
        pytest tests/e2e/test_query_agent_e2e.py -v -s
        ;;
    3)
        echo ""
        echo -e "${YELLOW}Starting interactive test script...${NC}"
        python scripts/test_query_agent_interactive.py
        ;;
    4)
        echo ""
        echo -e "${YELLOW}Viewing logs (Ctrl+C to exit)...${NC}"
        docker-compose -f docker/spark/docker-compose.yml logs -f
        ;;
    5)
        echo ""
        echo -e "${YELLOW}Stopping services...${NC}"
        docker-compose -f docker/spark/docker-compose.yml down
        echo -e "${GREEN}✓ Services stopped${NC}"
        ;;
    6)
        echo ""
        echo "Goodbye!"
        exit 0
        ;;
    *)
        echo ""
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Testing complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  - Review test results above"
echo "  - Check logs: docker-compose -f docker/spark/docker-compose.yml logs"
echo "  - Run more tests: pytest tests/ -v"
echo "  - Interactive mode: python scripts/test_query_agent_interactive.py"
echo ""
