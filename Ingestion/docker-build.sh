#!/bin/bash
# Docker build script for ETL Processor

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building ETL Processor Docker Image...${NC}"

# Build the Docker image
docker build -t etl-processor:latest .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Docker image built successfully!${NC}"
    echo -e "${YELLOW}Image name: etl-processor:latest${NC}"
    echo ""
    echo "To run the container:"
    echo "  docker run --rm -v \$(pwd)/data:/app/data etl-processor:latest"
    echo ""
    echo "Or use docker-compose:"
    echo "  docker-compose up"
else
    echo -e "${RED}✗ Docker build failed!${NC}"
    exit 1
fi
