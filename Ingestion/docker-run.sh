#!/bin/bash
# Docker run script for ETL Processor

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}Running ETL Processor Container...${NC}"

# Create data directories if they don't exist
mkdir -p data/input data/output data/config logs

# Check if required files exist
if [ ! -f "data/input/inputdata.csv" ]; then
    echo -e "${YELLOW}Warning: inputdata.csv not found in data/input/ directory${NC}"
    echo "Please place your input CSV file in data/input/ directory"
    echo ""
fi

# Run the container
docker run --rm \
    -v "$(pwd)/data/input:/app/data/input:ro" \
    -v "$(pwd)/data/output:/app/data/output" \
    -v "$(pwd)/data/config:/app/data/config:ro" \
    -v "$(pwd)/logs:/app/logs" \
    -e INPUT_FILE="data/input/inputdata.csv" \
    -e OUTPUT_FILE="data/output/transformed_output.csv" \
    -e LOG_LEVEL="INFO" \
    etl-processor:latest

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ ETL process completed successfully!${NC}"
    echo -e "${BLUE}Check the output in data/output/ directory${NC}"
else
    echo -e "${RED}✗ ETL process failed!${NC}"
    echo -e "${YELLOW}Check the logs in logs/ directory for more details${NC}"
    exit 1
fi
