#!/bin/bash
# Build script for Docker image

set -e

IMAGE_NAME="wxdata-pipeline"
IMAGE_TAG="latest"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

echo "Building Docker image: ${FULL_IMAGE_NAME}"

# Build the Docker image
docker build -t ${FULL_IMAGE_NAME} .

echo "Docker image built successfully: ${FULL_IMAGE_NAME}"
echo ""
echo "To run the container:"
echo "  docker run --rm ${FULL_IMAGE_NAME}"
echo ""
echo "To run with custom config:"
echo "  docker run --rm -v \$(pwd)/config.yaml:/app/config.yaml ${FULL_IMAGE_NAME} --config /app/config.yaml"
echo ""
echo "To run in continuous mode:"
echo "  docker run --rm ${FULL_IMAGE_NAME} --mode continuous"

