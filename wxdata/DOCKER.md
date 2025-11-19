# Docker Build and Run Instructions

This document provides instructions for building and running the Watsonx.data CSV to Iceberg Pipeline using Docker.

## Prerequisites

- Docker installed and running
- Docker Compose (optional, for easier deployment)

## Building the Docker Image

### Option 1: Using the build script

**Linux/Mac:**
```bash
chmod +x build-docker.sh
./build-docker.sh
```

**Windows:**
```cmd
build-docker.bat
```

### Option 2: Using Docker directly

```bash
docker build -t wxdata-pipeline:latest .
```

## Running the Container

### Basic Run (Single Execution)

```bash
docker run --rm wxdata-pipeline:latest
```

### Run with Custom Config File

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  wxdata-pipeline:latest \
  --config /app/config.yaml
```

### Run in Continuous Mode

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/logs:/app/logs \
  wxdata-pipeline:latest \
  --mode continuous
```

### Run with All Volumes Mounted

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/schemas:/app/schemas:ro \
  -v $(pwd)/logs:/app/logs \
  wxdata-pipeline:latest
```

## Using Docker Compose

### Build and Run

```bash
docker-compose up --build
```

### Run in Detached Mode

```bash
docker-compose up -d
```

### View Logs

```bash
docker-compose logs -f wxdata-pipeline
```

### Stop the Container

```bash
docker-compose down
```

## Environment Variables

You can override configuration using environment variables:

```bash
docker run --rm \
  -e SPARK_MASTER=spark://your-spark-master:7077 \
  -e MINIO_ENDPOINT=minio:9000 \
  wxdata-pipeline:latest
```

## Network Configuration

If you need to connect to external services (MinIO, Hive Metastore, etc.), you may need to:

1. Use Docker networks:
```bash
docker network create wxdata-network
docker run --rm --network wxdata-network wxdata-pipeline:latest
```

2. Or use host network (Linux only):
```bash
docker run --rm --network host wxdata-pipeline:latest
```

## Troubleshooting

### Check if the image was built successfully

```bash
docker images | grep wxdata-pipeline
```

### Inspect the container

```bash
docker run --rm -it wxdata-pipeline:latest /bin/bash
```

### View container logs

```bash
docker logs <container-id>
```

### Check Spark configuration

The Docker image includes:
- Apache Spark 3.5.0
- Iceberg Spark Runtime 1.4.2
- Hadoop AWS libraries for S3 support
- Java 11

## Image Details

- **Base Image**: `openjdk:11-jdk-slim`
- **Python Version**: 3.9
- **Spark Version**: 3.5.0
- **Image Size**: ~1.5GB (approximate)

## Notes

- The Dockerfile uses a multi-stage build for optimization
- Spark and Iceberg JARs are downloaded during build
- Logs are written to `/app/logs` inside the container
- Mount the logs directory to persist logs on the host

