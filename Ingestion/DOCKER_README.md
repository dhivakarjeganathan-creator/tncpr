# Docker Setup for ETL Processor

This document provides instructions for building and running the ETL Processor application using Docker.

## Prerequisites

- Docker installed on your system
- Docker Compose (optional, for easier management)

## Quick Start

### 1. Build the Docker Image

```bash
# Build the image
docker build -t etl-processor:latest .

# Or use the build script (Linux/Mac)
chmod +x docker-build.sh
./docker-build.sh
```

### 2. Prepare Your Data

Create the required directory structure and place your files:

```bash
# Create directories
mkdir -p data/input data/output data/config logs

# Copy your CSV files to the appropriate directories
cp inputdata.csv data/input/
cp KPIDatasources.csv data/config/
cp KPIDetails.csv data/config/
cp KPIResourceTypes.csv data/config/  # Optional
cp KPIResourceRules.csv data/config/  # Optional
cp KpiRuleRules.csv data/config/      # Optional
```

### 3. Run the Container

#### Option A: Using Docker Run

```bash
# Basic run
docker run --rm \
    -v "$(pwd)/data/input:/app/data/input:ro" \
    -v "$(pwd)/data/output:/app/data/output" \
    -v "$(pwd)/data/config:/app/data/config:ro" \
    -v "$(pwd)/logs:/app/logs" \
    etl-processor:latest

# Or use the run script (Linux/Mac)
chmod +x docker-run.sh
./docker-run.sh
```

#### Option B: Using Docker Compose

```bash
# Start the service
docker-compose up

# Run in detached mode
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

## Configuration

### Environment Variables

You can customize the container behavior using environment variables:

- `INPUT_FILE`: Path to input CSV file (default: `inputdata.csv`)
- `OUTPUT_FILE`: Path to output CSV file (default: `transformed_output.csv`)
- `LOG_LEVEL`: Logging level - DEBUG, INFO, WARNING, ERROR (default: INFO)

### Example with Custom Configuration

```bash
docker run --rm \
    -v "$(pwd)/data:/app/data" \
    -e INPUT_FILE="data/input/mydata.csv" \
    -e OUTPUT_FILE="data/output/myresult.csv" \
    -e LOG_LEVEL="DEBUG" \
    etl-processor:latest
```

## Command Line Options

The container supports various command line options:

```bash
# Show help
docker run --rm etl-processor:latest --help

# Run with custom input/output files
docker run --rm \
    -v "$(pwd)/data:/app/data" \
    etl-processor:latest \
    --input data/input/mydata.csv \
    --output data/output/myresult.csv \
    --log-level DEBUG

# Run tests instead of ETL process
docker run --rm etl-processor:latest --test
```

## Directory Structure

```
project/
├── data/
│   ├── input/          # Input CSV files
│   ├── output/         # Output CSV files
│   └── config/         # Configuration CSV files
├── logs/               # Log files
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── ...
```

## Development Mode

For development with live code reloading:

```bash
# Start development container
docker-compose --profile dev up etl-processor-dev

# Or run tests in development mode
docker-compose --profile dev up etl-processor-dev
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Make sure the data directories have proper permissions
   ```bash
   chmod -R 755 data/
   ```

2. **File Not Found**: Ensure all required CSV files are in the correct directories
   ```bash
   ls -la data/input/
   ls -la data/config/
   ```

3. **Container Won't Start**: Check Docker logs
   ```bash
   docker logs <container_id>
   ```

4. **Memory Issues**: For large files, increase Docker memory limits
   ```bash
   docker run --memory=4g --rm etl-processor:latest
   ```

### Debug Mode

Run the container with debug logging:

```bash
docker run --rm \
    -v "$(pwd)/data:/app/data" \
    -e LOG_LEVEL="DEBUG" \
    etl-processor:latest
```

### Health Check

The container includes a health check. You can monitor it:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' <container_id>

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' <container_id>
```

## Production Deployment

### Security Considerations

- The container runs as a non-root user (`appuser`)
- Input directories are mounted as read-only
- Sensitive data should be handled through environment variables or secrets

### Resource Limits

For production, consider setting resource limits:

```yaml
# In docker-compose.yml
services:
  etl-processor:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
        reservations:
          memory: 1G
          cpus: '0.5'
```

### Monitoring

The container exposes port 8000 for potential future web interface. You can monitor logs:

```bash
# Follow logs in real-time
docker-compose logs -f

# View specific log files
docker exec <container_id> tail -f /app/etl_processor.log
```

## Advanced Usage

### Custom Entrypoint

You can override the default entrypoint for custom behavior:

```bash
docker run --rm \
    -v "$(pwd)/data:/app/data" \
    --entrypoint python \
    etl-processor:latest \
    -c "from etl_processor import ETLProcessor; print('ETL Processor loaded successfully')"
```

### Interactive Mode

For debugging or interactive use:

```bash
docker run --rm -it \
    -v "$(pwd)/data:/app/data" \
    --entrypoint /bin/bash \
    etl-processor:latest
```

### Multi-stage Builds

The Dockerfile is optimized for production with:
- Multi-stage build for smaller image size
- Non-root user for security
- Health checks for monitoring
- Proper layer caching for faster builds

## Support

For issues or questions:
1. Check the logs in the `logs/` directory
2. Review the main README.md for application-specific documentation
3. Ensure all required CSV files are present and properly formatted
