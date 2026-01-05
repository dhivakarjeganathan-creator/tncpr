FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Add proxy settings for build
ENV http_proxy=http://vzproxy.verizon.com:9290
ENV https_proxy=http://vzproxy.verizon.com:9290
ENV HTTP_PROXY=http://vzproxy.verizon.com:9290
ENV HTTPS_PROXY=http://vzproxy.verizon.com:9290

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY config.py .

# Copy all modules
COPY api/ ./api/
COPY database/ ./database/
COPY utils/ ./utils/

# Create non-root user and group
RUN groupadd -r appuser -g 1001 && \
    useradd -r -u 1001 -g appuser appuser

# Create log directory and set permissions
RUN mkdir -p /opt/logs && \
    chown -R appuser:appuser /app /opt/logs && \
    chmod -R 755 /app && \
    chmod 644 /app/*.py && \
    chmod -R 755 /app/api && \
    chmod -R 755 /app/database && \
    chmod -R 755 /app/utils && \
    chmod -R 777 /opt/logs

# Unset proxy for runtime
ENV http_proxy=
ENV https_proxy=
ENV HTTP_PROXY=
ENV HTTPS_PROXY=

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER 1001

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/api/v1/health', timeout=5)" || exit 1

# Run the API server
CMD ["python", "-u", "main.py"]
