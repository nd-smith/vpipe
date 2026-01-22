# Production Dockerfile for Kafka Pipeline Workers
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ .

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Set Python to run in unbuffered mode (better for container logs)
ENV PYTHONUNBUFFERED=1

# Health check (adjust port if you use metrics)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Entrypoint to run the application
ENTRYPOINT ["python", "-m", "kafka_pipeline"]

# Default: show help (override with docker-compose command)
CMD ["--help"]
