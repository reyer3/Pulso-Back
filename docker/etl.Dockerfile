# 📊 ETL Pipeline Container
FROM python:3.11-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Development stage
FROM base as development
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .
CMD ["python", "etl/etl_main.py"]

# Production stage
FROM base as production

# Create non-root user
RUN useradd --create-home --shell /bin/bash pulso
USER pulso

# Copy application code
COPY --chown=pulso:pulso . /app/

# Create required directories
RUN mkdir -p /app/logs /app/credentials /app/etl_outputs

# Setup cron job for ETL
COPY docker/etl-crontab /tmp/etl-crontab
RUN crontab /tmp/etl-crontab

# Start ETL scheduler
CMD ["python", "etl/etl_main.py", "--scheduler"]