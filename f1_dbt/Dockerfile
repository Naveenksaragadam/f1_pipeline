FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install dbt-clickhouse
RUN pip install --no-cache-dir dbt-clickhouse==1.8.3

# Set working directory
WORKDIR /usr/app/f1_dbt

ENTRYPOINT ["dbt"]
