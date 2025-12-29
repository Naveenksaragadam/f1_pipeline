# 1. stable version 
FROM apache/airflow:2.10.3

# 2. Switch to root ONLY for file copying
USER root

# 3. Install uv (copy binary from the official image - fast and secure)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 4. Copy dependency files first (better caching)
# Copying uv.lock is highly recommended for reproducible builds
COPY pyproject.toml uv.lock* ./

# 4. Install dependencies using uv AS ROOT
RUN uv pip install --system --no-cache -r pyproject.toml

# 5. Switch back to airflow user to install packages
# This ensures libraries are owned by 'airflow' and found in the path
USER airflow