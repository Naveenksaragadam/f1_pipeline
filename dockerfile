# 1. Stable Airflow base
FROM apache/airflow:2.10.3

# 2. Switch to root for installations
USER root

# 3. Install uv (fast Python package installer)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 4. Copy dependency files (layer caching optimization)
COPY pyproject.toml uv.lock* ./

# 5. Install dependencies using uv
# Fix: Use '.' not '-r' for pyproject.toml
RUN uv pip install --system --no-cache .

# 6. Switch back to airflow user
USER airflow