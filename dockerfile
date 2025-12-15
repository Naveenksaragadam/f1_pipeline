# 1. Use a stable version (2.10.3 is current stable, 3.0.0 is beta/bleeding edge)
FROM apache/airflow:2.10.3

# 2. Switch to root ONLY for file copying
USER root

# Copy requirements
COPY requirements.txt /requirements.txt

# 3. Switch back to airflow user to install packages
# This ensures libraries are owned by 'airflow' and found in the path
USER airflow

# Install dependencies
RUN pip install --no-cache-dir -r /requirements.txt