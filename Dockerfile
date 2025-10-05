# Extend Astronomer Runtime (Airflow pre-installed)
FROM astrocrpublic.azurecr.io/runtime:3.0-11

# Switch to root for system installations
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /usr/local/airflow

# Install Python dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project (dags/, include/, scripts/, etc.)
COPY . .

# Setup dbt workspace in /usr/local/airflow at build time (persists in image)
# profiles.yml is in config/ subdirectory to avoid local dev conflicts
RUN mkdir -p /usr/local/airflow/dbt_workspace /usr/local/airflow/dbt_logs && \
    cp -r /usr/local/airflow/include/vote_dbt/* /usr/local/airflow/dbt_workspace/ && \
    cd /usr/local/airflow/dbt_workspace && \
    dbt deps --profiles-dir ./config && \
    cd /usr/local/airflow

# Set ownership and permissions
RUN chown -R 50000:50000 /usr/local/airflow && \
    chmod -R 755 /usr/local/airflow && \
    chmod 1777 /tmp

# Switch back to astro user
USER 50000

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/include/scripts"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8