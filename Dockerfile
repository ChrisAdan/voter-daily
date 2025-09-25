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

# Create necessary directories with proper permissions BEFORE copying files
RUN mkdir -p /tmp && \
    mkdir -p /usr/local/airflow/logs && \
    mkdir -p /usr/local/airflow/include/data/raw && \
    mkdir -p /usr/local/airflow/include/scripts && \
    mkdir -p /usr/local/airflow/include/vote_dbt && \
    chmod 755 /tmp && \
    chmod -R 755 /usr/local/airflow

# Copy the entire project (dags/, include/, scripts/, etc.)
COPY . .

# Create dbt profiles.yml in proper location (container-specific)
RUN mkdir -p /usr/local/airflow/include/vote_dbt && \
    mkdir -p /tmp/dbt_logs && \
    mkdir -p /usr/local/airflow/include/vote_dbt/dbt_packages && \
    chown -R 50000:50000 /tmp/dbt_logs && \
    chmod -R 755 /tmp/dbt_logs && \
    echo "vote_dbt:" > /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "  target: prod" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "  outputs:" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "    prod:" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "      type: duckdb" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "      path: /tmp/goodparty_prod.duckdb" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "      schema: raw" >> /usr/local/airflow/include/vote_dbt/profiles.yml && \
    echo "      threads: 4" >> /usr/local/airflow/include/vote_dbt/profiles.yml

# Add sample data file (ensure directory exists first)
RUN mkdir -p /usr/local/airflow/include/data/raw && \
    echo 'id,first_name,last_name,age,gender,state,party,email,registered_date,last_voted_date' > /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '1,John,Smith,45,M,CA,Democrat,john.smith@email.com,2010-03-15,2020-11-03' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '2,Sarah,Johnson,32,F,NY,Republican,sarah.j@email.com,2012-08-22,2022-11-08' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '3,Michael,Brown,28,M,TX,Independent,m.brown@email.com,2018-01-10,' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '4,Emily,Davis,67,F,FL,Democrat,emily.davis@email.com,2008-05-20,2024-11-05' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '5,Robert,Wilson,41,M,PA,Republican,rob.wilson@email.com,2015-09-12,2020-11-03' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '6,Lisa,Anderson,35,F,OH,Independent,lisa.a@email.com,2016-02-28,2018-11-06' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '7,David,Taylor,52,M,MI,Democrat,david.taylor@email.com,2009-07-18,2022-11-08' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '8,Jennifer,Thomas,29,F,WI,Republican,jen.thomas@email.com,2019-11-01,2022-11-08' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '9,James,Jackson,73,M,AZ,Democrat,j.jackson@email.com,2007-01-15,2020-11-03' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv && \
    echo '10,Mary,White,38,F,NC,Independent,mary.white@email.com,2013-06-30,2016-11-08' >> /usr/local/airflow/include/data/raw/sample_voter_data.csv

# Set all ownership to astro user (UID 50000) and ensure proper permissions
# This must be done as root AFTER copying files to fix ownership issues
RUN chown -R 50000:50000 /usr/local/airflow && \
    chown -R 50000:50000 /tmp && \
    chmod -R 755 /usr/local/airflow && \
    chmod 1777 /tmp && \
    # Force remove any existing dbt_packages that may have wrong ownership
    rm -rf /usr/local/airflow/include/vote_dbt/dbt_packages || true

# Switch back to astro user
USER 50000

# Set environment variables
ENV DBT_PROFILES_DIR=/usr/local/airflow/include/vote_dbt
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/include/scripts"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8