# Voter Analytics Pipeline

Daily processing pipeline for Good Party voter analytics with automated ETL and dashboard visualization.

## Architecture

- **Apache Airflow**: Orchestrates daily data pipeline
- **DuckDB**: High-performance analytics database
- **dbt**: Data transformation and modeling
- **Streamlit**: Interactive analytics dashboard

## Running the Pipeline

### Scheduled Pipeline (Production)

```bash
# Start Airflow with Astro CLI
astro dev start

# Pipeline runs daily automatically
# Access Airflow UI: http://localhost:8080
```

### Dashboard (Local Development)

```bash
# Clone repository
git clone https://github.com/ChrisAdan/voter-daily.git
cd voter-analytics/include

# Install dependencies
pip install -r requirements.txt

# Run ETL manually
python scripts/main.py

# Run dbt transformations
cd vote_dbt
dbt debug
dbt deps
dbt compile
dbt build

# Launch dashboard
streamlit run ../app.py
```

## Pipeline Steps

1. **Extract/Load**: Processes CSV voter data into DuckDB
2. **dbt deps**: Installs transformation dependencies
3. **dbt compile**: Validates model syntax
4. **dbt build**: Runs transformations and tests

## Files

- `voter_analytics_pipeline.py`: Main Airflow DAG
- `main.py`: Standalone ETL runner
- `app.py`: Streamlit analytics interface

---

```bash
Note: This README was generated automatically using Cline.ai
```
