# Voter Analytics Pipeline

Automated data pipeline for Good Party voter analytics with orchestrated ETL, transformations, and dashboard visualization.

## Architecture

- **Apache Airflow (Astronomer)**: DAG orchestration and scheduling
- **DuckDB**: High-performance analytics database
- **dbt (via Cosmos)**: Data transformation and modeling
- **Streamlit**: Interactive analytics dashboard

## Airflow DAGs

### 1. `dbt_workspace_setup` (One-time)

**Purpose**: Initialize DuckDB database and workspace environment  
**Trigger**: Manual (runs once per container lifecycle)  
**Tasks**:

- Check if setup already complete
- Initialize DuckDB with raw schema
- Create sample voter data
- Mark setup complete

**Run this first** before any other DAGs.

### 2. `voter_analytics_pipeline` (Daily)

**Purpose**: Core ETL pipeline for voter data processing  
**Schedule**: Daily  
**Tasks**:

1. Verify workspace setup
2. Extract and load voter CSV data to DuckDB
3. Run dbt transformations (staging → marts)
4. Generate analytics models

**Dependencies**: Requires `dbt_workspace_setup` to complete first.

### 3. `election_calendar_seed_monthly` (Monthly)

**Purpose**: Refresh upcoming election data from Google Civic API  
**Schedule**: 1st of each month at 6 AM UTC  
**Tasks**:

1. Verify workspace setup
2. Generate election seed files (MIT Election Lab + Google Civic API)
3. Load seeds into DuckDB via dbt
4. Verify data integrity

**Dependencies**: Requires `dbt_workspace_setup` to complete first.

## Getting Started

### Production (Airflow)

```bash
# Start Airflow
astro dev start

# Access UI at http://localhost:8080

# Run setup DAG (first time only)
# Trigger: dbt_workspace_setup

# Daily pipeline runs automatically
# Manual trigger available for voter_analytics_pipeline
```

### Local Development (Standalone)

```bash
# Clone repository
git clone https://github.com/ChrisAdan/voter-daily.git
cd voter-analytics/include

# Install dependencies
pip install -r requirements.txt

# Run ETL
python scripts/main.py

# Run dbt
cd vote_dbt
dbt build

# Launch dashboard
streamlit run ../app.py
```

## Key Features

- **Idempotent Setup**: Workspace initialization runs once, subsequent runs skip safely
- **Sequential Processing**: DuckDB single-writer constraint enforced via `max_active_tasks=1`
- **Persistent Workspace**: `/usr/local/airflow/dbt_workspace` created at build time, database initialized at runtime
- **Cosmos Integration**: dbt operations managed via Astronomer Cosmos for seamless Airflow integration
- **Election Data**: Automated monthly refresh of federal and local election calendars

## Files

- `dags/setup_environment.py`: One-time workspace setup
- `dags/voter_analytics_pipeline.py`: Daily voter ETL and transformations
- `dags/election_calendar_monthly.py`: Monthly election data refresh
- `include/scripts/main.py`: Standalone ETL runner
- `include/app.py`: Streamlit dashboard

---

```bash
Note: This README was generated automatically using Claude.ai
```
