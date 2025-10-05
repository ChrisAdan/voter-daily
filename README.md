# 🗳️ Voter Analytics Pipeline

A production voter analytics platform built with **Apache Airflow**, **dbt**, **DuckDB**, and **Streamlit** demonstrating enterprise data engineering patterns: schema enforcement, data contracts, incremental processing, and orchestration best practices.

![Pipeline Architecture](https://img.shields.io/badge/Pipeline-ETL%20%2B%20ELT-blue) ![Database](https://img.shields.io/badge/Database-DuckDB-orange) ![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-red) ![Analytics](https://img.shields.io/badge/Analytics-dbt-green) ![Dashboard](https://img.shields.io/badge/Dashboard-Streamlit-purple)

## 🏗️ Architecture

**Data Pipeline**: CSV voter files + election calendar → DuckDB → dbt transformations → Streamlit dashboards  
**Key Features**: Voter engagement segmentation, targeting opportunities, demographic insights, geographic analysis, historic and upcoming election tracking

### Pipeline Flow

```
CSV Files → Python ETL (Pandas) → DuckDB → dbt (Medallion) → Streamlit
    ↓              ↓                  ↓          ↓                ↓
Validation → Deduplication → Storage → Analytics → Interactive Viz

Election Seeds (MIT + Google Civic) → dbt → Analytics Integration
```

## 💼 Senior Data Engineering Highlights

### Data Quality & Contracts

- **Enforced dbt Contracts**: All mart models use `contract: enforced: true` with explicit column types
- **Strong Typing**: Explicit data types across all layers (pandas → DuckDB → dbt)
- **Comprehensive Testing**: 15+ dbt tests using `dbt-expectations` package (range validation, regex, referential integrity)
- **Schema Validation**: Runtime schema checks with detailed error handling
- **Data Contracts**: Type-safe interfaces between pipeline stages

### Orchestration & Reliability

- **Airflow DAGs**: Three production DAGs with dependency management and retry logic
- **Incremental Processing**: Timestamp-based change detection in ETL and dbt models
- **Idempotency**: Safe re-runs with deduplication (MD5 hashing) and marker files
- **Concurrency Control**: DuckDB single-writer constraint enforced via `max_active_tasks=1`
- **Error Recovery**: Graceful handling of malformed data with configurable thresholds

### Data Modeling Best Practices

- **Medallion Architecture**: Raw → Dimension → Staging → Mart layers
- **dbt Macros**: Reusable logic for election cycle calculations
- **Cosmos Integration**: Seamless dbt execution within Airflow
- **Incremental Models**: Optimized processing using `unique_key` and timestamp filters
- **Seed Management**: Automated refresh of external data sources

## 🚀 Quick Start

### Production (Airflow)

```bash
git clone https://github.com/ChrisAdan/voter-daily.git
cd voter-daily
astro dev start

# Airflow UI: http://localhost:8080 (admin/admin)
# Streamlit: http://localhost:8501

# First time: Trigger dbt_workspace_setup DAG
# Then: voter_analytics_pipeline runs daily at 6 AM UTC
# Monthly: election_calendar_seed_monthly refreshes calendar
```

### Local Development

```bash
cd voter-daily/include
pip install -r requirements.txt

# Generate election seeds, run ETL, transform with dbt
python scripts/seed_elections.py
python scripts/main.py
cd vote_dbt && dbt deps && dbt seed && dbt build

# Launch dashboard
cd .. && streamlit run app.py
```

## 📊 Data Requirements

### Voter CSV Format

```csv
id,first_name,last_name,age,gender,state,party,email,registered_date,last_voted_date
1,John,Smith,45,M,CA,Democrat,john.smith@email.com,2010-03-15,2020-11-03
```

**Location**: `include/data/raw/`  
**Processing**: Schema validation, deduplication, incremental loading

### Election Calendar

- **Historic**: MIT Election Lab (1976-2020) with vote totals and winners
- **Upcoming**: Google Civic API + calculated federal schedule (2021-2030)
- **Refresh**: Monthly via automated DAG

## 🎯 Technical Features

### Data Engineering

- ✅ **Pandas-First ETL**: Efficient CSV processing with error handling
- ✅ **Type Safety**: Explicit typing throughout pipeline (pandas dtypes → DuckDB → dbt contracts)
- ✅ **Hash-Based Deduplication**: MD5 hashing on composite keys
- ✅ **Incremental Patterns**: Both ETL and dbt layers support incremental processing
- ✅ **Audit Trails**: Complete data lineage with inserted_at/updated_at timestamps

### Analytics Layer

- 📈 **4 Mart Tables**: voter_snapshot, partisan_trends, targeting_opportunities, state_summary
- 🎯 **Engagement Scoring**: Composite 0-100 scale with weighted factors
- 📊 **Time Series**: Participation trends across election cycles (2008-2024)
- 🗺️ **Geographic Analysis**: State-level competitive landscape
- 🗳️ **Election Integration**: Historic outcomes and upcoming calendar

### Infrastructure

- **Orchestration**: Astronomer Airflow with Cosmos for dbt integration
- **Database**: DuckDB columnar storage optimized for analytics
- **Transformations**: dbt with contracts, tests, and documentation
- **Visualization**: Streamlit with Plotly for interactive dashboards
- **Containerization**: Docker for consistent deployment

## 🧪 Data Quality

- **dbt-expectations**: Advanced testing (regex, ranges, uniqueness)
- **Contract Enforcement**: Type-safe interfaces on all mart models
- **Referential Integrity**: Cross-table consistency checks
- **Range Validation**: Age (18-120), dates, percentages (0-100)
- **Unit Tests**: pytest suite for ETL validation

## 📈 Production Features

### Reliability

- **Retry Logic**: 2 retries with 5-minute delays on transient failures
- **Error Thresholds**: Configurable malformed data limits (5% default)
- **Monitoring**: Comprehensive logging and execution tracking
- **Sequential Processing**: DAG-level task ordering for database consistency

### Performance

- **Incremental Loading**: Only new/modified records processed
- **Batch Processing**: Configurable batch sizes (1000 rows default)
- **Columnar Storage**: DuckDB optimization for analytics queries
- **Connection Management**: Explicit connection closing for DuckDB single-writer constraint

## 🔮 Key Insights

- **Voter Segmentation**: 6 behavioral categories (Current → Never Voted)
- **Targeting Tiers**: High/Medium/Low priority based on opportunity scoring
- **Competitive States**: Partisan lean classification with engagement opportunities
- **Historic Trends**: Participation rates across 9 election cycles
- **Upcoming Calendar**: Federal elections through 2030

---

## 🏁 Ready to Start?

**Production**: `astro dev start` → Trigger setup DAG → Monitor in Airflow UI  
**Development**: `python scripts/seed_elections.py` → `python scripts/main.py` → `dbt build` → `streamlit run app.py`

**Documentation**: Check `include/vote_dbt/README.md` for dbt details and `dags/README.md` for orchestration patterns.

---

_Built for organizations working to strengthen democracy through data-driven voter engagement and civic participation._

```bash
Note: This README was generated automatically using Claude.ai
```
