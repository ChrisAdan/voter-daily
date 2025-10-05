# Voter Analytics Pipeline

A production data pipeline transforming raw voter registration data into analytics-ready insights for independent and non-partisan campaign strategy.

## Architecture

```
Raw CSV Files → Python ETL → DuckDB → dbt Transformations → Streamlit Dashboard
```

## Components

### `/scripts` - Data Ingestion

- **`extract_load.py`**: Pandas-based CSV processor with schema validation and deduplication
- **`main.py`**: Pipeline orchestrator and entry point
- **`seed_elections.py`**: Election calendar generator (MIT historic data + Google Civic API)

### `/vote_dbt` - Data Transformation

- **dbt project** implementing medallion architecture (raw → staging → mart)
- **4 mart tables**: voter_snapshot, partisan_trends, targeting_opportunities, state_summary
- **Full documentation**: See `/vote_dbt/README.md`

### `/app.py` - Visualization

- **Streamlit dashboard** consuming mart tables
- Interactive visualizations for voter engagement and demographic analysis

### `/test` - Quality Assurance

- pytest suite for ETL validation
- dbt schema tests and data quality checks

## Quick Start

```bash
# 1. Ingest raw voter data
python include/scripts/main.py

# 2. Run dbt transformations
cd include/vote_dbt
dbt run
dbt test

# 3. Launch dashboard
streamlit run include/app.py
```

## Data Flow

1. **Extract/Load**: CSV files → `raw.vote_records` (DuckDB)
2. **Dimension**: Deduplication → `dim_voter`
3. **Staging**: Enrichment → `stage_voter_metrics`
4. **Mart**: Aggregation → 4 analytics tables
5. **Visualization**: Streamlit reads marts

## Key Features

- **Incremental processing** with deduplication via MD5 hashing
- **Behavioral segmentation** (Current Voter → Never Voted)
- **Opportunity scoring** for targeted voter re-engagement
- **Partisan trend analysis** across election cycles (2008-2024)
- **Data quality testing** via dbt-expectations

## Dependencies

- Python 3.9+: pandas, duckdb, streamlit, plotly
- dbt Core 1.6+ with DuckDB adapter
- pytest for testing

## Documentation

- **dbt models**: `/vote_dbt/README.md`
- **API reference**: `/vote_dbt/models/schema.yml`
- **Architecture diagrams**: `/vote_dbt/docs/lineage.png`

---

```bash
Note: This README was generated automatically using Claude.ai
```
