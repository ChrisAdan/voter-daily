# 🗳️ Voter Analytics Pipeline

A comprehensive voter analytics platform built with **Apache Airflow**, **dbt**, **DuckDB**, and **Streamlit** for processing voter registration data and generating actionable insights for political campaigns and civic engagement.

![Pipeline Architecture](https://img.shields.io/badge/Pipeline-ETL%20%2B%20ELT-blue) ![Database](https://img.shields.io/badge/Database-DuckDB-orange) ![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-red) ![Analytics](https://img.shields.io/badge/Analytics-dbt-green) ![Dashboard](https://img.shields.io/badge/Dashboard-Streamlit-purple)

## 🏗️ What It Does

**Data Pipeline**: Processes CSV voter files → DuckDB analytics database → Interactive dashboards  
**Key Analytics**: Voter engagement segmentation, targeting opportunities, demographic insights, geographic analysis  
**Output**: Production-ready insights for voter outreach, campaign strategy, and civic engagement

### Pipeline Flow

```
CSV Files → Python ETL → DuckDB → dbt Transformations → Streamlit Dashboard
    ↓           ↓         ↓            ↓                    ↓
Raw Data → Validation → Storage → Analytics Tables → Interactive Viz
```

## 🚀 Two Ways to Use This Pipeline

### Option 1: Full Airflow Pipeline (Recommended for Production)

**What you get**: Automated daily processing, monitoring, retry logic, web UI

```bash
# Clone and start
git clone https://github.com/ChrisAdan/voter-daily.git
cd voter-daily
astro dev start

# Access interfaces
# Airflow UI: http://localhost:8080 (admin/admin)
# Streamlit Dashboard: http://localhost:8501
```

**Usage**:

- Pipeline runs daily at 6 AM automatically
- Manual trigger: Airflow UI → `voter_analytics_pipeline` → Trigger
- Monitor: View logs and execution status in Airflow
- Results: Dashboard updates after successful pipeline runs

### Option 2: Local Development Mode

**What you get**: Direct script execution, immediate results, development flexibility

```bash
# Clone and setup
git clone https://github.com/ChrisAdan/voter-daily.git
cd voter-daily/include

# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline
python scripts/main.py

# Run dbt transformations
cd vote_dbt
dbt deps && dbt build

# Launch dashboard
cd .. && streamlit run app.py
# Dashboard: http://localhost:8501
```

## 📊 What You'll See

### Dashboard Analytics

- **Executive Summary**: Total voters, engagement rates, key trends
- **Targeting Opportunities**: High-value segments for voter outreach
- **Geographic Analysis**: State-level competitive landscape
- **Demographics**: Age, gender, and partisan breakdowns
- **Engagement Tracking**: Voter lifecycle and participation patterns

### Key Insights Generated

- **Voter Segmentation**: Current, Occasional, Dormant, Never Voted classifications
- **Targeting Tiers**: High/Medium/Low priority segments based on engagement potential
- **Geographic Hotspots**: Competitive states with high recovery opportunities
- **Trend Analysis**: Participation changes across election cycles

## 📋 Data Requirements

### CSV Input Format

```csv
id,first_name,last_name,age,gender,state,party,email,registered_date,last_voted_date
1,John,Smith,45,M,CA,Democrat,john.smith@email.com,2010-03-15,2020-11-03
```

**Required Fields**: All 10 columns must be present  
**Location**: Place CSV files in `include/data/raw/`  
**Processing**: Automatic deduplication, schema validation, error handling

## 🎯 Key Features

### Data Processing

- ✅ **Schema Validation**: Automatic error detection and recovery
- ✅ **Deduplication**: Hash-based duplicate prevention
- ✅ **Incremental Loading**: Only processes new/modified files
- ✅ **Error Handling**: Robust processing with detailed logging
- ✅ **Audit Trail**: Complete data lineage and processing history

### Analytics Engine

- 📈 **Multi-Layer Architecture**: Raw → Dimensions → Staging → Marts
- 🎯 **Engagement Scoring**: Sophisticated voter lifecycle analysis
- 📊 **Cross-Tabulation**: Demographics vs. voting patterns
- 🗺️ **Geographic Analysis**: State-level competitive insights
- ⏱️ **Time Series**: Trends across election cycles

### Dashboard & Visualization

- 📱 **Interactive Streamlit UI**: Point-and-click analytics
- 📈 **Real-time Charts**: Dynamic visualizations update with data
- 🎛️ **Filtering Controls**: Drill down by state, demographics, engagement
- 📊 **Multiple Views**: Executive summary, detailed breakdowns, trends

## 🔧 Technical Stack

- **Orchestration**: Apache Airflow (via Astronomer Runtime)
- **Database**: DuckDB (high-performance analytics)
- **Transformations**: dbt (data modeling and testing)
- **Visualization**: Streamlit (interactive dashboards)
- **Container**: Docker (consistent deployment environment)
- **Language**: Python 3.11+

## 🧪 Data Quality & Testing

- **Schema Validation**: Ensures CSV files match expected format
- **dbt Tests**: 15+ data quality checks on transformed tables
- **Range Validation**: Age (18-100), valid state codes, email formats
- **Referential Integrity**: Cross-table consistency checks
- **Unit Tests**: Python test suite with pytest

## 📈 Production Ready Features

### Reliability

- **Retry Logic**: Automatic retry on transient failures (2x with 5min delay)
- **Error Recovery**: Graceful handling of malformed data
- **Monitoring**: Comprehensive logging and execution tracking
- **Concurrency Control**: Prevents overlapping pipeline runs

### Performance

- **Incremental Processing**: Only new/modified data processed
- **Batch Processing**: Configurable batch sizes for large datasets
- **Efficient Storage**: DuckDB columnar format optimized for analytics
- **Resource Management**: Proper memory and compute allocation

### Security & Compliance

- **Data Privacy**: Minimal PII storage with optional email hashing
- **Access Control**: Role-based permissions in Airflow
- **Audit Logging**: Complete data lineage and access tracking
- **Retention Policies**: Configurable data retention management

## 🚀 Quick Results

**Setup Time**: ~5 minutes from clone to running dashboard  
**Data Processing**: ~30 seconds for sample dataset (10 records)  
**Dashboard Load**: ~10 seconds after pipeline completion  
**Scale**: Tested with datasets up to 100K records

## 🔮 What's Next

### Immediate Use Cases

- **Campaign Targeting**: Identify high-value voter segments for outreach
- **Resource Allocation**: Focus efforts on competitive districts
- **Trend Analysis**: Track engagement changes over election cycles
- **Data-Driven Strategy**: Replace intuition with quantified insights

### Future Enhancements

- **Predictive Modeling**: Turnout prediction and voter likelihood scoring
- **Real-time Processing**: Streaming data updates for live campaigns
- **Advanced Segmentation**: ML-based voter classification
- **API Integration**: RESTful endpoints for external tools

---

## 🏁 Ready to Start?

Choose your path:

**🏢 Production Pipeline** → `astro dev start` → Access Airflow UI  
**🛠️ Development Mode** → `python scripts/main.py` → Run Streamlit

Both paths lead to the same powerful voter analytics insights. The choice depends on whether you want automated orchestration (Option 1) or direct control (Option 2).

**Questions?** Check the detailed documentation in each component's README or examine the sample data in `include/data/raw/sample_voter_data.csv`.

---

_Built for organizations working to strengthen democracy through data-driven voter engagement and civic participation._

```bash
Note: This README was generated automatically using Cline.ai
```
