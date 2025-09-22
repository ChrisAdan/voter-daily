# GoodParty Voter Data Pipeline

A robust ETL pipeline for processing voter registration data using Apache Airflow [via Astro] and DuckDB.

## 🏗️ Architecture

```
include/
├── data/
│   ├── raw/              # Source CSV files
│   ├── goodparty.duckdb # DuckDB database
│   └── .last_run_timestamp # Processing metadata
└── scripts/
    ├── main.py           # Main entry point
    └── extract_load.py   # ETL processing logic
```

## 📊 Data Schema

### Source CSV Schema

- `id` - Voter ID (string)
- `first_name` - First name (string)
- `last_name` - Last name (string)
- `age` - Age in years (integer)
- `gender` - Gender (string)
- `state` - State abbreviation (string)
- `party` - Political party affiliation (string)
- `email` - Email address (string)
- `registered_date` - Voter registration date (YYYY-MM-DD)
- `last_voted_date` - Last voting date (YYYY-MM-DD)

### Database Schema (raw.vote_records)

- `record_uuid` - Unique primary key (UUID)
- `source_id` - Original CSV ID (string)
- `first_name` - First name (string)
- `last_name` - Last name (string)
- `age` - Age in years (integer)
- `gender` - Gender (string)
- `state` - State abbreviation (string)
- `party` - Political party affiliation (string)
- `email` - Email address (string)
- `registered_date` - Registration date (date)
- `last_voted_date` - Last voting date (date)
- `source_file` - Source filename (string)
- `record_hash` - Deduplication hash (string)
- `inserted_at` - Record insertion timestamp
- `updated_at` - Record update timestamp

## 🚀 Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

### Directory Setup

```bash
mkdir -p include/data/raw
mkdir -p include/scripts
```

### Running the Pipeline

```bash
cd include/scripts
python main.py
```

## ✨ Features

### Data Quality & Integrity

- **Schema Validation**: Ensures all required columns are present
- **Data Type Conversion**: Automatic type casting with error handling
- **Duplicate Prevention**: Hash-based deduplication using MD5
- **Primary Key Generation**: UUID-based unique identifiers
- **Audit Trail**: Full lineage tracking with timestamps

### Performance & Reliability

- **Incremental Processing**: Only processes files modified since last run
- **Batch Processing**: Efficient bulk inserts using pandas + DuckDB
- **Idempotent Operations**: Safe to re-run without data corruption
- **Error Resilience**: Continues processing despite individual record errors
- **Memory Efficient**: Streams large files in configurable batches

### Operational Features

- **Multiple Run Modes**: Append (default) or truncate options
- **Progress Tracking**: Real-time processing statistics
- **Timestamp Tracking**: Database and file-based run history
- **Interactive Mode**: User confirmation before execution
- **Comprehensive Logging**: Detailed success/error reporting

## 🔧 Configuration Options

### Database Configuration

```python
# Default DuckDB path
db_path = "include/data/goodparty.duckdb"

# Custom database type (currently supports 'duckdb')
processor = VoteDataProcessor(db_path="custom/path/db.duckdb")
```

### Processing Modes

```python
# Append mode (default) - only new/modified files
processor.run_pipeline(truncate_mode=False)

# Truncate mode - full refresh
processor.run_pipeline(truncate_mode=True)
```

### Batch Size Tuning

```python
# In extract_load.py, modify batch_size for performance tuning
batch_size = 1000  # Adjust based on memory constraints
```

## 🔍 Data Validation

The pipeline performs comprehensive validation:

1. **File Validation**: Checks for CSV format and required headers
2. **Data Type Validation**: Converts and validates each field type
3. **Date Parsing**: Supports multiple date formats (YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY)
4. **Null Handling**: Graceful handling of missing/empty values
5. **Duplicate Detection**: Hash-based deduplication across runs

## 📈 Monitoring & Observability

### Processing Metadata

The pipeline maintains processing history in `raw.processing_metadata`:

- Last processing timestamp
- Files processed count
- Records processed count

### Error Handling

- Individual record errors don't stop pipeline execution
- Comprehensive error reporting with context
- Failed records are logged but skipped

## 🔄 Integration with Apache Airflow

This pipeline is designed to be called from Airflow DAGs:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract_load import VoteDataProcessor

def run_voter_data_pipeline():
    processor = VoteDataProcessor()
    result = processor.run_pipeline()
    return result

# DAG definition
dag = DAG('goodparty_voter_pipeline', schedule_interval='@daily')

process_task = PythonOperator(
    task_id='process_voter_data',
    python_callable=run_voter_data_pipeline,
    dag=dag
)
```

## 🧪 Testing

Sample test data is provided in `include/data/raw/sample_voter_data.csv`.

Run the pipeline manually to test:

```bash
python include/scripts/main.py
```

Unit tests will be implemented in a forthcoming update.

## 📋 Future Enhancements

- [ ] Visualization (Analytics & Lineage)
- [ ] Support for multiple database backends (PostgreSQL, Snowflake)
- [ ] Data quality metrics and alerting
- [ ] Automated schema evolution
- [ ] Partitioning strategies for large datasets

---

```bash
Note: This README was autogenerated by Cline.ai. I use these as the jumping off point, and this file will be refined as aspects of the project are further developed.
```
