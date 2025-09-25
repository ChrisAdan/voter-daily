"""
Voter Analytics Pipeline DAG
===========================

Daily processing pipeline for Good Party voter analytics:
1. Extract and load CSV voter data to DuckDB
2. Run dbt transformations (staging and mart layers)  
3. Launch Streamlit dashboard for analytics

Author: Chris
Project: Good Party Voter Analytics Platform
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import sys
import os
from pathlib import Path

# Add include/scripts to path for imports
scripts_path = '/usr/local/airflow/include/scripts'
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# DAG configuration
default_args = {
    'owner': 'goodparty-analytics',
    'depends_on_past': False,
    'start_date': datetime.now(timezone.utc) - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'voter_analytics_pipeline',
    default_args=default_args,
    description='Daily voter data processing and analytics pipeline',
    schedule=timedelta(days=1),  # Daily schedule
    catchup=False,
    tags=['goodparty', 'voter-analytics', 'daily', 'dbt', 'streamlit'],
    max_active_runs=1,
)


def extract_load_voter_data(**context):
    """
    Python callable to run the voter data extraction and loading process
    """
    logging.info("🗳️  Starting voter data extraction and loading process...")
    
    try:
        # Import with absolute path handling
        sys.path.insert(0, '/usr/local/airflow/include/scripts')
        from extract_load import VoteDataProcessor
        
        # Initialize processor with production target
        processor = VoteDataProcessor(target="prod")
        
        # Run the ETL pipeline
        logging.info("📥 Running Extract, Transform, Load pipeline...")
        result = processor.run_pipeline()
        
        if result['success']:
            logging.info(f"✅ ETL Pipeline completed successfully!")
            logging.info(f"📊 Records processed: {result.get('records_processed', 0)}")
            logging.info(f"📈 New records added: {result.get('new_records', 0)}")
            logging.info(f"👥 Total unique voters: {result.get('voter_count', 0)}")
            logging.info(f"🗺️  States represented: {result.get('state_count', 0)}")
            logging.info(f"⏱️  Processing time: {result.get('processing_time', 'N/A')}")
            
            # Push results to XCom for downstream tasks
            context['task_instance'].xcom_push(key='etl_results', value=result)
            
        else:
            error_msg = f"❌ ETL Pipeline failed: {result.get('error', 'Unknown error')}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except ImportError as e:
        error_msg = f"❌ Failed to import VoteDataProcessor: {str(e)}"
        logging.error(error_msg)
        logging.error(f"Python path: {sys.path}")
        logging.error(f"Scripts directory exists: {os.path.exists('/usr/local/airflow/include/scripts')}")
        if os.path.exists('/usr/local/airflow/include/scripts'):
            logging.error(f"Files in scripts dir: {os.listdir('/usr/local/airflow/include/scripts')}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"❌ Unexpected error in ETL process: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)





# Task 1: Extract and Load voter data
extract_load_task = PythonOperator(
    task_id='extract_load_data',
    python_callable=extract_load_voter_data,
    dag=dag,
)

# Task 2: Install dbt dependencies  
dbt_deps_task = BashOperator(
    task_id='dbt_deps',
    bash_command="""
    # Move to a completely clean working directory in /tmp
    mkdir -p /tmp/dbt_work
    cp -r /usr/local/airflow/include/vote_dbt/* /tmp/dbt_work/ 2>/dev/null || true
    cd /tmp/dbt_work
    
    # Ensure we have write permissions
    chmod -R 755 . 2>/dev/null || true
    
    # Remove any existing dbt_packages 
    rm -rf dbt_packages 2>/dev/null || true
    
    echo "📦 Installing dbt dependencies..." &&
    echo "Working directory: $(pwd)" &&
    echo "DBT_PROFILES_DIR: $DBT_PROFILES_DIR" &&
    
    # Create clean profiles.yml in /tmp location
    mkdir -p /tmp/dbt_profiles
    cat > /tmp/dbt_profiles/profiles.yml << 'EOF'
vote_dbt:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: /tmp/goodparty_prod.duckdb
      schema: raw
      threads: 4
EOF
    
    echo "Profile contents:" &&
    cat /tmp/dbt_profiles/profiles.yml &&
    
    # Run dbt deps with /tmp profile location
    export DBT_LOG_PATH=/tmp/dbt_logs &&
    dbt deps --profiles-dir /tmp/dbt_profiles &&
    
    # Copy successful results back to original location
    cp -r dbt_packages /usr/local/airflow/include/vote_dbt/ 2>/dev/null || true &&
    
    echo "✅ dbt dependencies installed successfully!"
    """,
    env={
        'DBT_PROFILES_DIR': '/tmp/dbt_profiles',
        'DBT_LOG_PATH': '/tmp/dbt_logs', 
        'PATH': '/usr/local/bin:/usr/bin:/bin'
    },
    dag=dag,
)

# Task 3: Compile dbt models
dbt_compile_task = BashOperator(
    task_id='dbt_compile',
    bash_command="""
    # Work from /tmp directory to avoid permission issues
    cd /tmp/dbt_work &&
    echo "🔧 Compiling dbt models..." &&
    echo "Working directory: $(pwd)" &&
    ls -la . &&
    export DBT_LOG_PATH=/tmp/dbt_logs &&
    dbt compile --profiles-dir /tmp/dbt_profiles &&
    echo "✅ dbt models compiled successfully!"
    """,
    env={
        'DBT_PROFILES_DIR': '/tmp/dbt_profiles',
        'DBT_LOG_PATH': '/tmp/dbt_logs',
        'PATH': '/usr/local/bin:/usr/bin:/bin'
    },
    dag=dag,
)

# Task 4: Build dbt models (run + test)
dbt_build_task = BashOperator(
    task_id='dbt_build',
    bash_command="""
    # Work from /tmp directory to avoid permission issues
    cd /tmp/dbt_work &&
    echo "🏗️  Building dbt models (run + test)..." &&
    echo "Working directory: $(pwd)" &&
    echo "Database path should be: /tmp/goodparty_prod.duckdb" &&
    ls -la /tmp/goodparty_prod.duckdb || echo "Database file not found" &&
    export DBT_LOG_PATH=/tmp/dbt_logs &&
    dbt build --profiles-dir /tmp/dbt_profiles &&
    echo "✅ dbt build completed successfully!" &&
    echo "📊 Analytics tables ready in DuckDB"
    """,
    env={
        'DBT_PROFILES_DIR': '/tmp/dbt_profiles',
        'DBT_LOG_PATH': '/tmp/dbt_logs',
        'PATH': '/usr/local/bin:/usr/bin:/bin'
    },
    dag=dag,
)

# Define task dependencies
extract_load_task >> dbt_deps_task >> dbt_compile_task >> dbt_build_task