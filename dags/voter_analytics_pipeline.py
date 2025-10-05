"""
Voter Analytics Pipeline DAG
===========================

Daily processing pipeline for Good Party voter analytics using Cosmos:
1. Extract and load CSV voter data to DuckDB
2. Run dbt transformations via Cosmos (staging and mart layers)
3. Generate documentation

Leverages persistent workspace in /tmp/dbt_workspace created by dbt_workspace_setup DAG.
Cosmos handles all dbt operations using DuckDBUserPasswordProfileMapping.

Author: Chris
Project: Good Party Voter Analytics Platform
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

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
    description='Daily voter data processing and analytics pipeline with Cosmos',
    schedule=timedelta(days=1),  # Daily schedule
    catchup=False,
    tags=['goodparty', 'voter-analytics', 'daily', 'dbt', 'cosmos'],
    max_active_runs=1,
)

# Cosmos Configuration
# Reference /usr/local/airflow/dbt_workspace - created at Docker build time
# profiles.yml is in config/ subdirectory (won't interfere with local dev)
workspace_path = Path("/usr/local/airflow/dbt_workspace")

project_config = ProjectConfig(
    dbt_project_path=workspace_path,
)

# Point to profiles.yml in config/ subdirectory
profile_config = ProfileConfig(
    profile_name="vote_dbt",
    target_name="prod",
    profiles_yml_filepath=workspace_path / "config" / "profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

render_config = RenderConfig(
    enable_mock_profile=False,
    dbt_deps=False,  # Dependencies installed at build time
)

# CRITICAL: DuckDB only allows one writer at a time
# Force sequential execution at the DAG level
dag.max_active_tasks = 1


def check_workspace_setup(**context):
    """
    Check that the workspace setup file exists.
    This verifies that dbt_workspace_setup DAG has completed successfully.
    """
    workspace_dir = '/usr/local/airflow/dbt_workspace'
    marker_file = f'{workspace_dir}/.setup_complete'
    
    if not os.path.exists(marker_file):
        error_msg = (
            f"Workspace setup marker not found: {marker_file}\n"
            "Please run the dbt_workspace_setup DAG first to initialize the database."
        )
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    logging.info("Workspace setup marker found")
    
    # Read and log setup details
    with open(marker_file, 'r') as f:
        setup_info = f.read()
        logging.info(f"Setup info:\n{setup_info}")


def verify_workspace_ready(**context):
    """
    Verify that the workspace setup is complete before proceeding.
    """
    workspace_dir = '/usr/local/airflow/dbt_workspace'
    marker_file = f'{workspace_dir}/.setup_complete'
    db_path = '/usr/local/airflow/goodparty_prod.duckdb'
    dbt_project = f'{workspace_dir}/dbt_project.yml'
    
    # Check workspace directory
    if not os.path.exists(workspace_dir):
        error_msg = f"Workspace directory does not exist: {workspace_dir}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Check setup marker
    if not os.path.exists(marker_file):
        error_msg = "Database not initialized. Run dbt_workspace_setup DAG first."
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Check dbt_project.yml
    if not os.path.exists(dbt_project):
        error_msg = f"dbt_project.yml not found: {dbt_project}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Check database file
    if not os.path.exists(db_path):
        error_msg = f"Database file not found: {db_path}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info("Workspace verification successful")
    logging.info(f"Workspace: {workspace_dir}")
    logging.info(f"dbt project: {dbt_project}")
    logging.info(f"Database: {db_path}")
    logging.info("Cosmos will auto-generate profiles.yml using DuckDBUserPasswordProfileMapping")
    
    # Read and log setup details
    with open(marker_file, 'r') as f:
        setup_info = f.read()
        logging.info(f"Setup info:\n{setup_info}")


def extract_load_voter_data(**context):
    """
    Extract and load voter data to DuckDB.
    Uses the persistent DuckDB database at /usr/local/airflow/goodparty_prod.duckdb
    """
    logging.info("Starting voter data extraction and loading process...")
    
    try:
        # Import with absolute path handling
        sys.path.insert(0, '/usr/local/airflow/include/scripts')
        from extract_load import VoteDataProcessor
        
        # Initialize processor with production target
        processor = VoteDataProcessor(target="prod")
        
        # Run the ETL pipeline
        logging.info("Running Extract, Transform, Load pipeline...")
        result = processor.run_pipeline()
        
        if result['success']:
            logging.info("ETL Pipeline completed successfully!")
            logging.info(f"Records processed: {result.get('records_processed', 0)}")
            logging.info(f"New records added: {result.get('new_records', 0)}")
            logging.info(f"Total unique voters: {result.get('voter_count', 0)}")
            logging.info(f"States represented: {result.get('state_count', 0)}")
            logging.info(f"Processing time: {result.get('processing_time', 'N/A')}")
            
            # CRITICAL: Explicitly close the database connection
            # DuckDB only allows one writer at a time
            if hasattr(processor, 'conn') and processor.conn:
                processor.conn.close()
                logging.info("Database connection closed successfully")
            
            # Push results to XCom for downstream tasks
            context['task_instance'].xcom_push(key='etl_results', value=result)
            
        else:
            error_msg = f"ETL Pipeline failed: {result.get('error', 'Unknown error')}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except ImportError as e:
        error_msg = f"Failed to import VoteDataProcessor: {str(e)}"
        logging.error(error_msg)
        logging.error(f"Python path: {sys.path}")
        logging.error(f"Scripts directory exists: {os.path.exists('/usr/local/airflow/include/scripts')}")
        if os.path.exists('/usr/local/airflow/include/scripts'):
            logging.error(f"Files in scripts dir: {os.listdir('/usr/local/airflow/include/scripts')}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in ETL process: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    finally:
        # Ensure connection is closed even if there's an error
        try:
            if 'processor' in locals() and hasattr(processor, 'conn') and processor.conn:
                processor.conn.close()
                logging.info("Database connection closed in finally block")
        except Exception as close_error:
            logging.warning(f"Error closing connection: {close_error}")


# Task 1: Check workspace setup file exists
check_workspace_setup_task = PythonOperator(
    task_id='check_workspace_setup',
    python_callable=check_workspace_setup,
    dag=dag,
)

# Task 2: Verify workspace is ready
verify_workspace_task = PythonOperator(
    task_id='verify_workspace',
    python_callable=verify_workspace_ready,
    dag=dag,
)

# Task 3: Extract and Load voter data
extract_load_task = PythonOperator(
    task_id='extract_load_data',
    python_callable=extract_load_voter_data,
    dag=dag,
)

# Task 4: Run dbt transformations via Cosmos
# Always create the DbtTaskGroup - mock profile handles parse-time issues
dbt_transformation = DbtTaskGroup(
    group_id="dbt_transformation",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args={
        "install_deps": False,  # Dependencies installed at build time in Dockerfile
        "full_refresh": False,
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    dag=dag,
)

# Task 5: Pipeline complete marker
pipeline_complete = EmptyOperator(
    task_id='pipeline_complete',
    dag=dag,
)

# Define task dependencies
check_workspace_setup_task >> verify_workspace_task >> extract_load_task >> dbt_transformation >> pipeline_complete