"""
Election Calendar Seed DAG - Monthly
=====================================

Monthly refresh of upcoming election data using include/scripts/seed_elections.py.

Runs on the 1st of each month to capture new elections from Google Civic API
and update calculated federal schedule.

Leverages persistent workspace in /usr/local/airflow/dbt_workspace created by dbt_workspace_setup DAG.
Uses Astronomer Cosmos for dbt operations.

Dependencies: 
- seed_elections.py in include/scripts/
- dbt_workspace_setup DAG (for persistent environment)
- astronomer-cosmos package
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import duckdb
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from cosmos import DbtSeedLocalOperator, ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig

default_args = {
    'owner': 'goodparty-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'election_calendar_seed_monthly',
    default_args=default_args,
    description='Monthly refresh of upcoming election seed files with Cosmos',
    schedule='0 6 1 * *',  # 6 AM UTC on 1st of each month
    catchup=False,
    tags=['elections', 'seed', 'MIT', 'Google Civic API', 'monthly', 'cosmos'],
    max_active_runs=1,
)

# Cosmos Configuration - Match voter_analytics_pipeline pattern
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
    
    # Read and log setup details
    with open(marker_file, 'r') as f:
        setup_info = f.read()
        logging.info(f"Setup info:\n{setup_info}")


def generate_election_seeds(**context):
    """
    Generate election seed files directly to /usr/local/airflow/dbt_workspace/seeds directory.
    Runs the seed_elections.py script and validates output.
    """
    logging.info("Generating election calendar seed files...")
    
    workspace_dir = '/usr/local/airflow/dbt_workspace'
    seeds_dir = f'{workspace_dir}/seeds'
    scripts_dir = '/usr/local/airflow/include/scripts'
    
    # Ensure seeds directory exists
    os.makedirs(seeds_dir, exist_ok=True)
    logging.info(f"Seeds directory ready: {seeds_dir}")
    
    # Add scripts directory to path
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    
    try:
        # Set environment variable for seed output location
        os.environ['SEED_OUTPUT_DIR'] = seeds_dir
        
        # Import and run the seed generation script
        logging.info("Importing seed_elections module...")
        import seed_elections
        
        # Execute the seed generation
        logging.info("Running election seed generation...")
        # Assuming seed_elections has a main() or run() function
        # Adjust this based on the actual structure of seed_elections.py
        if hasattr(seed_elections, 'main'):
            seed_elections.main()
        elif hasattr(seed_elections, 'run'):
            seed_elections.run()
        else:
            # Fallback: execute the module directly
            import runpy
            runpy.run_module('seed_elections', run_name='__main__')
        
        # Verify seed files were created
        seed_files = [f for f in os.listdir(seeds_dir) if f.endswith('.csv')]
        
        if not seed_files:
            error_msg = f"No seed files generated in {seeds_dir}"
            logging.error(error_msg)
            raise Exception(error_msg)
        
        logging.info(f"Successfully generated {len(seed_files)} seed file(s):")
        seed_info = {}
        for seed_file in seed_files:
            file_path = os.path.join(seeds_dir, seed_file)
            file_size = os.path.getsize(file_path)
            logging.info(f"  - {seed_file} ({file_size:,} bytes)")
            seed_info[seed_file] = file_size
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='seed_files', value=seed_info)
        
    except ImportError as e:
        error_msg = f"Failed to import seed_elections: {str(e)}"
        logging.error(error_msg)
        logging.error(f"Python path: {sys.path}")
        logging.error(f"Scripts directory exists: {os.path.exists(scripts_dir)}")
        if os.path.exists(scripts_dir):
            logging.error(f"Files in scripts dir: {os.listdir(scripts_dir)}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error generating seed files: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)


def verify_seed_data(**context):
    """
    Verify that seed data was successfully loaded into DuckDB.
    Lists all tables in the database after seed operation.
    """
    logging.info("Verifying seed data in DuckDB...")
    
    db_path = '/usr/local/airflow/goodparty_prod.duckdb'
    
    if not os.path.exists(db_path):
        error_msg = f"Database file not found: {db_path}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    conn = None
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get all tables across all schemas
        all_tables_query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            ORDER BY table_schema, table_name
        """
        all_tables = conn.execute(all_tables_query).fetchall()
        
        logging.info(f"All tables in database ({len(all_tables)}):")
        for schema, table in all_tables:
            logging.info(f"  - {schema}.{table}")
        
        logging.info("Seed data verification complete")
        
    except Exception as e:
        error_msg = f"Error verifying database: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    finally:
        # CRITICAL: Always close DuckDB connection
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed successfully")
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

# Task 3: Generate election seed files
generate_seeds_task = PythonOperator(
    task_id='generate_election_seeds',
    python_callable=generate_election_seeds,
    dag=dag,
)

# Task 4: Load seeds into DuckDB using Cosmos DbtSeedLocalOperator
dbt_seed_task = DbtSeedLocalOperator(
    task_id='dbt_seed_elections',
    profile_config=profile_config,
    project_dir=workspace_path,
    install_deps=False,  # Dependencies installed at build time
    full_refresh=True,  # Full refresh for seed data
    dag=dag,
)

# Task 5: Verify seed data loaded correctly
verify_seeds_task = PythonOperator(
    task_id='verify_seed_data',
    python_callable=verify_seed_data,
    dag=dag,
)

# Task 6: Pipeline complete marker
pipeline_complete = EmptyOperator(
    task_id='pipeline_complete',
    dag=dag,
)

# Define task dependencies
(
    check_workspace_setup_task
    >> verify_workspace_task 
    >> generate_seeds_task 
    >> dbt_seed_task 
    >> verify_seeds_task
    >> pipeline_complete
)