"""
Environment Setup DAG
========================

One-time initialization that runs on Airflow startup to prepare the
DuckDB database for dbt operations.

What it does:
1. Checks if setup already complete (.setup_complete marker)
2. If not complete: Initializes DuckDB database with raw schema
3. If complete: Skips setup

Why:
- Workspace is created at Docker build time (/tmp/dbt_workspace)
- This DAG only handles runtime initialization (database, sample data)
- Runs once per container start, then other DAGs reuse the database

Dependencies: None (first DAG to run)
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
import logging

default_args = {
    'owner': 'goodparty-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dbt_workspace_setup',
    default_args=default_args,
    description='One-time DuckDB initialization for dbt workspace',
    schedule=None,  # Manual trigger only - runs once per container lifecycle
    catchup=False,
    tags=['setup', 'dbt', 'initialization', 'infrastructure'],
    max_active_runs=1,
)

SETUP_MARKER = '/usr/local/airflow/dbt_workspace/.setup_complete'


def check_if_setup_needed(**context):
    """
    Check if database setup has already been completed.
    Returns task_id to branch to based on marker file existence.
    """
    if os.path.exists(SETUP_MARKER):
        logging.info("Database already initialized - setup marker found")
        logging.info(f"   Marker: {SETUP_MARKER}")
        return 'skip_setup'
    else:
        logging.info("Database not initialized - proceeding with setup")
        return 'initialize_duckdb'


# Task 0: Branch based on setup status
check_setup_branch = BranchPythonOperator(
    task_id='check_setup_status',
    python_callable=check_if_setup_needed,
    dag=dag,
)

# Task 1a: Skip path (setup already done)
skip_setup_task = EmptyOperator(
    task_id='skip_setup',
    dag=dag,
)

# Task 1b: Initialize DuckDB database
def initialize_duckdb(**context):
    """
    Initialize DuckDB database with raw schema and proper permissions.
    """
    import duckdb
    
    db_path = '/usr/local/airflow/goodparty_prod.duckdb'
    
    logging.info("Initializing DuckDB database...")
    
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.close()
    
    # Set permissions for database file
    os.chmod(db_path, 0o666)
    
    logging.info(f"✓ DuckDB initialized at {db_path}")
    logging.info("  - Schema 'raw' created")
    logging.info(f"  - File permissions: {oct(os.stat(db_path).st_mode)[-3:]}")
    logging.info(f"  - File size: {os.path.getsize(db_path)} bytes")


init_duckdb_task = PythonOperator(
    task_id='initialize_duckdb',
    python_callable=initialize_duckdb,
    dag=dag,
)

# Task 2: Create sample voter data file
create_sample_data_task = BashOperator(
    task_id='create_sample_data',
    bash_command="""
    echo "Creating sample voter data file..."
    
    mkdir -p /usr/local/airflow/include/data/raw
    
    cat > /usr/local/airflow/include/data/raw/sample_voter_data.csv << 'EOF'
id,first_name,last_name,age,gender,state,party,email,registered_date,last_voted_date
1,John,Smith,45,M,CA,Democrat,john.smith@email.com,2010-03-15,2020-11-03
2,Sarah,Johnson,32,F,NY,Republican,sarah.j@email.com,2012-08-22,2022-11-08
3,Michael,Brown,28,M,TX,Independent,m.brown@email.com,2018-01-10,
4,Emily,Davis,67,F,FL,Democrat,emily.davis@email.com,2008-05-20,2024-11-05
5,Robert,Wilson,41,M,PA,Republican,rob.wilson@email.com,2015-09-12,2020-11-03
6,Lisa,Anderson,35,F,OH,Independent,lisa.a@email.com,2016-02-28,2018-11-06
7,David,Taylor,52,M,MI,Democrat,david.taylor@email.com,2009-07-18,2022-11-08
8,Jennifer,Thomas,29,F,WI,Republican,jen.thomas@email.com,2019-11-01,2022-11-08
9,James,Jackson,73,M,AZ,Democrat,j.jackson@email.com,2007-01-15,2020-11-03
10,Mary,White,38,F,NC,Independent,mary.white@email.com,2013-06-30,2016-11-08
EOF
    
    echo "✓ Sample data created at /usr/local/airflow/include/data/raw/sample_voter_data.csv"
    """,
    dag=dag,
)

# Task 3: Mark setup as complete
mark_complete_task = BashOperator(
    task_id='mark_setup_complete',
    bash_command="""
    echo "Writing setup completion marker..."
    
    TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
    
    cat > /usr/local/airflow/dbt_workspace/.setup_complete << EOF
dbt workspace setup completed successfully
Timestamp: ${TIMESTAMP}
Workspace: /usr/local/airflow/dbt_workspace (created at build time)
Database: /usr/local/airflow/goodparty_prod.duckdb (initialized at runtime)
Logs: /usr/local/airflow/dbt_logs
Profiles: /usr/local/airflow/dbt_workspace/config/profiles.yml
Dependencies: Installed at build time via dbt deps

Note: 
- Workspace copied at Docker build time from include/vote_dbt/
- dbt dependencies (dbt_packages/) installed at build time
- profiles.yml stored in config/ subdirectory to avoid local dev conflicts
- Database initialized at first runtime
EOF
    
    echo "Setup complete marker:"
    cat /usr/local/airflow/dbt_workspace/.setup_complete
    
    echo ""
    echo "================================================================================"
    echo "dbt DATABASE SETUP COMPLETE"
    echo "================================================================================"
    echo ""
    echo "✓ Workspace ready at: /usr/local/airflow/dbt_workspace (from build time)"
    echo "✓ Database ready at: /usr/local/airflow/goodparty_prod.duckdb"
    echo "✓ Profiles: config/profiles.yml"
    echo "✓ Dependencies: dbt_packages/ (installed at build time)"
    echo ""
    echo "All dbt operations use the container-specific profiles.yml."
    echo "Local development uses ~/.dbt/profiles.yml (no conflicts)."
    echo "================================================================================"
    """,
    dag=dag,
)

# Task 4: Join point for both paths
setup_complete = EmptyOperator(
    task_id='setup_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Define task dependencies
check_setup_branch >> [skip_setup_task, init_duckdb_task]

# Skip path
skip_setup_task >> setup_complete

# Setup path
init_duckdb_task >> create_sample_data_task >> mark_complete_task >> setup_complete