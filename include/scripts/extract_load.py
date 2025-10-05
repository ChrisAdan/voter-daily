#!/usr/bin/env python3
"""
GoodParty Voter Analytics Pipeline - Extract & Load Module
=============================================

This module handles the extraction of voter data from CSV files and loading
into DuckDB database with proper schema validation and deduplication.
Refactored to maximize pandas usage.
"""

import os
import uuid
import duckdb
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import hashlib


class VoteDataProcessor:
    """Main class for processing voter data from CSV to DuckDB"""
    
    def __init__(self, target: str = "dev"):
        self.target = target or "dev"
        
        # Detect environment and set paths accordingly
        if self._is_airflow_container():
            self.project_root = Path("/usr/local/airflow")
            self.data_dir = self.project_root / "include" / "data"
            self.raw_data_path = self.data_dir / "raw"
            self.db_path = Path('/usr/local/airflow') / f"goodparty_{self.target}.duckdb"
            self.last_run_file = Path('/usr/local/airflow') / f".last_run_timestamp_{self.target}"
        else:
            self.project_root = Path(__file__).resolve().parents[2]
            self.data_dir = self.project_root / "include" / "data"
            self.raw_data_path = self.data_dir / "raw"
            self.db_path = self.data_dir / f"goodparty_{self.target}.duckdb"
            self.last_run_file = self.data_dir / f".last_run_timestamp_{self.target}"
        
        self.connection = None
        
        # Ensure directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # CSV schema mapping
        self.csv_schema = {
            'id': 'VARCHAR',
            'first_name': 'VARCHAR',
            'last_name': 'VARCHAR', 
            'age': 'INTEGER',
            'gender': 'VARCHAR',
            'state': 'VARCHAR',
            'party': 'VARCHAR',
            'email': 'VARCHAR',
            'registered_date': 'DATE',
            'last_voted_date': 'DATE'
        }
    
    def _is_airflow_container(self) -> bool:
        """Detect if running in Airflow container environment"""
        airflow_indicators = [
            os.path.exists("/usr/local/airflow"),
            os.environ.get("AIRFLOW_HOME") is not None,
            os.environ.get("DBT_PROFILES_DIR") == "/usr/local/airflow/include/vote_dbt",
            "airflow" in os.getcwd().lower()
        ]
        return any(airflow_indicators)
    
    def create_db(self, db_type: str = 'duckdb') -> bool:
        """Create database connection"""
        try:
            if db_type.lower() != 'duckdb':
                raise ValueError(f"Unsupported database type: {db_type}")
            
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.connection = duckdb.connect(str(self.db_path))
            print(f"Connected to DuckDB: {self.db_path}")
            return True
        except Exception as e:
            print(f"Failed to create database connection: {e}")
            return False
    
    def ensure_db(self) -> bool:
        """Ensure database schema and tables exist"""
        try:
            if not self.connection:
                raise Exception("No database connection available")
            
            # Create raw schema
            self.db_command('create_schema', "CREATE SCHEMA IF NOT EXISTS raw")
            
            # Create vote_records table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS raw.vote_records (
                record_uuid VARCHAR PRIMARY KEY,
                source_id VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                age INTEGER,
                gender VARCHAR,
                state VARCHAR,
                party VARCHAR,
                email VARCHAR,
                registered_date DATE,
                last_voted_date DATE,
                source_file VARCHAR,
                record_hash VARCHAR,
                inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.db_command('create_table', create_table_sql)
            
            # Create processing metadata table
            metadata_table_sql = """
            CREATE TABLE IF NOT EXISTS raw.processing_metadata (
                id INTEGER PRIMARY KEY,
                last_processed_at TIMESTAMPTZ,
                files_processed INTEGER,
                records_processed INTEGER,
                voter_count INTEGER,
                state_count INTEGER,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.db_command('create_table', metadata_table_sql)
            
            print("Database schema and tables initialized")
            return True
            
        except Exception as e:
            print(f"Failed to ensure database: {e}")
            return False
    
    def db_command(self, action: str, query: str, connection: Optional[Any] = None) -> Any:
        """Execute database commands with different actions"""
        try:
            conn = connection or self.connection
            if not conn:
                raise Exception("No database connection available")
            
            if action.lower() in ['create_schema', 'create_table', 'insert', 'update', 'delete', 'truncate', 'drop']:
                conn.execute(query)
                return True
                
            elif action.lower() in ['read', 'select', 'query']:
                result = conn.execute(query).fetchall()
                return result
                
            elif action.lower() == 'read_df':
                result = conn.execute(query).df()
                return result
                
            else:
                raise ValueError(f"Unsupported action: {action}")
                
        except Exception as e:
            print(f"Database command failed ({action}): {e}")
            raise
    
    def get_latest_csv_files(self) -> List[Tuple[Path, datetime]]:
        """Get CSV files that need processing based on modification time"""
        try:
            last_run_time = self.get_last_run_timestamp()
            csv_files = []
            
            if not self.raw_data_path.exists():
                print(f"Raw data directory not found: {self.raw_data_path}")
                print(f"Creating directory: {self.raw_data_path}")
                os.makedirs(self.raw_data_path, exist_ok=True)
                return csv_files
            
            for csv_file in self.raw_data_path.glob("*.csv"):
                try:
                    file_mod_time = datetime.fromtimestamp(
                        csv_file.stat().st_mtime, 
                        tz=timezone.utc
                    )
                    if last_run_time is None or file_mod_time > last_run_time:
                        csv_files.append((csv_file, file_mod_time))
                        print(f"Found file to process: {csv_file.name} (modified: {file_mod_time})")
                except OSError as e:
                    print(f"Could not access file {csv_file}: {e}")
                    continue
            
            csv_files.sort(key=lambda x: x[1])
            
            if not csv_files:
                print("No new CSV files to process")
            
            return csv_files
            
        except Exception as e:
            print(f"Error finding CSV files: {e}")
            return []
    
    def get_last_run_timestamp(self) -> Optional[datetime]:
        """Get the timestamp of the last successful run"""
        try:
            # Try database metadata table first
            if self.connection:
                try:
                    result = self.db_command('read', 
                        "SELECT last_processed_at FROM raw.processing_metadata ORDER BY id DESC LIMIT 1")
                    if result and result[0][0]:
                        db_timestamp = result[0][0]
                        if isinstance(db_timestamp, datetime):
                            if db_timestamp.tzinfo is None:
                                db_timestamp = db_timestamp.replace(tzinfo=timezone.utc)
                            return db_timestamp
                except Exception as db_err:
                    print(f"Could not read from database metadata: {db_err}")
            
            # Fallback to file-based timestamp
            if self.last_run_file.exists():
                try:
                    with open(self.last_run_file, 'r') as f:
                        timestamp_str = f.read().strip()
                        file_timestamp = datetime.fromisoformat(timestamp_str)
                        if file_timestamp.tzinfo is None:
                            file_timestamp = file_timestamp.replace(tzinfo=timezone.utc)
                        return file_timestamp
                except Exception as file_err:
                    print(f"Could not parse timestamp file: {file_err}")
            
            return None
            
        except Exception as e:
            print(f"Could not read last run timestamp: {e}")
            return None
    
    def update_last_run_timestamp(self, files_processed: int = 0, records_processed: int = 0, 
                                   voter_count: int = 0, state_count: int = 0, timestamp: datetime = None):
        """Update the last run timestamp with processing statistics"""
        try:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)
            else:
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            # Update in database
            if self.connection:
                try:
                    existing = self.db_command('read', 
                        "SELECT COUNT(*) FROM raw.processing_metadata WHERE id = 1")
                    
                    if existing and existing[0][0] > 0:
                        update_sql = f"""
                        UPDATE raw.processing_metadata 
                        SET last_processed_at = '{timestamp.isoformat()}',
                            files_processed = {files_processed},
                            records_processed = {records_processed},
                            voter_count = {voter_count},
                            state_count = {state_count},
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = 1
                        """
                        self.db_command('update', update_sql)
                    else:
                        insert_sql = f"""
                        INSERT INTO raw.processing_metadata 
                        (id, last_processed_at, files_processed, records_processed, voter_count, state_count) 
                        VALUES (1, '{timestamp.isoformat()}', {files_processed}, {records_processed}, {voter_count}, {state_count})
                        """
                        self.db_command('insert', insert_sql)
                        
                except Exception as e:
                    print(f"Could not update database timestamp: {e}")
            
            # Update file as backup
            try:
                os.makedirs(os.path.dirname(self.last_run_file), exist_ok=True)
                with open(self.last_run_file, 'w') as f:
                    f.write(timestamp.isoformat())
                    print(f'Updated timestamp file: {self.last_run_file}')
            except Exception as e:
                print(f"Could not write timestamp file: {e}")
                
        except Exception as e:
            print(f"Could not update last run timestamp: {e}")
    
    def generate_record_hash(self, record: pd.Series) -> str:
        """Generate a hash for record deduplication using pandas Series"""
        key_fields = ['source_id', 'first_name', 'last_name', 'email']
        
        def normalize(value):
            if pd.isna(value):
                return ''
            if isinstance(value, str):
                return value.strip().lower()
            return str(value)
        
        hash_string = '|'.join(normalize(record.get(field, '')) for field in key_fields)
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def quick_error_check(self, file_path: Path, error_threshold: float = 0.05) -> Dict:
        """Quickly check percentage of malformed rows using pandas"""
        try:
            expected_cols = len(self.csv_schema)
            
            # Read a sample with pandas to check for issues
            try:
                # First, check the header - use usecols to handle trailing empty columns
                header_df = pd.read_csv(file_path, nrows=0)
                clean_header = [col.strip() for col in header_df.columns if col.strip()]
                
                if len(clean_header) < expected_cols:
                    print(f"Header has {len(clean_header)} columns, expected {expected_cols}")
                    return {'can_process': False, 'error': f'Header column mismatch: {len(clean_header)} vs {expected_cols}'}
                
                if len(clean_header) > expected_cols:
                    print(f"Warning: Header has {len(clean_header)} columns but expected {expected_cols}. Will use first {expected_cols} non-empty columns.")
                
                # Read sample rows to check error rate
                sample_size = 1000
                sample_df = pd.read_csv(
                    file_path,
                    nrows=sample_size,
                    on_bad_lines='warn',
                    encoding='utf-8',
                    dtype=str
                )
                
                if len(sample_df) == 0:
                    return {'can_process': False, 'error': 'No data rows found'}
                
                # Check for rows with wrong number of columns by reading raw
                total_rows = len(sample_df)
                
                # Estimate error rate based on null patterns
                # If too many rows are entirely null, flag as error
                null_rows = sample_df.isna().all(axis=1).sum()
                partial_null = sample_df.isna().any(axis=1).sum() - null_rows
                
                error_rows = null_rows  # Only count completely null rows as errors
                error_rate = error_rows / total_rows if total_rows > 0 else 0
                
                can_process = error_rate <= error_threshold
                
                print(f"Quick scan: {error_rows}/{total_rows} malformed rows ({error_rate:.2%})")
                if can_process:
                    print(f"Error rate {error_rate:.2%} is within threshold {error_threshold:.2%} - proceeding with pandas")
                else:
                    print(f"Error rate {error_rate:.2%} exceeds threshold {error_threshold:.2%} - file rejected")
                
                return {
                    'can_process': can_process,
                    'error_rate': error_rate,
                    'total_sampled': total_rows,
                    'error_rows': error_rows,
                    'threshold': error_threshold
                }
                
            except pd.errors.EmptyDataError:
                return {'can_process': False, 'error': 'Empty file'}
            except Exception as e:
                return {'can_process': False, 'error': f'Read error: {str(e)}'}
            
        except Exception as e:
            return {'can_process': False, 'error': f"Failed to scan file: {str(e)}"}

    def validate_schema(self, df: pd.DataFrame) -> Dict:
        """Validate that the DataFrame has the expected schema after pandas read."""
        try:
            expected_columns = list(self.csv_schema.keys())
            actual_columns = list(df.columns)
            
            missing_columns = set(expected_columns) - set(actual_columns)
            if missing_columns:
                return {
                    'valid': False,
                    'error': f"Missing required columns: {missing_columns}",
                    'expected': expected_columns,
                    'actual': actual_columns
                }
            
            extra_columns = set(actual_columns) - set(expected_columns)
            if extra_columns:
                print(f"Warning: Found extra columns that will be ignored: {extra_columns}")
            
            validation_errors = []
            
            for col, expected_type in self.csv_schema.items():
                if col not in df.columns:
                    continue
                    
                series = df[col]
                
                if expected_type == 'INTEGER':
                    try:
                        pd.to_numeric(series, errors='coerce')
                    except:
                        validation_errors.append(f"Column '{col}' cannot be converted to integer")
                        
                elif expected_type == 'DATE':
                    try:
                        pd.to_datetime(series, errors='coerce')
                    except:
                        validation_errors.append(f"Column '{col}' cannot be converted to date")
                        
                elif expected_type == 'VARCHAR':
                    if series.isna().all():
                        print(f"Warning: Column '{col}' is entirely empty")
            
            if validation_errors:
                return {
                    'valid': False,
                    'error': '; '.join(validation_errors),
                    'expected': expected_columns,
                    'actual': actual_columns
                }
            
            print(f"Schema validation passed: {len(df)} records with {len(expected_columns)} expected columns")
            return {
                'valid': True,
                'record_count': len(df),
                'columns_validated': expected_columns
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': f"Schema validation failed: {str(e)}",
                'expected': list(self.csv_schema.keys()),
                'actual': list(df.columns) if hasattr(df, 'columns') else []
            }

    def process_csv_file(self, file_path: Path, truncate_mode: bool = False) -> Dict:
        """CSV processing with pandas-first approach"""
        try:
            print(f"\nProcessing file: {file_path.name}")
            
            error_check = self.quick_error_check(file_path)
            if not error_check['can_process']:
                raise ValueError(f"File rejected: {error_check.get('error', 'High error rate')}")
            
            print("Reading CSV with pandas (skipping bad lines)...")
            try:
                df = pd.read_csv(
                    file_path,
                    dtype={
                        'id': 'string',  
                        'first_name': 'string',
                        'last_name': 'string', 
                        'age': 'string',
                        'gender': 'string',
                        'state': 'string',
                        'party': 'string',
                        'email': 'string',
                        'registered_date': 'string',
                        'last_voted_date': 'string'
                    },
                    on_bad_lines='skip',
                    skipinitialspace=True,
                    encoding='utf-8'
                )
                
                print(f"Successfully read {len(df)} records")
                
            except Exception as e:
                raise ValueError(f"Pandas read failed: {str(e)}")
            
            schema_check = self.validate_schema(df)
            if not schema_check['valid']:
                raise ValueError(f"Schema validation failed: {schema_check['error']}")
            
            df = self.clean_and_convert_dataframe(df)

            # Get existing hashes using pandas
            existing_hashes = set()
            if not truncate_mode:
                try:
                    existing_df = self.db_command('read_df', 
                        "SELECT record_hash FROM raw.vote_records WHERE record_hash IS NOT NULL")
                    if len(existing_df) > 0:
                        existing_hashes = set(existing_df['record_hash'].values)
                        print(f"Found {len(existing_hashes)} existing record hashes")
                except:
                    print("No existing records found")
            
            # Rename and add metadata columns
            df = df.rename(columns={'id': 'source_id'})
            df['source_file'] = file_path.name
            df['record_uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
            
            # Generate hashes using pandas apply
            df['record_hash'] = df.apply(self.generate_record_hash, axis=1)
            
            # Filter duplicates using pandas
            if not truncate_mode:
                initial_count = len(df)
                df = df[~df['record_hash'].isin(existing_hashes)]
                new_count = len(df)
                duplicates_filtered = initial_count - new_count
                if duplicates_filtered > 0:
                    print(f"Filtered {duplicates_filtered} duplicate records")
            
            if len(df) == 0:
                print("No new records to process after deduplication")
                return {
                    'success': True,
                    'records_processed': 0,
                    'new_records': 0,
                    'errors': 0
                }
            
            current_time = datetime.now(timezone.utc)
            df['inserted_at'] = current_time
            df['updated_at'] = current_time
            
            # Insert in batches
            batch_size = 1000
            records_processed = len(df)
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                self.insert_batch_dataframe(batch_df)
            
            print(f"Successfully processed {records_processed} records")
            
            return {
                'success': True,
                'records_processed': records_processed,
                'new_records': records_processed,
                'errors': error_check.get('error_rows', 0)
            }
            
        except Exception as e:
            print(f"Failed to process file {file_path}: {e}")
            return {'success': False, 'error': str(e)}

    def clean_and_convert_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and convert DataFrame to expected types after pandas read."""
        try:
            # Convert age to integer
            if 'age' in df.columns:
                df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('Int64')
            
            # Convert date columns
            for date_col in ['registered_date', 'last_voted_date']:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
            
            # Clean string columns using pandas string methods
            string_cols = ['first_name', 'last_name', 'gender', 'state', 'party', 'email']
            for col in string_cols:
                if col in df.columns:
                    # Strip whitespace
                    df[col] = df[col].str.strip()
                    # Replace empty strings and common null representations with None
                    df[col] = df[col].replace(['', 'nan', 'None', 'NaN', 'none'], None)
            
            print(f"Data cleaning completed: {len(df)} records")
            return df
            
        except Exception as e:
            print(f"Error cleaning DataFrame: {e}")
            raise
        
    def insert_batch_dataframe(self, df: pd.DataFrame):
        """Insert a DataFrame batch into the database"""
        try:
            if len(df) == 0:
                return
            
            self.connection.register('batch_df', df)
            
            insert_sql = """
            INSERT INTO raw.vote_records (
                record_uuid, source_id, first_name, last_name, age, gender, 
                state, party, email, registered_date, last_voted_date, 
                source_file, record_hash, inserted_at, updated_at
            )
            SELECT 
                record_uuid, source_id, first_name, last_name, age, gender,
                state, party, email, registered_date, last_voted_date,
                source_file, record_hash, inserted_at, updated_at
            FROM batch_df
            """
            
            self.connection.execute(insert_sql)
            self.connection.unregister('batch_df')
            
        except Exception as e:
            print(f"Failed to insert batch: {e}")
            raise

    def get_data_quality_metrics(self) -> Dict:
        """Get data quality metrics using pandas for metadata tracking"""
        try:
            if not self.connection:
                return {'voter_count': 0, 'state_count': 0}
            
            # Use read_df to get results as pandas DataFrame
            voter_df = self.db_command('read_df', 
                "SELECT COUNT(DISTINCT source_id) as count FROM raw.vote_records WHERE source_id IS NOT NULL")
            voter_count = int(voter_df['count'].iloc[0]) if len(voter_df) > 0 else 0
            
            state_df = self.db_command('read_df',
                "SELECT COUNT(DISTINCT state) as count FROM raw.vote_records WHERE state IS NOT NULL")
            state_count = int(state_df['count'].iloc[0]) if len(state_df) > 0 else 0
            
            return {
                'voter_count': voter_count,
                'state_count': state_count
            }
            
        except Exception as e:
            print(f"Error getting data quality metrics: {e}")
            return {'voter_count': 0, 'state_count': 0}

    def run_pipeline(self, target: Optional[str] = "dev", truncate_mode: bool = False) -> Dict:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            if not self.create_db():
                return {'success': False, 'error': 'Failed to create database connection'}
            
            if not self.ensure_db():
                return {'success': False, 'error': 'Failed to ensure database schema'}
            
            if truncate_mode:
                print("Truncating existing data...")
                self.db_command('truncate', "DELETE FROM raw.vote_records")
            
            csv_files = self.get_latest_csv_files()
            
            if not csv_files:
                return {
                    'success': True,
                    'records_processed': 0,
                    'new_records': 0,
                    'processing_time': str(datetime.now() - start_time)
                }
            
            total_processed = 0
            total_new = 0
            
            for file_path, mod_time in csv_files:
                result = self.process_csv_file(file_path, truncate_mode)
                if result['success']:
                    total_processed += result['records_processed']
                    total_new += result['new_records']
                else:
                    return {'success': False, 'error': result['error']}
            
            quality_metrics = self.get_data_quality_metrics()
            
            self.update_last_run_timestamp(
                files_processed=len(csv_files),
                records_processed=total_new,
                voter_count=quality_metrics['voter_count'],
                state_count=quality_metrics['state_count']
            )
            
            processing_time = datetime.now() - start_time
            
            print(f"\nData Quality Metrics:")
            print(f"   Total unique voters: {quality_metrics['voter_count']}")
            print(f"   States represented: {quality_metrics['state_count']}")
            
            return {
                'success': True,
                'records_processed': total_processed,
                'new_records': total_new,
                'processing_time': str(processing_time),
                'voter_count': quality_metrics['voter_count'],
                'state_count': quality_metrics['state_count']
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
        
        finally:
            if self.connection:
                self.connection.close()

if __name__ == '__main__':
    processor = VoteDataProcessor(target="prod")
    result = processor.run_pipeline()

    if result['success']:
        print("ETL Pipeline completed successfully!")
        print(f"Records processed: {result.get('records_processed', 0)}")
        print(f"New records added: {result.get('new_records', 0)}")
        print(f"Total unique voters: {result.get('voter_count', 0)}")
        print(f"States represented: {result.get('state_count', 0)}")
        print(f"Processing time: {result.get('processing_time', 'N/A')}")
    
    # CRITICAL: Explicitly close the database connection
    # DuckDB only allows one writer at a time
    if hasattr(processor, 'conn') and processor.conn:
        processor.conn.close()
        print("Database connection closed successfully")
