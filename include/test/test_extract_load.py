# include/test/test_extract_load.py
import sys
from pathlib import Path

# Add project root (include/) to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import pandas as pd
import pytest
from datetime import datetime, timezone

from extract_load import VoteDataProcessor


@pytest.fixture
def processor(tmp_path):
    """Fixture to create a processor with temp paths."""
    db_path = tmp_path / "test.duckdb"
    raw_path = tmp_path / "raw"
    raw_path.mkdir()
    last_run_file = tmp_path / ".last_run_timestamp"

    proc = VoteDataProcessor(db_path=str(db_path))
    proc.raw_data_path = raw_path
    proc.last_run_file = last_run_file
    yield proc
    if proc.connection:
        proc.connection.close()


def make_csv(path: Path, rows: list[dict]):
    """Helper to write a CSV with the right schema."""
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


def test_create_and_ensure_db(processor):
    assert processor.create_db()
    assert processor.ensure_db()

    # Verify tables exist
    tables = processor.db_command("read",
        "SELECT table_name FROM information_schema.tables WHERE table_schema='raw'")
    table_names = {t[0] for t in tables}
    assert "vote_records" in table_names
    assert "processing_metadata" in table_names


def test_generate_record_hash_consistency(processor):
    record = {
        "source_id": "1",
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "a@example.com",
        "registered_date": "2020-01-01"
    }
    h1 = processor.generate_record_hash(record)
    h2 = processor.generate_record_hash(record)
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 32


def test_quick_error_check_valid_and_invalid(tmp_path, processor):
    good_csv = tmp_path / "good.csv"
    bad_csv = tmp_path / "bad.csv"

    # Valid file
    make_csv(
        good_csv,
        [{
            "id": "1", "first_name": "Bob", "last_name": "Lee", "age": 30,
            "gender": "M", "state": "CA", "party": "I", "email": "b@x.com",
            "registered_date": "2020-01-01", "last_voted_date": "2022-01-01"
        }]
    )
    result = processor.quick_error_check(good_csv)
    assert result["can_process"] is True

    # Invalid header count
    with open(bad_csv, "w") as f:
        f.write("id,first_name\n1,Bob\n")
    result = processor.quick_error_check(bad_csv)
    assert "can_process" in result


def test_validate_schema_success_and_failure(processor):
    df = pd.DataFrame([{
        "id": "1", "first_name": "Test", "last_name": "User", "age": 25,
        "gender": "F", "state": "TX", "party": "D", "email": "t@u.com",
        "registered_date": "2020-01-01", "last_voted_date": "2021-01-01"
    }])
    result = processor.validate_schema(df)
    assert result["valid"]

    # Missing required column
    df_bad = df.drop(columns=["email"])
    result = processor.validate_schema(df_bad)
    assert result["valid"] is False
    assert "email" in result["error"]


def test_process_csv_file_and_dedup(processor):
    processor.create_db()
    processor.ensure_db()

    file1 = processor.raw_data_path / "voters1.csv"
    rows = [
        {
            "id": "1", "first_name": "Sam", "last_name": "One", "age": 40,
            "gender": "M", "state": "WA", "party": "R", "email": "s1@x.com",
            "registered_date": "2019-01-01", "last_voted_date": "2021-01-01"
        }
    ]
    make_csv(file1, rows)

    result = processor.process_csv_file(file1)
    assert result["success"]
    assert result["records_processed"] == 1

    # Reprocess same file → should deduplicate
    result2 = processor.process_csv_file(file1)
    assert result2["records_processed"] == 0


def test_update_and_get_last_run_timestamp(processor):
    processor.create_db()
    processor.ensure_db()

    now = datetime.now(timezone.utc)
    processor.update_last_run_timestamp(files_processed=1, records_processed=5,
                                        voter_count=3, state_count=2, timestamp=now)
    ts = processor.get_last_run_timestamp()
    assert isinstance(ts, datetime)
    assert ts.replace(microsecond=0) == now.replace(microsecond=0)


def test_run_pipeline_end_to_end(processor):
    # Make a valid CSV
    file1 = processor.raw_data_path / "voters.csv"
    make_csv(file1, [{
        "id": "42", "first_name": "Eve", "last_name": "Zed", "age": 50,
        "gender": "F", "state": "NY", "party": "G", "email": "e@z.com",
        "registered_date": "2018-01-01", "last_voted_date": "2020-01-01"
    }])

    result = processor.run_pipeline()
    assert result["success"]
    assert result["new_records"] == 1
    assert "voter_count" in result and result["voter_count"] >= 1
