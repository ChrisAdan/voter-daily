#!/usr/bin/env python3
"""
GoodParty Data Pipeline - Main Entry Point
========================================

This script serves as the main entry point for the GoodParty voter data processing pipeline.
It reads CSV files from the raw data directory and processes them into a DuckDB database
for further analysis and reporting.

Author: Chris
Project: GoodParty Voter Analytics Platform
"""

import sys
from datetime import datetime
from pathlib import Path

# Add the scripts directory to Python path
sys.path.append(str(Path(__file__).parent))

from extract_load import VoteDataProcessor


def print_header():
    """Print project header and introduction prior to pipeline execution"""
    print("=" * 80)
    print("🗳️  GOODPARTY VOTER DATA PIPELINE")
    print("=" * 80)
    print()
    print("📊 Data Engineering Pipeline for Voter Analytics")
    print("🏢 Company: GoodParty")
    print("📅 Date:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print()
    print("📋 Pipeline Overview:")
    print("   • Reads voter registration CSV files from include/data/raw/")
    print("   • Processes data with proper schema validation")
    print("   • Loads data into DuckDB (raw.vote_records table)")
    print("   • Ensures data quality and prevents duplicates")
    print("   • Supports both append and truncate mode")
    print("   • Orchestration in Airflow via Astro environment")
    print()
    print("=" * 80)
    print()



def main():
    """Main execution function"""
    try:
        # Print header and introduction
        print_header()    
        
        # Initialize the data processor
        processor = VoteDataProcessor()
        
        # Run the ETL pipeline
        print("📥 Starting Extract, Validate, and Load process...")
        result = processor.run_pipeline()
        
        if result['success']:
            print(f"\n✅ Pipeline completed successfully!")
            print(f"📊 Records processed: {result.get('records_processed', 0)}")
            print(f"📈 New records added: {result.get('new_records', 0)}")
            print(f"👥 Total unique voters: {result.get('voter_count', 0)}")
            print(f"🗺️  States represented: {result.get('state_count', 0)}")
            print(f"⏱️  Processing time: {result.get('processing_time', 'N/A')}")
        else:
            print(f"\n❌ Pipeline failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()