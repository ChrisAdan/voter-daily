#!/usr/bin/env python3
"""
Election Calendar Seeder - Separate Historic and Upcoming Seeds
================================================================

Data Strategy:
    Historic (1976-2020): MIT Election Lab provides actual election results
    with vote totals and winners for analytical modeling. Output to
    election_calendar_historic.csv with analytics fields.
    
    Upcoming (2021+): Combines Google Civic API sparse data with calculated
    federal schedule. Output to election_calendar_upcoming.csv with only
    relevant descriptive fields - no null analytics padding.
    
    Separation Rationale: Historic and upcoming have fundamentally different
    schemas. Historic contains results (votes, winners) while upcoming contains
    only scheduled dates. Forcing unified schema creates noise with null fields.
    Separate seeds allow independent evolution and clearer dbt model semantics.
    Join on shared fields (date, type, level) when unified view needed.

Dependencies: pandas, requests, python-dotenv (optional)
"""

import os
import sys
import uuid
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from calendar import monthcalendar
from typing import Optional


def get_google_api_key() -> Optional[str]:
    """
    Retrieve Google Civic API key from environment.
    
    Rationale: Optional dependency allows script to function with calculated
    federal schedule alone. Google Civic API provides minimal upcoming data
    but is free and adds real-world validation where available.
    """
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass
    
    return os.getenv('GOOGLE_CIVIC_API_KEY')


def download_mit_historic_data() -> Optional[pd.DataFrame]:
    """
    Download MIT Election Lab presidential results (1976-2020).
    
    Rationale: MIT data is gold standard for historic analysis - peer-reviewed,
    includes vote totals and winners. Direct CSV download from Harvard Dataverse
    avoids API complexity. Temporary file approach handles large datasets without
    memory issues.
    """
    print("📥 Downloading historic data from MIT Election Lab...")
    
    MIT_URL = "https://dataverse.harvard.edu/api/access/datafile/4299753"
    
    try:
        response = requests.get(MIT_URL, timeout=30)
        response.raise_for_status()
        
        from io import BytesIO
        df = pd.read_csv(
            BytesIO(response.content),
            sep='\t',  # Tab-separated, not comma
            on_bad_lines='skip',
            encoding='utf-8',
            quoting=3  # QUOTE_NONE - don't interpret quotes
        )
        # Strip triple quotes from all string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip('"""')
        df.to_csv('raw_elections.csv', index=False)
        print(f"✅ Downloaded {len(df)} historic election records")
        return df
        
    except Exception as e:
        print(f"❌ Failed to download MIT data: {e}")
        return None


def fetch_google_civic_elections(api_key: str) -> pd.DataFrame:
    """
    Fetch upcoming elections from Google Civic API.
    
    Rationale: Google Civic returns minimal data (~4 elections) but is free
    and provides real-world upcoming elections. Return empty DataFrame on failure
    to maintain pandas-centric approach and simplify downstream processing.
    """
    if not api_key:
        return pd.DataFrame()
    
    print("📥 Fetching from Google Civic Information API...")
    
    BASE_URL = "https://www.googleapis.com/civicinfo/v2/elections"
    
    try:
        response = requests.get(BASE_URL, params={'key': api_key}, timeout=10)
        response.raise_for_status()
        data = response.json()
        elections = data.get('elections', [])
        
        print(f"✅ Retrieved {len(elections)} elections from Google Civic API")
        if len(elections) < 10:
            print("   ⚠️  Google Civic API has limited upcoming data")
        
        return pd.DataFrame(elections) if elections else pd.DataFrame()
        
    except Exception as e:
        print(f"⚠️  Google Civic API failed: {e}")
        return pd.DataFrame()


def calculate_election_date(year: int, month: int = 11) -> str:
    """
    Calculate Election Day per Constitutional requirement.
    
    Rationale: U.S. federal elections occur on first Tuesday after first Monday
    in November (even years). This is Constitutionally mandated, making calculation
    more reliable than sparse API data for future federal elections.
    
    Implementation: Uses calendar module to find first Monday, adds 1 day for Tuesday.
    Handles edge case where first week doesn't contain Monday (month starts on Tuesday+).
    """
    cal = monthcalendar(year, month)
    # If first week has no Monday (0), use second week
    first_week = cal[0] if cal[0][0] != 0 else cal[1]
    first_monday = first_week[0] if first_week[0] != 0 else cal[1][0]
    election_day = first_monday + 1
    return f"{year}-{month:02d}-{election_day:02d}"


def generate_federal_calendar(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Generate calculated federal election schedule.
    
    Rationale: Federal elections follow predictable schedule (even years only,
    Presidential every 4th year). APIs have limited future coverage, but
    Constitutional requirements make calculation reliable. Includes both
    general elections and major primaries (Super Tuesday) for completeness.
    
    Trade-off: Sacrifices primary date precision for comprehensive coverage.
    Super Tuesday varies by state but clustering around early March is sufficient
    for voter engagement modeling.
    """
    elections = []
    
    for year in range(start_year, end_year + 1, 2):  # Even years only
        election_date = calculate_election_date(year)
        is_presidential = (year % 4 == 0)
        
        # General election
        elections.append({
            'election_date': election_date,
            'election_year': year,
            'election_name': f'{year} Federal General Election',
            'election_type': 'Presidential' if is_presidential else 'Midterm',
            'election_level': 'Federal',
            'office_type': 'President' if is_presidential else 'Congress',
            'is_primary': False,
            'state': None,
            'data_source': 'Calculated Federal Schedule',
        })
        
        # Presidential primaries (Super Tuesday approximation)
        if is_presidential:
            super_tuesday = calculate_election_date(year, month=3)
            elections.append({
                'election_date': super_tuesday,
                'election_year': year,
                'election_name': f'{year} Super Tuesday Presidential Primary',
                'election_type': 'Primary',
                'election_level': 'Federal',
                'office_type': 'President',
                'is_primary': True,
                'state': 'Multiple',
                'data_source': 'Calculated Federal Schedule',
            })
    
    return pd.DataFrame(elections)


def transform_mit_to_historic_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MIT data to historic election schema with analytics fields.
    
    Rationale: MIT data is election-result level (multiple rows per election).
    Aggregate to election level for calendar use while preserving analytical
    value (vote totals, winners). Group by year since MIT data is presidential
    only and one election per year is guaranteed.
    
    Schema: Historic-specific fields include vote totals, winner information
    for retrospective analysis.
    """
    elections = []
    
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        election_date = calculate_election_date(year)
        
        # Aggregate vote data
        total_votes = int(year_data['totalvotes'].sum()) if 'totalvotes' in year_data.columns else None
        
        # Extract winner (candidate with most votes)
        if 'candidate' in year_data.columns and 'candidatevotes' in year_data.columns:
            winner_row = year_data.loc[year_data['candidatevotes'].idxmax()]
            winner_name = winner_row.get('candidate')
            winner_party = winner_row.get('party_simplified', winner_row.get('party_detailed'))
            winner_votes = int(winner_row.get('candidatevotes', 0))
        else:
            winner_name = None
            winner_party = None
            winner_votes = None
        
        elections.append({
            # Shared fields
            'election_date': election_date,
            'election_year': year,
            'election_name': f'{year} Presidential Election',
            'election_type': 'Presidential',
            'election_level': 'Federal',
            'office_type': 'President',
            'is_primary': False,
            'state': None,
            'data_source': 'MIT Election Lab',
            
            # Historic-specific analytics fields
            'total_votes_cast': total_votes,
            'winner_name': winner_name,
            'winner_party': winner_party,
            'winner_votes': winner_votes,
        })
    
    return pd.DataFrame(elections)


def transform_google_civic_to_upcoming_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Google Civic API data to upcoming election schema.
    
    Rationale: Google Civic returns minimal structure (id, name, electionDay).
    Extract classification from text patterns in name field since API doesn't
    provide structured type/level fields. Defensive extraction handles varying
    name formats across jurisdictions.
    
    Schema: Upcoming-only fields, no null padding for historic analytics.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Rename and extract basic fields
    df = df.copy()
    df['election_date'] = df['electionDay'].str.replace('T00:00:00Z', '')
    df['election_name'] = df['name']
    
    # Extract year from date
    df['election_year'] = pd.to_datetime(df['election_date']).dt.year
    
    # Classify elections from name patterns
    df['election_type'] = df.apply(lambda row: classify_election_type(row['election_name'], row['election_year']), axis=1)
    df['election_level'] = df['election_name'].apply(classify_election_level)
    df['office_type'] = df['election_name'].apply(extract_office_type)
    df['is_primary'] = df['election_name'].str.lower().str.contains('primary')
    df['state'] = df['election_name'].apply(extract_state)
    df['data_source'] = 'Google Civic Information API'
    
    # Select only upcoming-relevant columns (no null analytics fields)
    cols = ['election_date', 'election_year', 'election_name', 'election_type',
            'election_level', 'office_type', 'is_primary', 'state', 'data_source']
    
    return df[cols]


def classify_election_type(name: str, year: int) -> str:
    """
    Infer election type from name and year.
    
    Rationale: Primary indicator is name keyword, fallback to year-based federal
    cycle classification. Prioritizes explicit markers (primary/special) over
    inference to avoid misclassification.
    """
    name_lower = name.lower()
    if 'primary' in name_lower:
        return 'Primary'
    elif 'special' in name_lower:
        return 'Special'
    elif year % 4 == 0:
        return 'Presidential'
    elif year % 2 == 0:
        return 'Midterm'
    else:
        return 'Off-Year'


def classify_election_level(name: str) -> str:
    """
    Infer election level from name keywords.
    
    Rationale: Hierarchical keyword matching (federal > state > local) prevents
    ambiguous classifications. Falls back to 'General' rather than guessing.
    """
    name_lower = name.lower()
    if any(word in name_lower for word in ['federal', 'presidential', 'senate', 'congress']):
        return 'Federal'
    elif any(word in name_lower for word in ['state', 'governor', 'legislature']):
        return 'State'
    elif any(word in name_lower for word in ['county', 'city', 'local', 'municipal']):
        return 'Local'
    else:
        return 'General'


def extract_office_type(name: str) -> Optional[str]:
    """Extract office type from election name via keyword matching."""
    name_lower = name.lower()
    if 'president' in name_lower:
        return 'President'
    elif 'senate' in name_lower:
        return 'Senate'
    elif 'house' in name_lower or 'congress' in name_lower:
        return 'House'
    elif 'governor' in name_lower:
        return 'Governor'
    else:
        return 'General'


def extract_state(name: str) -> Optional[str]:
    """
    Extract state code from election name.
    
    Rationale: Simple word-level matching against known state codes. Doesn't
    handle full state names (e.g., "California") to avoid false positives
    from words like "Washington" in "Washington County, Oregon".
    """
    words = name.split()
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
              'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
              'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
              'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
              'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    
    for word in words:
        if word.upper() in states:
            return word.upper()
    return None


def deduplicate_elections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate elections based on natural key.
    
    Rationale: Multiple sources may report same election. Natural key of
    (date, type, level, state) identifies unique elections. Keep first
    occurrence to preserve source priority (Google > Calculated).
    """
    return df.drop_duplicates(
        subset=['election_date', 'election_type', 'election_level', 'state'],
        keep='first'
    )


def finalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add metadata and standardize types for election data.
    
    Rationale: UUID primary keys avoid collisions. UTC timestamps provide
    timezone consistency. Nullable Int64 preserves NULLs in numeric data.
    
    Universal schema includes all possible fields. The existence check 
    (if col in df.columns) naturally handles schema differences between
    historic (with analytics) and upcoming (without) datasets without
    requiring conditional logic.
    """
    # Generate UUID primary keys
    df['election_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # Add UTC timestamp
    current_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')
    df['last_updated'] = current_utc
    
    # Ensure date format
    df['election_date'] = pd.to_datetime(df['election_date']).dt.strftime('%Y-%m-%d')
    
    # Universal schema (includes all possible fields)
    type_spec = {
        # Primary key
        'election_id': 'string',
        
        # Shared temporal and descriptive fields
        'election_date': 'string',
        'election_year': 'Int64',
        'election_name': 'string',
        'election_type': 'string',
        'election_level': 'string',
        'office_type': 'string',
        'is_primary': 'boolean',
        'state': 'string',
        
        # Historic-only analytics fields (present only in historic df)
        'total_votes_cast': 'Int64',
        'winner_name': 'string',
        'winner_party': 'string',
        'winner_votes': 'Int64',
        
        # Metadata
        'data_source': 'string',
        'last_updated': 'string',
    }
    
    # Apply types only to columns that exist in the DataFrame
    for col, dtype in type_spec.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    # Return columns in spec order, filtering to only those present
    return df[[col for col in type_spec.keys() if col in df.columns]]


def save_to_csv(df: pd.DataFrame, output_path: Path) -> bool:
    """Save DataFrame to CSV using pandas."""
    if df.empty:
        print(f"⚠️  No data to save for {output_path.name}")
        return False
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(df)} elections to {output_path}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save {output_path.name}: {e}")
        return False


def main():
    """
    Execute hybrid election calendar generation with separate outputs.
    
    Process:
        1. Download MIT historic results → election_calendar_historic.csv
        2. Fetch Google Civic + generate federal schedule → election_calendar_upcoming.csv
        3. Each seed has optimized schema for its use case
    """
    print("=" * 80)
    print("🗳️  DUAL-SEED ELECTION CALENDAR GENERATOR")
    print("=" * 80)
    print()
    print("📊 Output Strategy:")
    print("   • election_calendar_historic.csv - Analytics fields (votes, winners)")
    print("   • election_calendar_upcoming.csv - Descriptive fields only")
    print()
    
    script_dir = Path(__file__).parent
    seeds_dir = script_dir.parent / "vote_dbt" / "seeds"
    
    # ===== HISTORIC SEED =====
    print("=" * 80)
    print("HISTORIC ELECTIONS (1976-2020)")
    print("=" * 80)
    
    mit_df = download_mit_historic_data()
    if mit_df is not None:
        mit_df['insertion_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')
        print("Writing raw MIT data to elections_historic_raw_mit.csv")
        raw_path = seeds_dir / "elections_historic_raw_mit.csv"
        save_to_csv(mit_df, raw_path)
        print("\n🔄 Transforming MIT data to historic schema...")
        historic_df = transform_mit_to_historic_schema(mit_df)
        historic_df = finalize_schema(historic_df)
        historic_df = historic_df.sort_values('election_date').reset_index(drop=True)
        
        historic_path = seeds_dir / "election_outcome_federal_historic.csv"
        save_to_csv(historic_df, historic_path)
        
        print(f"\n📊 Historic Summary:")
        print(f"   • Total raw election records: {len(mit_df)}")
        print(f"   • Total elections: {len(historic_df)}")
        print(f"   • Date range: {historic_df['election_date'].min()} to {historic_df['election_date'].max()}")
        print(f"   • Columns: {', '.join(historic_df.columns.tolist())}")
    
    # ===== UPCOMING SEED =====
    print("\n" + "=" * 80)
    print("UPCOMING ELECTIONS (2021+)")
    print("=" * 80)
    
    upcoming_dfs = []
    
    # Google Civic API
    api_key = get_google_api_key()
    if api_key:
        civic_df = fetch_google_civic_elections(api_key)
        if not civic_df.empty:
            print("\n🔄 Transforming Google Civic data to upcoming schema...")
            civic_transformed = transform_google_civic_to_upcoming_schema(civic_df)
            upcoming_dfs.append(civic_transformed)
            print(f"   ✅ Added {len(civic_transformed)} from Google Civic API")
    else:
        print("\n⚠️  No Google Civic API key - using calculated federal schedule only")
    
    # Calculated federal schedule
    current_year = datetime.now().year
    start_year = min(2022, current_year) if current_year % 2 == 0 else min(2022, current_year - 1)
    
    print(f"\n📅 Generating federal election schedule ({start_year}-2030)...")
    federal_df = generate_federal_calendar(start_year, 2030)
    upcoming_dfs.append(federal_df)
    print(f"   ✅ Added {len(federal_df)} calculated federal elections")
    
    # Combine and deduplicate
    print("\n🔄 Combining upcoming sources...")
    upcoming_df = pd.concat(upcoming_dfs, ignore_index=True)
    
    print("🔄 Removing duplicates...")
    initial_count = len(upcoming_df)
    upcoming_df = deduplicate_elections(upcoming_df)
    deduped_count = initial_count - len(upcoming_df)
    if deduped_count > 0:
        print(f"   Removed {deduped_count} duplicates")
    
    upcoming_df = finalize_schema(upcoming_df)
    upcoming_df = upcoming_df.sort_values('election_date').reset_index(drop=True)
    
    upcoming_path = seeds_dir / "election_calendar_upcoming.csv"
    save_to_csv(upcoming_df, upcoming_path)
    
    print(f"\n📊 Upcoming Summary:")
    print(f"   • Total elections: {len(upcoming_df)}")
    print(f"   • Date range: {upcoming_df['election_date'].min()} to {upcoming_df['election_date'].max()}")
    print(f"   • Columns: {', '.join(upcoming_df.columns.tolist())}")
    
    by_source = upcoming_df['data_source'].value_counts()
    print("\n   Breakdown by source:")
    for source, count in by_source.items():
        print(f"      • {source}: {count}")
    
    # ===== FINAL SUMMARY =====
    print("\n" + "=" * 80)
    print("✅ SUCCESS - TWO SEED FILES CREATED")
    print("=" * 80)
    print("\n🎯 Next steps:")
    print("  1. cd include/vote_dbt")
    print("  2. dbt seed") 
    print("  3. Query raw MIT election results: SELECT * FROM {{ ref('elections_historic_raw_mit') }}")
    print("  3. Query historic: SELECT * FROM {{ ref('election_outcome_federal_historic') }}")
    print("  4. Query upcoming: SELECT * FROM {{ ref('election_calendar_upcoming') }}")
    print("  5. Join when needed:")
    print("     SELECT election_id, election_date, ... FROM {{ ref('election_outcome_federal_historic') }}")
    print("     UNION ALL")
    print("     SELECT election_id, election_date, ... FROM {{ ref('election_calendar_upcoming') }}")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())