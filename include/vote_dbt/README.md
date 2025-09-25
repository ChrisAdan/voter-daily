<<<<<<< HEAD
# Good Party Voter Analytics Pipeline

A dbt-powered data transformation pipeline for voter registration and engagement analysis, designed to support Good Party's mission of promoting independent and non-partisan candidates.

## 🎯 Project Overview

This pipeline transforms raw voter registration data into analytics-ready tables that support:

- **Partisan trend analysis** across demographics and election cycles
- **Voter engagement segmentation** and re-engagement targeting
- **Geographic and demographic insights** for strategic planning
- **Data quality monitoring** and audit capabilities

## 📊 Data Architecture

### Raw Layer (`raw` schema)

- **`vote_records`**: Raw voter registration records from CSV ingestion

### Staging Layer (`stage` schema)

- **`stage_voter_metrics`**: Cleaned voter records with engagement segmentation and derived metrics

### Dimension Layer (`dim` schema)

- **`dim_voter`**: Core voter dimension with demographics and registration history

### Mart Layer (`mart` schema) - Analytics Ready Tables

#### 🏛️ `mart_voter_snapshot`

Current period aggregated voter statistics by demographics.

```sql
-- Example: Get current voter engagement by state and party
select state, party, pct_current_voters, pct_targetable_voters
from mart_voter_snapshot
order by pct_targetable_voters desc;
```

#### 📈 `mart_partisan_trends`

Time series analysis of partisan composition across election cycles.

```sql
-- Example: Track Democratic voter participation trends
select election_year, election_type, state, participation_rate
from mart_partisan_trends
where party = 'Democrat' and state = 'PA'
order by election_year desc;
```

#### 🎯 `mart_targeting_opportunities`

Ranked demographic segments with highest voter re-engagement potential.

```sql
-- Example: Find top targeting opportunities nationwide
select state, age_group, gender, party, opportunity_score, prime_target_voters
from mart_targeting_opportunities
where targeting_tier = 'High Priority'
order by opportunity_score desc limit 20;
```

#### 🗺️ `mart_state_summary`

Executive-level state summaries with competitive analysis.

```sql
-- Example: Identify competitive states with high engagement opportunities
select state, partisan_lean, engagement_opportunity_score, total_registered_voters
from mart_state_summary
where partisan_lean in ('Highly Competitive', 'Competitive')
order by engagement_opportunity_score desc;
=======
# Voter Analytics Pipeline | dbt Reference

A production-ready dbt data transformation pipeline that converts raw voter registration data into analytics-ready insights supporting the mission to strengthen democracy through independent and non-partisan candidates.

## 🏗️ Architecture Overview

This pipeline implements a modern **Medallion Architecture** with four distinct layers, each serving specific data quality and analytical purposes:

```
Raw Sources → Dimension Tables → Staging Layer → Mart Layer
     ↓              ↓              ↓           ↓
  CSV Files    Cleaned Dims    Enriched     Analytics
   (Bronze)      (Silver)     Metrics        Ready
                                           (Gold)
```

### Layer Responsibilities

**Raw Layer (`raw` schema)**

- Direct CSV ingestion landing zone
- Minimal processing, preserves source data integrity
- Audit fields for data lineage and quality monitoring
- `vote_records`: Raw voter registration records with full history

**Dimension Layer (`dim` schema)**

- Core business entities with data quality enforcement
- Deduplication, standardization, and cleansing logic
- Primary keys and referential integrity constraints
- `dim_voter`: Authoritative voter dimension with demographics

**Staging Layer (`stage` schema)**

- Enriched, analytics-ready individual records
- Derived metrics and behavioral segmentation
- Incremental processing for performance optimization
- `stage_voter_metrics`: Voter-level metrics with engagement scoring

**Mart Layer (`mart` schema)**

- Aggregated, business-ready analytical tables
- Optimized for dashboard consumption and reporting
- Cross-tabulated insights across multiple dimensions
- Four core marts supporting distinct analytical use cases

## 📊 Data Model Details

![dbt Lineage](docs/lineage.png)

### Core Data Flow

```sql
-- Raw ingestion preserves source fidelity
raw.vote_records (CSV → DuckDB)
    ↓
-- Dimension layer ensures data quality
dim_voter (deduped, validated, standardized)
    ↓
-- Staging enriches with analytics fields
stage_voter_metrics (segmentation, tenure, engagement)
    ↓
-- Marts aggregate for business consumption
mart.* (voter_snapshot, partisan_trends, targeting_opportunities, state_summary)
```

### Key Design Decisions

**Incremental Processing**

- `stage_voter_metrics` uses incremental materialization with `voter_id` as unique key
- Processes only new/modified records based on audit timestamps
- Balances freshness with computational efficiency

**Behavioral Segmentation**

- Standardized voter engagement categories based on federal election participation
- Hardcoded election calendar via `simple_elections_since_last_vote` macro
- Current Voter (0 missed) → Never Voted (null history)

**Demographic Standardization**

- Age groups aligned with Pew Research political demographic brackets
- Party affiliation simplified to three major categories for analytical clarity
- State-level geographic analysis using standard two-letter codes

## 🎯 Mart Layer Analytics

### `mart_voter_snapshot`

**Purpose**: Current-state demographic cross-tabs for executive dashboards  
**Grain**: State × Age Group × Gender × Party × Engagement Segment  
**Key Metrics**: Voter counts, engagement percentages, tenure statistics  
**Use Case**: "Show me current voter composition and engagement by demographics"

### `mart_partisan_trends`

**Purpose**: Time series analysis of partisan composition across election cycles  
**Grain**: Election Year × Type × State × Demographics × Party  
**Key Metrics**: Eligible voters, participation rates, trend changes  
**Use Case**: "Track Democratic participation in Pennsylvania over presidential cycles"

### `mart_targeting_opportunities`

**Purpose**: Ranked segments for voter re-engagement campaigns  
**Grain**: State × Age Group × Gender × Party (with composite scoring)  
**Key Metrics**: Opportunity scores, targeting tiers, lapsed voter counts  
**Use Case**: "Identify top 20 demographic segments for GOTV outreach"

### `mart_state_summary`

**Purpose**: Executive-level geographic competitive analysis  
**Grain**: State-level aggregations with partisan lean classification  
**Key Metrics**: Total voters, party composition, competitive indicators  
**Use Case**: "Show me competitive states with high engagement opportunities"

## 🔧 Technical Implementation

### Data Quality Framework

- **dbt-expectations** package for advanced data quality testing
- **Contract enforcement** on all mart models for API stability
- **Referential integrity** testing between dimension and fact tables
- **Regex validation** for email formats and state codes
- **Range validation** for dates, percentages, and counts

### Performance Optimizations

- **Incremental processing** on high-volume staging tables
- **Appropriate materializations**: Views for staging, tables for marts
- **Efficient aggregations** using CTEs and window functions
- **Composite indexes** on frequently joined columns

### Macro Library

```sql
-- Centralized election calendar logic
{{ simple_elections_since_last_vote('last_voted_date') }}
-- Returns: Count of federal elections missed since last participation
-- Hardcoded: 2008-2024 federal election dates for initial implementation
>>>>>>> dev
```

## 🚀 Getting Started

### Prerequisites

<<<<<<< HEAD
- dbt Core 1.6+
- DuckDB adapter
- dbt-expectations package

### Setup

```bash
# Install dependencies
dbt deps

# Run the full pipeline
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Key Commands

```bash
# Run only mart tables
dbt run --select mart.*

# Test specific model
dbt test --select mart_targeting_opportunities

# Run with custom analysis date
dbt run --vars '{"analysis_date": "2024-01-01"}'
```

## 📋 Key Metrics & Definitions

### Voter Engagement Segments

- **Current Voter**: Participated in most recent election (0 missed)
- **Missed Last Election**: Skipped 1 recent election
- **Occasional Voter**: Missed 2-3 recent elections
- **Infrequent Voter**: Missed 4-6 recent elections
- **Dormant Voter**: Missed 7+ elections
- **Never Voted**: No voting history since registration

### Targeting Tiers

- **High Priority**: Opportunity score ≥25, segment size ≥100 voters
- **Medium Priority**: Opportunity score ≥15, segment size ≥50 voters
- **Low Priority**: Opportunity score ≥8, segment size ≥25 voters
- **Monitor Only**: Below priority thresholds

### Election Types

- **Presidential**: Every 4 years (2024, 2020, 2016...)
- **Midterm**: Even years between presidential (2022, 2018...)
- **Off-Year/Primary**: Odd years and primary elections

## 🎨 Dashboard Use Cases

### 1. Geographic Strategy Dashboard

Use `mart_state_summary` to identify:

- Competitive states needing attention
- States with high engagement opportunities
- Demographic composition by geography

### 2. Targeting Campaign Dashboard

Use `mart_targeting_opportunities` to:

- Prioritize outreach segments by opportunity score
- Size targeting campaigns by available voters
- Track campaign effectiveness over time

### 3. Partisan Trends Analysis

Use `mart_partisan_trends` to:

- Analyze voter participation shifts over time
- Compare presidential vs midterm engagement patterns
- Identify demographic groups with changing political preferences

### 4. Executive Summary Dashboard

Use `mart_voter_snapshot` for:

- Current period KPIs and voter composition
- High-level engagement metrics
- Snapshot comparisons over time

## 🔍 Data Quality & Testing

The pipeline includes comprehensive testing:

- **Uniqueness** constraints on voter IDs
- **Referential integrity** between dimension and fact tables
- **Accepted values** validation for categorical fields
- **Date logic** validation (registration before voting)
- **Regex validation** for email formats

## 🛠️ Customization

### Adding New Election Dates

Update the `election_cycles` CTE in `mart_partisan_trends.sql`:

```sql
select '2026-11-03'::date as election_date, 'Midterm' as election_type, 2026 as election_year
```

### Modifying Engagement Segments

Update the logic in `stage_voter_metrics.sql`:

```sql
case
    when elections_since_last_vote = 0 then 'Current Voter'
    when elections_since_last_vote = 1 then 'Missed Last Election'
    when elections_since_last_vote between 2 and 3 then 'Occasional Voter'
    when elections_since_last_vote between 4 and 6 then 'Infrequent Voter'
    when elections_since_last_vote >= 7 then 'Dormant Voter'
    else 'Never Voted'
end as voter_engagement_segment
```

### Adjusting Opportunity Scoring

Modify the scoring weights in `mart_targeting_opportunities.sql`:

```sql
-- Current weights: Recent (40%), Medium (30%), Tenure (20%), Size (10%)
round(
    (40.0 * lapsed_1_election / total_voters) +
    (30.0 * (lapsed_2_3_elections + lapsed_4_6_elections) / total_voters) +
    (20.0 * least(avg_registration_tenure / 10.0, 1.0)) +
    (10.0 * least(total_voters / 1000.0, 1.0))
, 2) as opportunity_score
=======
```bash
# Required dependencies
dbt-core>=1.6.0
dbt-duckdb>=1.5.0
dbt-expectations>=0.10.9
```

### Installation & Setup

```bash
# Clone and install dependencies
git clone <repository>
cd vote_dbt
dbt deps

# Run full pipeline
dbt run

# Execute data quality tests
dbt test

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

### Development Workflow

```bash
# Run specific layer
dbt run --select stage.*
dbt run --select mart.*

# Test individual models
dbt test --select mart_targeting_opportunities

# Historical analysis with custom date
dbt run --vars '{"analysis_date": "2023-01-01"}'
```

## 📈 Business Intelligence Integration

### Example Queries

**Executive KPI Dashboard**

```sql
-- State-level competitive landscape
SELECT
    state,
    total_registered_voters,
    partisan_lean,
    engagement_opportunity_score,
    pct_recoverable_voters
FROM mart_state_summary
WHERE partisan_lean IN ('Highly Competitive', 'Competitive')
ORDER BY engagement_opportunity_score DESC;
```

**GOTV Campaign Targeting**

```sql
-- Top voter re-engagement opportunities
SELECT
    state, age_group, party,
    opportunity_score,
    prime_target_voters,
    pct_recently_lapsed
FROM mart_targeting_opportunities
WHERE targeting_tier = 'High Priority'
ORDER BY opportunity_score DESC LIMIT 50;
```

**Trend Analysis**

```sql
-- Democratic participation trends in swing states
SELECT
    election_year,
    state,
    participation_rate,
    participation_rate_change_pts
FROM mart_partisan_trends
WHERE party = 'Democrat'
    AND state IN ('PA', 'MI', 'WI', 'AZ')
    AND election_type = 'Presidential'
ORDER BY election_year DESC, participation_rate_change_pts DESC;
>>>>>>> dev
```

## 🔮 Future Enhancements

<<<<<<< HEAD
### Phase 2: Enhanced Election Calendar

- **Dynamic election calendar**: Replace hardcoded dates with external election calendar API
- **State-specific elections**: Include gubernatorial, mayoral, and local elections
- **Primary elections**: Track primary participation patterns
- **Special elections**: Account for off-cycle elections

### Phase 3: Advanced Analytics

- **Predictive modeling**: ML models for voter turnout prediction
- **Cohort analysis**: Track voter behavior changes over time
- **Geographic clustering**: Identify similar voting districts for targeted strategies
- **Social network analysis**: Understand voter influence patterns

### Phase 4: Real-time Integration

- **Streaming updates**: Real-time voter registration updates
- **Campaign integration**: Connect with campaign management systems
- **API endpoints**: Expose analytics via REST API for applications
- **Automated alerts**: Notify on significant demographic shifts

## 📈 Performance Optimization

### Current Optimizations

- **Incremental models**: Use `{{ incremental_strategy }}` for large tables
- **Partitioning**: Tables partitioned by state and election year where beneficial
- **Indexing**: Key columns indexed for query performance
- **Materialization**: Balanced use of tables vs views based on usage patterns

### Scaling Considerations

```sql
-- Example incremental configuration for large datasets
{{ config(
    materialized='incremental',
    unique_key='voter_id',
    on_schema_change='fail',
    partition_by=['state', 'election_year']
) }}
```

## 🧪 Testing Strategy

### Unit Tests

- Individual model logic validation
- Edge case handling (null values, date boundaries)
- Data type consistency

### Integration Tests

- Cross-model referential integrity
- Aggregation accuracy between layers
- Historical trend consistency

### Data Quality Tests

```yaml
# Example custom test in schema.yml
tests:
  - dbt_expectations.expect_column_values_to_be_between:
      arguments:
        column_name: participation_rate
        min_value: 0
        max_value: 100
```

## 🔒 Privacy & Security

### Data Governance

- **PII handling**: Email addresses are the only PII; consider hashing for production
- **Data retention**: Implement retention policies per organizational requirements
- **Access control**: Role-based access to sensitive demographic data
- **Audit logging**: Track all data access and transformations

### Compliance Considerations

- **GDPR**: Right to deletion and data portability
- **CCPA**: California privacy requirements
- **Election law**: Compliance with voter privacy regulations
- **Data sharing**: Restrictions on political data usage

## 🤝 Contributing

### Development Workflow

1. **Feature branch**: Create from `main` for new features
2. **Model development**: Follow naming conventions (`stage_`, `dim_`, `mart_`)
3. **Testing**: Add appropriate tests in `schema.yml`
4. **Documentation**: Update model descriptions and README
5. **Code review**: Peer review before merging

### Coding Standards

- **SQL style**: Use consistent formatting and clear aliases
- **Model naming**: Descriptive names reflecting business purpose
- **Comments**: Explain complex business logic
- **Version control**: Commit frequently with descriptive messages

### Model Development Guidelines

```sql
-- Template for new mart models
{{ config(
    materialized='table',
    schema='mart',
    description='Brief model description'
) }}

with source_data as (
    select * from {{ ref('stage_model_name') }}
),

transformed as (
    -- Business logic transformations
    select
        dimension_columns,
        aggregated_metrics,
        calculated_fields
    from source_data
    group by dimension_columns
)

select * from transformed
order by primary_sort_column
```

## 📞 Support & Resources

### Documentation

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-expectations](https://github.com/calogica/dbt-expectations)
- [Good Party Data Guidelines](internal-link)

### Troubleshooting

- **Model failures**: Check `logs/dbt.log` for detailed error messages
- **Test failures**: Run `dbt test --select failing_model` for specifics
- **Performance issues**: Use `dbt run --profile` to identify bottlenecks

### Team Contacts

- **Data Engineering**: data-eng@goodparty.org
- **Analytics**: analytics@goodparty.org
- **Platform**: platform@goodparty.org

---

_This pipeline supports Good Party's mission to strengthen democracy through data-driven insights that help independent and non-partisan candidates compete effectively in elections._

```bash
Note: This README was autogenerated by Claude.ai
=======
### Phase 2: Enhanced Dimensional Model

- **State dimension table** with geographic, economic, and political context
- **Gender dimension expansion** to support non-binary and inclusive categories
- **Dynamic election calendar** replacing hardcoded dates with external API integration
- **Household clustering** using address standardization for family-level insights

### Phase 3: Advanced Analytics

- **Predictive voter turnout models** using historical patterns and external factors
- **Cohort analysis** tracking voter lifecycle transitions over time
- **Geographic clustering** identifying similar voting districts for targeted strategies
- **Campaign effectiveness tracking** measuring outreach ROI and engagement lift

### Phase 4: Real-time Operations

- **Streaming incremental updates** for near real-time voter registration changes
- **Change data capture** from upstream voter file systems
- **Automated data quality monitoring** with alert systems for anomaly detection
- **API layer** exposing mart tables for application consumption

### Phase 5: Operational Integration

- **Campaign management system integration** for seamless targeting workflows
- **Automated report generation** with email distribution for stakeholders
- **Interactive dashboard application** built on mart layer foundations
- **Multi-tenant architecture** supporting state and local Good Party chapters

## 🔒 Data Governance & Compliance

### Privacy Considerations

- **PII minimization**: Only email addresses stored; consider hashing for production
- **Data retention policies**: Implement automated archival of historical records
- **Access control**: Role-based permissions for sensitive demographic data
- **Audit logging**: Comprehensive tracking of all data access and transformations

### Compliance Framework

- **GDPR compliance**: Right to deletion and data portability requirements
- **CCPA compliance**: California privacy law requirements for voter data
- **Election law compliance**: Adherence to voter privacy and usage regulations
- **Data sharing restrictions**: Proper handling of political data usage limitations

## 🧪 Testing & Data Quality

### Testing Strategy

```yaml
# Comprehensive testing approach
Unit Tests: # Individual model logic validation
Integration Tests: # Cross-model referential integrity
Data Quality Tests: # dbt-expectations for advanced validation
Contract Tests: # Mart table schema enforcement
```

### Quality Metrics

- **Completeness**: Null value tracking across critical fields
- **Uniqueness**: Primary key constraint enforcement
- **Validity**: Range and format validation for all data types
- **Consistency**: Cross-table relationship validation
- **Timeliness**: Data freshness monitoring and SLA tracking

## 📞 Next Steps

### Immediate Priorities

1. **Snapshot implementation** for slowly changing dimensions (voter registration changes over time)
2. **Performance benchmarking** with larger datasets and optimization tuning
3. **Production deployment** with proper CI/CD pipeline and environment management

### Integration Development

1. **Dashboard application wrapper** for business user consumption of mart tables
2. **API layer development** exposing analytical insights for campaign tools
3. **Automated reporting system** with scheduled delivery of key insights

---

_This dbt pipeline provides the foundational data architecture supporting Good Party's mission to strengthen democracy through data-driven insights that help independent and non-partisan candidates compete effectively in elections._

```bash
Note: This README was generated automatically using Cline.ai
>>>>>>> dev
```
