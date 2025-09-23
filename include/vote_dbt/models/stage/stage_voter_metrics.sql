-- models/stage/stage_voter_metrics.sql
-- Prepares voter-level records for aggregation by state, age group, and party 
-- Derived from dim_voter with additional segmentation for elections-since-last-vote, 
-- registration tenure, and engagement categorization
{{ config(
    materialized='incremental',
    unique_key='voter_id',
    on_schema_change='fail',
    schema='stage'
) }}

with base as (
select
    source_id as voter_id,
    age,
    gender,
    state,
    party,
    registered_date,
    last_voted_date,
    {{simple_elections_since_last_vote('last_voted_date')}} as elections_since_last_vote,

    -- Flag for presidential vs midterm last participation
    case 
        when last_voted_date is null then null
        when extract(year from last_voted_date) % 4 = 0 then 'Presidential Year'
        when extract(year from last_voted_date) % 2 = 0 then 'Midterm Year'
        else 'Off-Year/Primary'
    end as last_vote_cycle_type,
    
    -- Add audit fields for incremental processing
    inserted_date,
    last_modified_date

from {{ref('dim_voter')}}
where age >= 18  -- Only eligible voters
  
{% if is_incremental() %}
  -- Only process new or updated records
  and (inserted_date > (select max(last_processed_date) from {{ this }})
       or last_modified_date > (select max(last_processed_date) from {{ this }}))
{% endif %}
),

labelled as (
    select
        voter_id,
        -- Age bracket grouping based on Pew Research political demographics
        -- https://www.pewresearch.org/politics/fact-sheet/party-affiliation-fact-sheet-npors/
        case
            when age between 18 and 29 then '18-29'
            when age between 30 and 49 then '30-49'
            when age between 50 and 64 then '50-64'
            when age >= 65 then '65+'
        end as age_group,
        gender,
        state,
        party,
        registered_date,
        datediff('day', registered_date, current_date())::integer as days_since_registered,
        datediff('year', registered_date, current_date())::integer as years_since_registered,
        last_voted_date,
        elections_since_last_vote,

        -- Voter engagement segmentation based on federal election participation
        case 
            when elections_since_last_vote = 0 then 'Current Voter'
            when elections_since_last_vote = 1 then 'Missed Last Election'
            when elections_since_last_vote between 2 and 3 then 'Occasional Voter'
            when elections_since_last_vote between 4 and 6 then 'Infrequent Voter'
            when elections_since_last_vote >= 7 then 'Dormant Voter'
            when elections_since_last_vote is null then 'Never Voted'
        end as voter_engagement_segment,
        
        last_vote_cycle_type,
        
        -- Audit timestamp for incremental processing
        now() as last_processed_date
        
    from base
)

select * from labelled