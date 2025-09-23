-- models/mart/voter_snapshot.sql
-- Current snapshot of voter demographics and engagement for latest analysis period
{{ config(
    materialized='table',
    schema='mart'
) }}

with voter_metrics as (
    select * from {{ ref('stage_voter_metrics') }}
),

snapshot_base as (
    select
        -- Demographic dimensions
        state::varchar(2) as state,
        age_group::varchar(10) as age_group,
        gender::varchar(1) as gender,
        party::varchar(12) as party,
        voter_engagement_segment::varchar(20) as voter_engagement_segment,
        last_vote_cycle_type::varchar(18) as last_vote_cycle_type,
        
        -- Voter counts
        count(distinct voter_id)::bigint as total_voters,
        
        -- Registration tenure metrics
        avg(years_since_registered)::double as avg_years_registered,
        median(years_since_registered)::double as median_years_registered,
        
        -- Engagement metrics
        avg(elections_since_last_vote)::double as avg_elections_missed,
        
        -- Engagement distribution
        sum(case when voter_engagement_segment = 'Current Voter' then 1 else 0 end)::bigint as current_voters,
        sum(case when voter_engagement_segment = 'Missed Last Election' then 1 else 0 end)::bigint as missed_last_voters,
        sum(case when voter_engagement_segment = 'Occasional Voter' then 1 else 0 end)::bigint as occasional_voters,
        sum(case when voter_engagement_segment = 'Infrequent Voter' then 1 else 0 end)::bigint as infrequent_voters,
        sum(case when voter_engagement_segment = 'Dormant Voter' then 1 else 0 end)::bigint as dormant_voters,
        sum(case when voter_engagement_segment = 'Never Voted' then 1 else 0 end)::bigint as never_voted,
        
        -- Targeting opportunity metrics
        sum(case when elections_since_last_vote >= 2 then 1 else 0 end)::bigint as targetable_voters,
        sum(case when elections_since_last_vote >= 4 then 1 else 0 end)::bigint as high_opportunity_voters
        
    from voter_metrics
    group by 1,2,3,4,5,6
),

final as (
    select
        state,
        age_group,
        gender,
        party,
        voter_engagement_segment,
        last_vote_cycle_type,
        total_voters,
        avg_years_registered,
        median_years_registered,
        avg_elections_missed,
        current_voters,
        missed_last_voters,
        occasional_voters,
        infrequent_voters,
        dormant_voters,
        never_voted,
        targetable_voters,
        high_opportunity_voters,
        
        -- Calculate percentages for targeting
        round(100.0 * current_voters / total_voters, 2)::double as pct_current_voters,
        round(100.0 * targetable_voters / total_voters, 2)::double as pct_targetable_voters,
        round(100.0 * high_opportunity_voters / total_voters, 2)::double as pct_high_opportunity_voters,
        
        -- Additional engagement percentages
        round(100.0 * missed_last_voters / total_voters, 2)::double as pct_missed_last_voters,
        round(100.0 * occasional_voters / total_voters, 2)::double as pct_occasional_voters,
        round(100.0 * infrequent_voters / total_voters, 2)::double as pct_infrequent_voters,
        round(100.0 * dormant_voters / total_voters, 2)::double as pct_dormant_voters,
        round(100.0 * never_voted / total_voters, 2)::double as pct_never_voted,
        
        -- Snapshot metadata
        current_date() as snapshot_date
        
    from snapshot_base
)

select * from final
order by state, party, age_group, gender