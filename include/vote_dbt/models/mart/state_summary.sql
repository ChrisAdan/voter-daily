-- models/mart/state_summary.sql  
-- High-level state summaries for executive dashboards and geographic analysis
{{ config(
    materialized='table',
    schema='mart'
) }}

with voter_base as (
    select * from {{ ref('stage_voter_metrics') }}
),

state_totals as (
    select
        state,
        
        -- Total registered voters
        count(distinct voter_id)::bigint as total_registered_voters,
        
        -- Party composition
        sum(case when party = 'Democrat' then 1 else 0 end)::bigint as democrat_voters,
        sum(case when party = 'Republican' then 1 else 0 end)::bigint as republican_voters, 
        sum(case when party = 'Independent' then 1 else 0 end)::bigint as independent_voters,
        
        -- Age distribution  
        sum(case when age_group = '18-29' then 1 else 0 end)::bigint as voters_18_29,
        sum(case when age_group = '30-49' then 1 else 0 end)::bigint as voters_30_49,
        sum(case when age_group = '50-64' then 1 else 0 end)::bigint as voters_50_64,
        sum(case when age_group = '65+' then 1 else 0 end)::bigint as voters_65_plus,
        
        -- Gender distribution
        sum(case when gender = 'M' then 1 else 0 end)::bigint as male_voters,
        sum(case when gender = 'F' then 1 else 0 end)::bigint as female_voters,
        
        -- Engagement metrics
        avg(elections_since_last_vote)::double as avg_elections_missed,
        sum(case when voter_engagement_segment = 'Current Voter' then 1 else 0 end)::bigint as current_voters,
        sum(case when voter_engagement_segment in ('Missed Last Election', 'Occasional Voter') then 1 else 0 end)::bigint as recoverable_voters,
        sum(case when voter_engagement_segment in ('Infrequent Voter', 'Dormant Voter') then 1 else 0 end)::bigint as disengaged_voters,
        sum(case when voter_engagement_segment = 'Never Voted' then 1 else 0 end)::bigint as never_voted,
        
        -- Registration tenure
        avg(years_since_registered)::double as avg_years_registered,
        median(years_since_registered)::double as median_years_registered
        
    from voter_base
    group by state
),

state_percentages as (
    select
        state::varchar(2) as state,
        total_registered_voters,
        
        -- Party percentages
        round(100.0 * democrat_voters / total_registered_voters, 2)::double as pct_democrat,
        round(100.0 * republican_voters / total_registered_voters, 2)::double as pct_republican,
        round(100.0 * independent_voters / total_registered_voters, 2)::double as pct_independent,
        
        -- Age percentages
        round(100.0 * voters_18_29 / total_registered_voters, 2)::double as pct_18_29,
        round(100.0 * voters_30_49 / total_registered_voters, 2)::double as pct_30_49,  
        round(100.0 * voters_50_64 / total_registered_voters, 2)::double as pct_50_64,
        round(100.0 * voters_65_plus / total_registered_voters, 2)::double as pct_65_plus,
        
        -- Gender percentages
        round(100.0 * female_voters / total_registered_voters, 2)::double as pct_female,
        round(100.0 * male_voters / total_registered_voters, 2)::double as pct_male,
        
        -- Engagement percentages
        round(100.0 * current_voters / total_registered_voters, 2)::double as pct_current_voters,
        round(100.0 * recoverable_voters / total_registered_voters, 2)::double as pct_recoverable_voters,
        round(100.0 * disengaged_voters / total_registered_voters, 2)::double as pct_disengaged_voters,
        round(100.0 * never_voted / total_registered_voters, 2)::double as pct_never_voted,
        
        -- Other metrics
        round(avg_elections_missed, 2)::double as avg_elections_missed,
        round(avg_years_registered, 1)::double as avg_years_registered,
        round(median_years_registered, 1)::double as median_years_registered,
        
        -- Absolute counts for reference
        democrat_voters,
        republican_voters,
        independent_voters,
        current_voters,
        recoverable_voters,
        disengaged_voters
        
    from state_totals
),

final as (
    select
        state,
        total_registered_voters,
        
        -- Party percentages
        pct_democrat,
        pct_republican, 
        pct_independent,
        
        -- Age percentages
        pct_18_29,
        pct_30_49,
        pct_50_64,
        pct_65_plus,
        
        -- Gender percentages
        pct_female,
        pct_male,
        
        -- Engagement percentages
        pct_current_voters,
        pct_recoverable_voters,
        pct_disengaged_voters,
        pct_never_voted,
        
        -- Summary metrics
        avg_elections_missed,
        avg_years_registered,
        median_years_registered,
        
        -- Absolute counts
        democrat_voters,
        republican_voters,
        independent_voters,
        current_voters,
        recoverable_voters,
        disengaged_voters,
        
        -- Calculated fields
        round(
            (pct_recoverable_voters * 0.6) +  -- Recent lapsers weighted high
            (pct_disengaged_voters * 0.3) +   -- Longer-term lapsers medium weight  
            (pct_never_voted * 0.1)           -- Never voted low weight
        , 2)::double as engagement_opportunity_score,
        
        -- Competitive balance indicator
        case
            when abs(pct_democrat - pct_republican) <= 5 then 'Highly Competitive'
            when abs(pct_democrat - pct_republican) <= 15 then 'Competitive'  
            when pct_democrat > pct_republican then 'Lean Democrat'
            else 'Lean Republican'
        end::varchar(20) as partisan_lean,
        
        current_date() as summary_date
        
    from state_percentages
)

select * from final
order by total_registered_voters desc