-- models/mart/targeting_opportunities.sql
-- Identifies demographic segments with high voter re-engagement potential
{{ config(
    materialized='table',
    schema='mart'
) }}

with voter_metrics as (
    select * from {{ ref('stage_voter_metrics') }}
),

segment_analysis as (
    select
        state::varchar(2) as state,
        age_group::varchar(10) as age_group,
        gender::varchar(1) as gender,
        party::varchar(12) as party,
        
        -- Core metrics
        count(distinct voter_id)::bigint as total_voters,
        
        -- Engagement distribution
        sum(case when elections_since_last_vote = 0 then 1 else 0 end)::bigint as active_voters,
        sum(case when elections_since_last_vote = 1 then 1 else 0 end)::bigint as lapsed_1_election,
        sum(case when elections_since_last_vote between 2 and 3 then 1 else 0 end)::bigint as lapsed_2_3_elections,
        sum(case when elections_since_last_vote between 4 and 6 then 1 else 0 end)::bigint as lapsed_4_6_elections,
        sum(case when elections_since_last_vote >= 7 then 1 else 0 end)::bigint as dormant_voters,
        sum(case when elections_since_last_vote is null then 1 else 0 end)::bigint as never_voted,
        
        -- Average metrics for prioritization
        avg(elections_since_last_vote)::double as avg_elections_missed,
        avg(years_since_registered)::double as avg_registration_tenure,
        
        -- Key targeting segments
        sum(case when elections_since_last_vote between 1 and 3 then 1 else 0 end)::bigint as prime_target_voters,
        sum(case when elections_since_last_vote between 2 and 6 then 1 else 0 end)::bigint as broad_target_voters
        
    from voter_metrics
    group by 1,2,3,4
),

scoring as (
    select
        state,
        age_group,
        gender,
        party,
        total_voters,
        active_voters,
        lapsed_1_election,
        lapsed_2_3_elections,
        lapsed_4_6_elections,
        dormant_voters,
        never_voted,
        avg_elections_missed,
        avg_registration_tenure,
        prime_target_voters,
        broad_target_voters,
        
        -- Calculate targeting opportunity percentages
        round(100.0 * prime_target_voters / total_voters, 2)::double as pct_prime_targets,
        round(100.0 * broad_target_voters / total_voters, 2)::double as pct_broad_targets,
        round(100.0 * lapsed_1_election / total_voters, 2)::double as pct_recently_lapsed,
        round(100.0 * (lapsed_2_3_elections + lapsed_4_6_elections) / total_voters, 2)::double as pct_medium_lapsed,
        round(100.0 * dormant_voters / total_voters, 2)::double as pct_dormant,
        round(100.0 * never_voted / total_voters, 2)::double as pct_never_voted,
        round(100.0 * active_voters / total_voters, 2)::double as pct_active_voters,
        
        -- Create composite opportunity scores (0-100 scale)
        case
            when total_voters < 3 then 0.0  -- Skip small segments [mock data, so bear with me]
            else round(
                -- Weight recent lapsers highest (40%)
                (40.0 * lapsed_1_election / total_voters) +
                -- Medium lapsers get moderate weight (30%) 
                (30.0 * (lapsed_2_3_elections + lapsed_4_6_elections) / total_voters) +
                -- Long tenure voters more likely to re-engage (20%)
                (20.0 * least(avg_registration_tenure / 10.0, 1.0)) +
                -- Slight bonus for larger segments (10%)
                (10.0 * least(total_voters / 1000.0, 1.0))
            , 2)
        end::double as opportunity_score
        
    from segment_analysis
),

ranked as (
    select
        *,
        -- Rank segments within each state for prioritization
        row_number() over (
            partition by state 
            order by opportunity_score desc, total_voters desc
        )::integer as state_rank,
        
        -- Overall national ranking
        row_number() over (
            order by opportunity_score desc, total_voters desc  
        )::integer as national_rank,
        
        -- Create targeting tiers | with more sample data, these brackets should be refined
        case
            when opportunity_score >= 25 and total_voters >= 5 then 'High Priority'
            when opportunity_score >= 15 and total_voters >= 3 then 'Medium Priority'  
            when opportunity_score >= 8 and total_voters >= 2 then 'Low Priority'
            else 'Monitor Only'
        end::varchar(15) as targeting_tier
        
    from scoring
),

final as (
    select
        -- Identifiers
        state,
        age_group,
        gender, 
        party,
        targeting_tier,
        opportunity_score,
        state_rank,
        national_rank,
        
        -- Size metrics
        total_voters,
        
        -- Target segments (absolute numbers)
        prime_target_voters,
        broad_target_voters,
        active_voters,
        lapsed_1_election,
        lapsed_2_3_elections,
        lapsed_4_6_elections,
        dormant_voters,
        never_voted,
        
        -- Target segments (percentages)
        pct_prime_targets,
        pct_broad_targets,
        pct_active_voters,
        pct_recently_lapsed,
        pct_medium_lapsed,
        pct_dormant,
        pct_never_voted,
        
        -- Contextual metrics
        avg_elections_missed,
        avg_registration_tenure,
        
        -- Metadata
        current_date() as analysis_date
        
    from ranked
)

select * from final
where total_voters >= 1  -- Filter out very small segments
order by opportunity_score desc, total_voters desc