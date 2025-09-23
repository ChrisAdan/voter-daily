-- models/mart/partisan_trends.sql
-- Time series analysis of partisan composition by demographics and election cycles
{{ config(
    materialized='table',
    schema='mart'
) }}

with voter_base as (
    select * from {{ ref('stage_voter_metrics') }}
),

-- Create election cycle dimension based on federal election dates
election_cycles as (
    select
        '2024-11-05'::date as election_date,
        'Presidential'::varchar(12) as election_type,
        2024::integer as election_year
    union all
    select '2022-11-08'::date, 'Midterm'::varchar(12), 2022::integer
    union all
    select '2020-11-03'::date, 'Presidential'::varchar(12), 2020::integer
    union all  
    select '2018-11-06'::date, 'Midterm'::varchar(12), 2018::integer
    union all
    select '2016-11-08'::date, 'Presidential'::varchar(12), 2016::integer
    union all
    select '2014-11-04'::date, 'Midterm'::varchar(12), 2014::integer
    union all
    select '2012-11-06'::date, 'Presidential'::varchar(12), 2012::integer
    union all
    select '2010-11-02'::date, 'Midterm'::varchar(12), 2010::integer
    union all
    select '2008-11-04'::date, 'Presidential'::varchar(12), 2008::integer
),

-- For each election, calculate who was registered and eligible
voter_eligibility_by_election as (
    select
        v.voter_id::varchar as voter_id,
        v.state::varchar(2) as state,
        v.age_group::varchar(10) as age_group,
        v.gender::varchar(1) as gender,
        v.party::varchar(12) as party,
        v.registered_date,
        e.election_date,
        e.election_type,
        e.election_year,
        
        -- Was voter registered by this election?
        (case when v.registered_date <= e.election_date then 1 else 0 end)::integer as was_registered,
        
        -- Did voter participate in this specific election? 
        case 
            when v.registered_date <= e.election_date 
            and v.last_voted_date is not null
            and abs(datediff('day', v.last_voted_date, e.election_date)) <= 90
            then 1 else 0 
        end::integer as likely_voted
        
    from voter_base v
    cross join election_cycles e
),

-- Calculate totals by demographic group first
demo_totals as (
    select
        election_year,
        election_type,
        election_date,
        state,
        age_group,
        gender,
        sum(was_registered)::bigint as demo_eligible_total,
        sum(likely_voted)::bigint as demo_voted_total
    from voter_eligibility_by_election
    group by 1,2,3,4,5,6
),

-- Calculate party totals
party_totals as (
    select
        election_year,
        election_type,
        election_date,
        state,
        age_group,
        gender,
        party,
        sum(was_registered)::bigint as eligible_voters,
        sum(likely_voted)::bigint as estimated_voters
    from voter_eligibility_by_election
    group by 1,2,3,4,5,6,7
    having sum(was_registered) > 0
),

-- Join and calculate percentages
partisan_trends as (
    select
        pt.election_year,
        pt.election_type,
        pt.election_date,
        pt.state,
        pt.age_group,
        pt.gender,
        pt.party,
        pt.eligible_voters,
        pt.estimated_voters,
        
        -- Participation rate
        case 
            when pt.eligible_voters > 0 
            then round(100.0 * pt.estimated_voters / pt.eligible_voters, 2)
            else 0.0 
        end::double as participation_rate,
        
        -- Party share of eligible voters within demographic
        case 
            when dt.demo_eligible_total > 0
            then round(100.0 * pt.eligible_voters / dt.demo_eligible_total, 2)
            else 0.0
        end::double as pct_of_eligible_by_demo,
        
        -- Party share of actual voters within demographic
        case 
            when dt.demo_voted_total > 0
            then round(100.0 * pt.estimated_voters / dt.demo_voted_total, 2)
            else 0.0
        end::double as pct_of_voters_by_demo
        
    from party_totals pt
    join demo_totals dt using (election_year, election_type, election_date, state, age_group, gender)
),

final as (
    select 
        election_year::integer as election_year,
        election_type::varchar(12) as election_type,
        election_date::date as election_date,
        state::varchar(2) as state,
        age_group::varchar(10) as age_group,
        gender::varchar(1) as gender,
        party::varchar(12) as party,
        eligible_voters::bigint as eligible_voters,
        estimated_voters::bigint as estimated_voters,
        participation_rate::double as participation_rate,
        pct_of_eligible_by_demo::double as pct_of_eligible_by_demo,
        pct_of_voters_by_demo::double as pct_of_voters_by_demo,
        
        -- Add trend indicators
        lag(eligible_voters) over (
            partition by state, age_group, gender, party, election_type 
            order by election_year
        )::bigint as prev_eligible_voters,
        
        lag(participation_rate) over (
            partition by state, age_group, gender, party, election_type 
            order by election_year
        )::double as prev_participation_rate,
        
        -- Calculate trend changes
        case 
            when lag(eligible_voters) over (
                partition by state, age_group, gender, party, election_type 
                order by election_year
            ) > 0
            then round(100.0 * (
                eligible_voters - lag(eligible_voters) over (
                    partition by state, age_group, gender, party, election_type 
                    order by election_year
                )
            ) / lag(eligible_voters) over (
                partition by state, age_group, gender, party, election_type 
                order by election_year
            ), 2)
            else null
        end::double as eligible_voter_change_pct,
        
        case 
            when lag(participation_rate) over (
                partition by state, age_group, gender, party, election_type 
                order by election_year
            ) is not null 
            then round(participation_rate - lag(participation_rate) over (
                partition by state, age_group, gender, party, election_type 
                order by election_year
            ), 2)
            else null  
        end::double as participation_rate_change_pts
        
    from partisan_trends
)

select * from final
order by election_year desc, state, party, age_group, gender