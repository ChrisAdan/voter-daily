-- models/dimensions/dim_voter.sql
{{ config(materialized='table') }}
with base as (
select distinct
    source_id,
    first_name,
    last_name,
    age,
    gender,
    state,
    party,
    -- standardize: lowercase emails, ensure contain '@'
    -- Note: here's another location I'd consider for more robust handling;
    -- Email validation can be complex, so I would start by splitting this into a macro
    -- and looking up other email validation solutions to bring in
    -- we also use dbt_expectations to check emails against a simple regex
    case 
        when email like '%@%' then lower(email)
        else null
    end as email,
    registered_date,
    last_voted_date,
    current_date() as inserted_date,
    current_date() as last_modified_date
from {{ source('raw', 'vote_records') }}
)
select * from base
where
    source_id is not null
    and age is not null
    and gender is not null
    and email is not null
    and length(state) = 2
    and registered_date is not null
    and party in ('Democrat', 'Republican', 'Independent')