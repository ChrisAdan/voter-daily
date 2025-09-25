-- models/dimensions/dim_voter.sql
{{ config(
    materialized='incremental',
    unique_key='source_id',
    on_schema_change='fail'
) }}

with base as (
    select distinct
        source_id,
        first_name,
        last_name,
        age,
        gender,
        state,
        party,
        case 
            when email like '%@%' then lower(email)
            else null
        end as email,
        registered_date,
        last_voted_date,
        current_date() as inserted_date,
        current_date() as last_modified_date
    from {{ source('raw', 'vote_records') }}
),

deduped as (
    select *
    from base
    qualify count(*) over (partition by source_id) = 1
)

select *
from deduped
where
    source_id is not null
    and age is not null
    and gender is not null
    and email is not null
    and length(state) = 2
    and registered_date is not null
    and party in ('Democrat', 'Republican', 'Independent')
    and party in ('Democrat', 'Republican', 'Independent')

{% if is_incremental() %}
  -- Only consider records that are new or updated since the last run
  and last_modified_date > (
      select coalesce(max(last_modified_date), '1900-01-01') 
      from {{ this }}
  )
{% endif %}
