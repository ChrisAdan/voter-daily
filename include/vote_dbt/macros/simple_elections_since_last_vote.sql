-- Usage: {{ simple_elections_since_last_vote('last_voted_date') }}
-- Hardcoding a few federal election dates for simplicity. Later, would look to build in an election calendar in dim
{% macro simple_elections_since_last_vote(last_voted_date_column) %}
case 
    when {{ last_voted_date_column }} is null then null
    else 
        -- Count federal election cycles since last vote
        -- 2020, 2022, 2024, 2026... (November of even years)
        greatest(0,
            case 
                when {{ last_voted_date_column }} >= '2024-11-05' then 0  -- 2024 election
                when {{ last_voted_date_column }} >= '2022-11-08' then 1  -- Missed current, voted in 2022
                when {{ last_voted_date_column }} >= '2020-11-03' then 2  -- Missed 2024 & 2022, voted in 2020
                when {{ last_voted_date_column }} >= '2018-11-06' then 3  -- Missed 2024, 2022, 2020
                when {{ last_voted_date_column }} >= '2016-11-08' then 4  -- Missed 2024, 2022, 2020, 2018
                when {{ last_voted_date_column }} >= '2014-11-04' then 5  -- And so on...
                when {{ last_voted_date_column }} >= '2012-11-06' then 6
                when {{ last_voted_date_column }} >= '2010-11-02' then 7
                when {{ last_voted_date_column }} >= '2008-11-04' then 8
                else 9 -- 9+ elections missed (voted before 2008 or never)
            end
        )
end
{% endmacro %}