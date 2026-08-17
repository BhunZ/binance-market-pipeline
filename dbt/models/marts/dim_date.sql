{{ config(materialized = 'table') }}

-- One row per calendar date present in the data.
--
-- Built from the dates that exist rather than generated over a range: a date dimension covering
-- days the pipeline never ingested invites joins that silently produce rows with no facts, which
-- reads as "the market was closed" rather than "we have no data".
--
-- Crypto trades every day, so there is no trading-calendar flag here. Weekday and weekend are
-- still worth carrying, because volume differs across them and that is a real effect rather than
-- a market closure.

with dates as (

    select distinct dt, date_key from {{ ref('stg_klines') }}

)

select
    date_key,
    dt                                            as date,
    extract(year    from dt)::int                 as year,
    extract(quarter from dt)::int                 as quarter,
    extract(month   from dt)::int                 as month,
    extract(day     from dt)::int                 as day_of_month,
    extract(week    from dt)::int                 as iso_week,
    extract(isodow  from dt)::int                 as iso_day_of_week,
    to_char(dt, 'Day')                            as day_name,
    to_char(dt, 'Month')                          as month_name,
    extract(isodow from dt) in (6, 7)             as is_weekend,
    dt = date_trunc('month', dt)::date            as is_month_start,
    dt = (date_trunc('month', dt) + interval '1 month - 1 day')::date as is_month_end
from dates
