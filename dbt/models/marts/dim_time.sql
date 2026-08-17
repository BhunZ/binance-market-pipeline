{{ config(materialized = 'table') }}

-- One row per minute of the day, 0 to 1439. Fixed size, independent of how much data exists.
--
-- Generated rather than derived from the facts, unlike dim_date. Every minute of the day is a
-- valid minute whether or not a candle landed in it, and a gap here would hide exactly the case
-- worth finding: a minute that is missing across every symbol at once.
--
-- The session labels are the crypto convention — the market never closes, but liquidity follows
-- the Asian, European and US working days, and volume differs enough between them that grouping
-- by session is more useful than grouping by hour.

with minutes as (

    select generate_series(0, 1439) as minute_key

)

select
    minute_key,
    (minute_key / 60)::int                                   as hour_utc,
    (minute_key % 60)::int                                   as minute_of_hour,
    to_char(make_time(minute_key / 60, minute_key % 60, 0), 'HH24:MI') as time_utc,
    case
        when minute_key / 60 between  0 and  7 then 'asia'
        when minute_key / 60 between  8 and 15 then 'europe'
        else                                        'americas'
    end                                                      as session,
    -- The hour when European and US hours overlap carries the heaviest volume of the day, and
    -- it is the window most worth isolating in any liquidity comparison.
    minute_key / 60 between 13 and 16                        as is_overlap_hour,
    minute_key % 60 = 0                                      as is_hour_start
from minutes
