{{ config(materialized = 'table') }}

-- How much of each day the stream actually saw.
--
-- Kept apart from the reconciliation on purpose. That model answers *is the stream correct where
-- it ran*; this one answers *did it run*. Folding them together would let a two-hour outage
-- appear as an accuracy problem, or worse, average away against minutes that matched perfectly.
--
-- A gap here is expected and is not a defect: the consumer runs on one small VM, it restarts, and
-- Binance serves no history for the trade feed, so a minute missed is a minute gone. The value of
-- measuring it is being able to say **exactly** how much is missing rather than implying none is.

with batch_minutes as (

    select dt, symbol, count(*) as minutes_available
    from {{ source('raw', 'klines_1m') }}
    group by dt, symbol

),

stream_minutes as (

    select dt, symbol, count(*) as minutes_captured
    from {{ source('raw', 'trades_1m') }}
    group by dt, symbol

)

select
    b.dt,
    b.symbol,
    b.minutes_available,
    coalesce(s.minutes_captured, 0)                  as minutes_captured,
    b.minutes_available - coalesce(s.minutes_captured, 0) as minutes_missing,
    round(coalesce(s.minutes_captured, 0)::numeric
          / nullif(b.minutes_available, 0) * 100, 2) as coverage_pct
from batch_minutes b
left join stream_minutes s
    on s.dt = b.dt and s.symbol = b.symbol
