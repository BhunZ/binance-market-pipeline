{{ config(materialized = 'view') }}

-- One row per symbol-minute, typed and keyed, straight over the raw load.
--
-- The surrogate keys are built here rather than in the marts so that every downstream model
-- joins on the same expression. Two models deriving "the key for this minute" independently is
-- how a fact table ends up with rows no dimension matches.
--
-- `minute_key` is the minute of the day (0-1439) and `date_key` the date as an integer, which
-- makes both dimensions joinable without a lookup and keeps the fact table narrow.

with source as (

    select * from {{ source('raw', 'klines_1m') }}

),

typed as (

    select
        symbol,
        minute,
        dt,

        -- Integer surrogate keys. The natural keys are still on the row, so a debugging query
        -- never has to resolve a key to find out which minute it is looking at.
        cast(to_char(dt, 'YYYYMMDD') as integer)              as date_key,
        extract(hour from minute at time zone 'UTC') * 60
          + extract(minute from minute at time zone 'UTC')    as minute_key,

        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        trades,
        taker_base,
        taker_quote,

        -- Derived once, here, because three marts want it and each would round differently.
        -- Zero-volume minutes are real: Binance emits a candle even when nothing traded, so the
        -- guard is against dividing by zero, not against missing data.
        case when volume > 0 then quote_volume / volume end   as vwap,
        close - open                                          as change_abs,
        case when open > 0 then (close - open) / open end     as change_pct,
        high - low                                            as range_abs,

        -- A minute with no trades has open = high = low = close and volume 0. Flagged rather
        -- than filtered: how often a pair goes quiet is a property of the pair, and dropping
        -- those rows would make every symbol look equally liquid.
        trades = 0                                            as is_empty_minute,

        loaded_at

    from source

)

select * from typed
