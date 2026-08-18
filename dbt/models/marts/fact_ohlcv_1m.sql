{{ config(
    materialized = 'incremental',
    unique_key = ['symbol', 'date_key', 'minute_key'],
    incremental_strategy = 'delete+insert',
    partition_by = 'date_key'
) }}

-- The grain is one symbol, one minute. Nothing else.
--
-- Stating the grain first is not ceremony: every column below either identifies that grain or
-- measures it, and a column that does neither belongs in a dimension. The commonest way a fact
-- table goes wrong is a measure at a different grain sneaking in — a daily total on a
-- minute-level row — after which every sum is silently multiplied by 1440.
--
-- **Incremental by delete+insert, keyed on the date.** A re-ingested day is a whole partition
-- being corrected, so the run deletes the dates it is about to write and writes them fresh. A
-- merge on the row key would leave behind any minute that used to exist and no longer does,
-- which is the exact case a correction is meant to fix.

with klines as (

    select * from {{ ref('stg_klines') }}

    {% if is_incremental() %}
    -- Only the dates in this batch. The subquery is over the source rather than the target so a
    -- date can be re-run after its rows were deleted, which a `max(date_key) from this` filter
    -- would make impossible.
    where dt >= (select coalesce(max(date), '1970-01-01'::date) from {{ ref('dim_date') }})
    {% endif %}

),

joined as (

    select
        -- Points at the version of the symbol that was current when the minute happened, not at
        -- the symbol as it is today. This is the entire reason the dimension is Type 2: a minute
        -- from June must join to June's tick size even after it changed in August.
        s.symbol_version_key,
        s.symbol_key,
        k.date_key,
        k.minute_key,

        k.symbol,
        k.minute,

        k.open,
        k.high,
        k.low,
        k.close,

        k.volume,
        k.quote_volume,
        k.trades,
        k.taker_base,
        k.taker_quote,

        -- Taker buy volume as a share of the total. Above a half means buyers were crossing the
        -- spread more than sellers, which is the closest thing a candle carries to direction of
        -- pressure. Null rather than zero when nothing traded: no trades means no answer, and
        -- zero would read as "sellers dominated".
        case when k.volume > 0 then k.taker_base / k.volume end as taker_buy_ratio,

        k.vwap,
        k.change_abs,
        k.change_pct,
        k.range_abs,
        k.is_empty_minute,

        k.loaded_at

    from klines k
    inner join {{ ref('dim_symbol') }} s
        on  s.symbol = k.symbol
        -- The validity window is half-open: valid_from inclusive, valid_to exclusive, and null
        -- valid_to means still current. A closed interval on both ends would match two versions
        -- on the instant one replaced the other and silently double every measure for that
        -- minute.
        and k.minute >= s.valid_from
        and (s.valid_to is null or k.minute < s.valid_to)

)

select * from joined
