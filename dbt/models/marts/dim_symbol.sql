{{ config(materialized = 'table') }}

-- One row per trading pair.
--
-- **This is where SCD Type 2 will go, and it does not belong here yet.** A slowly changing
-- dimension needs an attribute that actually changes — tick size, minimum order size, trading
-- status — and those come from Binance's `exchangeInfo`, which this pipeline does not yet
-- capture. Everything below is derived from the symbol name and from the facts, so it cannot
-- change without the data itself changing, and giving it `valid_from` / `valid_to` columns would
-- be a schema that describes history it does not have.
--
-- The honest version is a Type 1 dimension now and a Type 2 dimension once daily `exchangeInfo`
-- snapshots land in Bronze. Written this way so the swap is a change to this model alone.
--
-- The activity columns are a deliberate exception to "dimensions hold attributes, facts hold
-- measures". First and last seen dates, and the count of days observed, are properties of the
-- pair's presence in this warehouse rather than measures of trading, and having them here is
-- what lets a query find a symbol that stopped arriving without scanning the fact table.

with observed as (

    select
        symbol,
        min(dt)                  as first_seen_date,
        max(dt)                  as last_seen_date,
        count(distinct dt)       as days_observed,
        sum(trades)              as lifetime_trades,
        sum(quote_volume)        as lifetime_quote_volume,
        avg(close)               as avg_close
    from {{ ref('stg_klines') }}
    group by symbol

),

parsed as (

    select
        *,
        -- Every pair here quotes in USDT. Splitting on the suffix rather than a lookup table
        -- keeps the model self-contained; if a non-USDT pair is ever added, the base asset comes
        -- out wrong and the test on quote_asset fails rather than the error passing unnoticed.
        case when symbol like '%USDT' then left(symbol, length(symbol) - 4) else symbol end
            as base_asset,
        case when symbol like '%USDT' then 'USDT' end
            as quote_asset
    from observed

)

select
    {{ dbt_utils.generate_surrogate_key(['symbol']) }}       as symbol_key,
    symbol,
    base_asset,
    quote_asset,

    first_seen_date,
    last_seen_date,
    days_observed,
    lifetime_trades,
    lifetime_quote_volume,
    avg_close,

    -- A pair whose last candle is older than the newest date in the warehouse has stopped
    -- arriving. That is either a delisting or a broken ingest, and either way it is worth
    -- surfacing as a column rather than leaving to whoever notices the gap.
    last_seen_date < (select max(dt) from {{ ref('stg_klines') }})  as is_stale,

    -- Kept for the Type 2 rewrite: today every row is current because there is only ever one row
    -- per symbol. Carrying the column now means downstream queries already filter on it, and the
    -- day history arrives nothing downstream has to change.
    true                                                     as is_current

from parsed
