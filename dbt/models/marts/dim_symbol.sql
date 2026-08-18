{{ config(materialized = 'table') }}

-- Slowly changing dimension, Type 2, one row per version of a trading pair.
--
-- Built on `snap_symbol`, which dbt maintains: each day's exchangeInfo is compared against the
-- current version, and a new row is written only when a checked attribute actually differs.
-- A symbol whose metadata is unchanged for ninety days has one row, not ninety.
--
-- **Why the surrogate key includes the validity start.** A fact joins to the version of the
-- symbol that was current when the trade happened, not to the symbol as it is today. Keying on
-- the symbol alone would collapse every version into one and make that impossible — the point of
-- Type 2 is precisely that a minute from June joins to June's tick size, even after it changed
-- in August.
--
-- The activity columns are derived from the facts rather than from exchangeInfo, and they
-- describe the symbol's presence in this warehouse rather than its trading. They live on the
-- current row only: attaching them to a closed historical version would state a lifetime total
-- as though it were true at that point in time.

with versions as (

    select
        symbol,
        status,
        base_asset,
        quote_asset,
        tick_size,
        step_size,
        min_notional,
        min_qty,
        max_qty,
        spot_trading_allowed,
        margin_trading_allowed,

        -- **The first version of a symbol is backdated to the beginning of time.**
        --
        -- dbt stamps `dbt_valid_from` with the moment the snapshot first ran, which is when this
        -- pipeline started looking — not when the attribute became true. Facts predate that by
        -- months, so joining on the raw value drops every one of them: the fact table came back
        -- empty and all sixty tests still passed, because an empty table satisfies uniqueness,
        -- not-null and referential checks trivially.
        --
        -- Backdating states what is actually known: the earliest observation is the earliest
        -- evidence, and nothing contradicts it having held before. Later versions keep their real
        -- timestamps, because for those the moment of change *was* observed.
        --
        -- The floor is the epoch rather than `-infinity`. Postgres accepts `-infinity` happily,
        -- and it is the more honest value, but no Python client can load it: psycopg raises
        -- `timestamp too small (before year 1)` on read, so every query touching this column
        -- crashes rather than returning a row. A representable sentinel that predates all
        -- possible data costs nothing and keeps the column readable.
        case
            when row_number() over (partition by symbol order by dbt_valid_from) = 1
            then '1970-01-01'::timestamp
            else dbt_valid_from
        end                                             as valid_from,
        dbt_valid_to                                    as valid_to,
        dbt_valid_to is null                            as is_current

    from {{ ref('snap_symbol') }}

),

activity as (

    select
        symbol,
        min(dt)             as first_seen_date,
        max(dt)             as last_seen_date,
        count(distinct dt)  as days_observed,
        sum(trades)         as lifetime_trades,
        sum(quote_volume)   as lifetime_quote_volume
    from {{ ref('stg_klines') }}
    group by symbol

)

select
    -- Version-scoped, so a fact can point at the symbol as it was on the day of the trade.
    {{ dbt_utils.generate_surrogate_key(['v.symbol', 'v.valid_from']) }}  as symbol_version_key,

    -- Stable across versions, for the common case of asking about a pair rather than a point in
    -- its history.
    {{ dbt_utils.generate_surrogate_key(['v.symbol']) }}                  as symbol_key,

    v.symbol,
    v.base_asset,
    v.quote_asset,
    v.status,

    v.tick_size,
    v.step_size,
    v.min_notional,
    v.min_qty,
    v.max_qty,
    v.spot_trading_allowed,
    v.margin_trading_allowed,

    v.valid_from,
    v.valid_to,
    v.is_current,

    case when v.is_current then a.first_seen_date       end as first_seen_date,
    case when v.is_current then a.last_seen_date        end as last_seen_date,
    case when v.is_current then a.days_observed         end as days_observed,
    case when v.is_current then a.lifetime_trades       end as lifetime_trades,
    case when v.is_current then a.lifetime_quote_volume end as lifetime_quote_volume,

    -- A pair whose last candle is older than the newest date in the warehouse has stopped
    -- arriving: a delisting or a broken ingest, and worth a column either way.
    case
        when v.is_current
        then a.last_seen_date < (select max(dt) from {{ ref('stg_klines') }})
    end as is_stale

from versions v
left join activity a on a.symbol = v.symbol
