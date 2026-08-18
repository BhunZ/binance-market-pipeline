{% snapshot snap_symbol %}
{{ config(
    target_schema = 'snapshots',
    unique_key = 'symbol',
    strategy = 'check',
    check_cols = ['status', 'tick_size', 'step_size', 'min_notional',
                  'spot_trading_allowed', 'margin_trading_allowed'],
    invalidate_hard_deletes = True
) }}

-- Slowly changing dimension, Type 2, over symbol metadata.
--
-- `check` rather than `timestamp`: Binance's exchangeInfo carries no "last modified" field, so
-- there is nothing to compare against. dbt compares the listed columns instead and writes a new
-- version only when one of them actually differs — a daily snapshot of an unchanged symbol adds
-- no row.
--
-- The checked columns are the ones that genuinely move. `status` changes when a pair is
-- suspended or delisted. `tick_size` and `step_size` are retuned as a price crosses an order of
-- magnitude. `min_notional` changes with exchange policy. Deliberately absent: base_asset and
-- quote_asset, which are fixed by the pair's name, and the precision fields, which have never
-- been observed to change — versioning on a column that cannot change costs a comparison per row
-- per day and can never produce a version.
--
-- `invalidate_hard_deletes` closes the validity window for a symbol that stops appearing at all.
-- Without it a delisted pair keeps a row that says it is current forever, which is the one thing
-- a symbol dimension exists to prevent.

select
    symbol,
    status,
    base_asset,
    quote_asset,
    base_precision,
    quote_precision,
    spot_trading_allowed,
    margin_trading_allowed,
    tick_size,
    min_price,
    max_price,
    step_size,
    min_qty,
    max_qty,
    min_notional,
    max_notional,
    snapshot_date
from {{ source('raw', 'exchange_info') }}
where snapshot_date = (select max(snapshot_date) from {{ source('raw', 'exchange_info') }})

{% endsnapshot %}
