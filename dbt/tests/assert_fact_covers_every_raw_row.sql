-- Every raw candle must reach the fact table.
--
-- The guard for the failure that has already happened once here. `dim_symbol` became Type 2, the
-- fact joined on the validity window, and because dbt stamps the first version with the moment
-- the snapshot ran rather than the moment the attribute became true, the join matched nothing.
-- The fact table came back with zero rows and **all sixty tests passed** — uniqueness, not-null
-- and referential integrity are all trivially true of an empty table.
--
-- A test that only checks the shape of what is present cannot notice that nothing is present.
-- This one compares against the source, which is the only thing that can.

with raw_count as (
    select count(*) as n from {{ source('raw', 'klines_1m') }}
),
fact_count as (
    select count(*) as n from {{ ref('fact_ohlcv_1m') }}
)

select
    r.n as raw_rows,
    f.n as fact_rows,
    r.n - f.n as rows_lost
from raw_count r
cross join fact_count f
where r.n <> f.n
