-- No minute may belong to two dates.
--
-- The invariant the ingest layer broke once: Binance's endTime is inclusive, so a naive
-- midnight-to-midnight request returns 1441 candles and the extra one belongs to the next day.
-- In a single Bronze partition that is invisible. Here, where the partitions are read together,
-- it is one duplicated minute per symbol per day.
--
-- Guarded in two places on purpose — `tests/test_klines.py` catches it at the boundary
-- arithmetic, this catches it in the data that actually landed.

select
    minute,
    symbol,
    count(distinct date_key) as dates_claiming_this_minute
from {{ ref('fact_ohlcv_1m') }}
group by minute, symbol
having count(distinct date_key) > 1
