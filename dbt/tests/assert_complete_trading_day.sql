-- Every symbol must have all 1440 minutes of every completed day.
--
-- This is the test that turns "the pipeline ran" into "the pipeline ran correctly". Binance
-- emits a candle even for minutes with no trades — measured on CHIPUSDT, 310 of 1440 minutes had
-- zero trades and all 1440 candles were still returned — so a shortfall is lost data, never a
-- quiet market.
--
-- The most recent date is excluded because it may still be in progress. Including it would fail
-- this test every day until midnight, and a test that cries wolf daily is a test people learn to
-- ignore.

select
    symbol,
    date_key,
    count(*) as minutes_present,
    1440 - count(*) as minutes_missing
from {{ ref('fact_ohlcv_1m') }}
where date_key < (select max(date_key) from {{ ref('fact_ohlcv_1m') }})
group by symbol, date_key
having count(*) <> 1440
