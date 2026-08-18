{{ config(materialized = 'table') }}

-- Where the two layers disagree, and by how much.
--
-- The batch layer asks Binance for a finished one-minute candle. The speed layer builds the same
-- candle from the live trade feed. They are computed from different sources over different
-- transports by different code, so agreement is evidence and disagreement is a fault report.
--
-- **Without this, a broken stream is invisible.** A consumer that drops one message in fifty
-- keeps running, keeps writing bars, and keeps producing numbers that are slightly wrong. No
-- exception, no gap, no row-count anomaly — just volumes a couple of percent light. The only way
-- to see it is to compute the same minute twice and compare.
--
-- Three faults produce three signatures, and telling them apart is the point of storing the
-- differences rather than a single pass/fail:
--
--   * **Dropped messages** — the stream's trade count is lower and volume is lower in proportion.
--     Steady across symbols, worse during busy minutes.
--   * **A window boundary off by a timezone or a second** — open and close disagree while volume
--     roughly matches, because the same trades were split across neighbouring minutes.
--   * **Trades arriving after their window was written** — the stream is light by exactly the
--     amount sitting in `late_arrivals`, and joining the two accounts for the difference.
--
-- Only minutes present in **both** layers are compared. A minute the stream never saw is a
-- coverage gap, which `fct_stream_coverage` reports separately — mixing the two would let an
-- outage look like an accuracy problem.

with batch as (

    select symbol, minute, dt, open, high, low, close, volume, quote_volume, trades, taker_base
    from {{ source('raw', 'klines_1m') }}

),

stream as (

    -- The newest minute is excluded because a live stream is always part way through writing
    -- it: some symbols are on object storage, the rest are still open in memory. Comparing it
    -- reports the reader's timing as a pipeline fault, which is how a healthy run came back
    -- showing eight of twenty symbols short by 150%.
    select symbol, minute, open, high, low, close, volume, quote_volume, trades, taker_base
    from {{ source('raw', 'trades_1m') }}
    where minute < (select max(minute) from {{ source('raw', 'trades_1m') }})

),

paired as (

    select
        b.symbol,
        b.minute,
        b.dt,

        b.trades        as batch_trades,
        s.trades        as stream_trades,
        b.volume        as batch_volume,
        s.volume        as stream_volume,
        b.close         as batch_close,
        s.close         as stream_close,
        b.open          as batch_open,
        s.open          as stream_open,
        b.taker_base    as batch_taker_base,
        s.taker_base    as stream_taker_base,

        s.trades - b.trades                             as trade_count_diff,
        s.volume - b.volume                             as volume_diff,

        -- Relative, because absolute volume spans six orders of magnitude across these pairs and
        -- a threshold in absolute terms would be meaningless for all but one of them.
        case when b.volume > 0
             then abs(s.volume - b.volume) / b.volume end   as volume_diff_pct,
        case when b.trades > 0
             then abs(s.trades - b.trades)::numeric / b.trades end as trade_diff_pct,

        -- Prices should be identical, not merely close: open and close are single trades, not
        -- aggregates, so any difference means the two layers cut the minute differently.
        b.open  = s.open                                as open_matches,
        b.close = s.close                               as close_matches

    from batch b
    inner join stream s
        on s.symbol = b.symbol
       and s.minute = b.minute

)

select
    *,

    -- The diagnosis, from the signature rather than from the size of the difference.
    case
        when trade_count_diff = 0 and open_matches and close_matches
            then 'exact'
        when not open_matches or not close_matches
            then 'boundary_mismatch'
        when trade_count_diff < 0
            then 'stream_missed_trades'
        when trade_count_diff > 0
            then 'stream_double_counted'
        else 'volume_only'
    end as verdict

from paired
