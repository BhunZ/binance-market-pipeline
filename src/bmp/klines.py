"""Fetch one symbol-day of 1-minute candles and write it as a Bronze partition.

The unit of work is deliberately **one symbol on one date**. That is what makes a run
idempotent: re-running writes the same partition with the same content, so a backfill can be
re-issued for any date without producing duplicates and without a merge step. Choosing the unit
first is most of the design.

**Bronze keeps the source's own numbers.** Prices arrive as strings so that a float conversion
cannot silently round a value before anyone has looked at it, and the string is what Binance
actually sent. Typing belongs in the next layer, where a bad cast is visible and reversible.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
from dataclasses import dataclass

import pandas as pd

from . import binance
from .config import KLINES_MAX_LIMIT, MINUTES_PER_DAY

logger = logging.getLogger(__name__)

#: Binance returns twelve positional fields per candle. Naming them here, once, is the only
#: thing standing between this project and a silent column shift if the API ever reorders them.
KLINE_FIELDS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_base", "taker_quote", "ignore",
]

#: Dropped on the way in. `ignore` is documented by Binance as unused, and `close_time` is
#: always open_time + 59,999 ms — storing it is 2.6 million copies of one arithmetic fact.
DROP_FIELDS = ["ignore", "close_time"]


@dataclass(frozen=True)
class DayResult:
    """What one symbol-day produced. Returned so the DAG can log and validate without re-reading
    the file it just wrote."""

    symbol: str
    run_date: str
    rows: int
    expected: int
    first_minute: str | None
    last_minute: str | None

    @property
    def complete(self) -> bool:
        return self.rows == self.expected

    @property
    def missing(self) -> int:
        return self.expected - self.rows


def day_bounds_ms(run_date: str) -> tuple[int, int]:
    """UTC midnight to 23:59 inclusive, in milliseconds.

    UTC, not local time. Binance timestamps everything in UTC, and a partition keyed on local
    dates would hold 1440 minutes that are not the 1440 minutes of that date anywhere else — the
    kind of error that surfaces months later as an unexplainable seven-hour offset.

    **The end is inclusive**, so it is the last minute of the day and not the first minute of the
    next one. Binance's `endTime` includes the candle that opens exactly at that instant, so
    passing the next midnight returns 1441 candles and the extra one belongs to tomorrow. That
    minute would then land in two partitions at once — invisible in either file, and a duplicate
    key the moment the two are read together.
    """
    d = dt.date.fromisoformat(run_date)
    start = dt.datetime.combine(d, dt.time.min, tzinfo=dt.UTC)
    end = start + dt.timedelta(days=1) - dt.timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def expected_minutes(run_date: str, now: dt.datetime | None = None) -> int:
    """How many candles this date should have.

    A day in the past has 1440. Today has however many minutes have elapsed — asking for 1440 of
    them and calling the result incomplete would fail every run before midnight.
    """
    now = now or dt.datetime.now(dt.UTC)
    d = dt.date.fromisoformat(run_date)
    if d < now.date():
        return MINUTES_PER_DAY
    if d > now.date():
        return 0
    return now.hour * 60 + now.minute


def fetch_day(symbol: str, run_date: str) -> pd.DataFrame:
    """Every 1-minute candle for one symbol on one UTC date.

    Paged because one call caps at 1000 candles and a day holds 1440. Paging advances by the
    last candle's open time plus one millisecond rather than by a count, so a gap in the middle
    of the day does not shift every later page.
    """
    start_ms, end_ms = day_bounds_ms(run_date)
    rows: list[list] = []
    cursor = start_ms

    while cursor <= end_ms:
        batch = binance.klines(symbol, "1m", cursor, end_ms, KLINES_MAX_LIMIT)
        if not batch:
            break
        rows.extend(batch)
        cursor = int(batch[-1][0]) + 1
        if len(batch) < KLINES_MAX_LIMIT:
            break

    df = pd.DataFrame(rows, columns=KLINE_FIELDS)
    if df.empty:
        return df.drop(columns=DROP_FIELDS)

    # Binance can return the same candle twice across page boundaries when a request lands
    # exactly on one. Deduplicating here rather than downstream keeps Bronze honest: the file
    # holds one row per minute, which is the only claim Bronze makes about its own content.
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")

    df["symbol"] = symbol
    df["minute"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    return df.drop(columns=DROP_FIELDS).reset_index(drop=True)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="zstd")
    return buf.getvalue()


def summarise(df: pd.DataFrame, symbol: str, run_date: str) -> DayResult:
    minutes = df["minute"] if "minute" in df.columns and not df.empty else None
    return DayResult(
        symbol=symbol,
        run_date=run_date,
        rows=len(df),
        expected=expected_minutes(run_date),
        first_minute=str(minutes.min()) if minutes is not None else None,
        last_minute=str(minutes.max()) if minutes is not None else None,
    )
