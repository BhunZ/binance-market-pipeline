"""The day boundary, and why it is the only thing in this module worth guarding hard.

The first working version of `fetch_day` returned **1441 candles**. Binance's `endTime` includes
the candle opening exactly at that instant, so asking for midnight-to-midnight returned the whole
day plus the first minute of the next one.

Nothing about that is visible in a single partition: 1441 rows in a file nobody counts looks
exactly like 1440. It becomes a duplicate key only when two partitions are read together, which
is downstream, in a fact table, weeks later — and by then it is one duplicated minute per symbol
per day across the entire backfill.

So the boundary gets three tests: the count, the last minute, and the non-overlap. The third is
the one that actually states the invariant.
"""

import datetime as dt

import pandas as pd
import pytest

from bmp import klines
from bmp.config import MINUTES_PER_DAY


# --- the day boundary -----------------------------------------------------------------

def test_the_day_ends_at_the_last_minute_not_the_next_midnight():
    start, end = klines.day_bounds_ms("2026-08-14")

    assert dt.datetime.fromtimestamp(start / 1000, dt.UTC) == \
        dt.datetime(2026, 8, 14, 0, 0, tzinfo=dt.UTC)
    assert dt.datetime.fromtimestamp(end / 1000, dt.UTC) == \
        dt.datetime(2026, 8, 14, 23, 59, 59, 999000, tzinfo=dt.UTC)


def test_the_window_is_exactly_one_day_minus_a_millisecond():
    start, end = klines.day_bounds_ms("2026-08-14")
    assert end - start == 86_400_000 - 1


def test_consecutive_days_do_not_share_an_instant():
    """The invariant the 1441-candle bug broke: no millisecond belongs to two partitions."""
    _, end_first = klines.day_bounds_ms("2026-08-14")
    start_second, _ = klines.day_bounds_ms("2026-08-15")
    assert end_first < start_second


def test_the_boundary_is_utc_not_local_time():
    """A partition keyed on local dates would hold 1440 minutes that are not that date's minutes
    anywhere else — a seven-hour offset nobody can explain months later."""
    start, _ = klines.day_bounds_ms("2026-08-14")
    assert dt.datetime.fromtimestamp(start / 1000, dt.UTC).hour == 0


# --- how many minutes a date should have ----------------------------------------------

def test_a_past_day_expects_a_full_day():
    now = dt.datetime(2026, 8, 20, 9, 30, tzinfo=dt.UTC)
    assert klines.expected_minutes("2026-08-14", now=now) == MINUTES_PER_DAY


def test_today_expects_only_the_minutes_that_have_happened():
    """Demanding 1440 from a day still in progress would fail every run before midnight."""
    now = dt.datetime(2026, 8, 20, 9, 30, tzinfo=dt.UTC)
    assert klines.expected_minutes("2026-08-20", now=now) == 9 * 60 + 30


def test_a_future_day_expects_nothing():
    now = dt.datetime(2026, 8, 20, 9, 30, tzinfo=dt.UTC)
    assert klines.expected_minutes("2026-08-21", now=now) == 0


# --- what a completed day reports ------------------------------------------------------

def _frame(n: int, symbol: str = "BTCUSDT") -> pd.DataFrame:
    base = dt.datetime(2026, 8, 14, tzinfo=dt.UTC)
    return pd.DataFrame({
        "open_time": [int((base + dt.timedelta(minutes=i)).timestamp() * 1000) for i in range(n)],
        "minute": [base + dt.timedelta(minutes=i) for i in range(n)],
        "symbol": [symbol] * n,
        "trades": [0] * n,
    })


def test_a_full_day_reports_complete():
    now = dt.datetime(2026, 8, 20, tzinfo=dt.UTC)
    result = klines.summarise(_frame(MINUTES_PER_DAY), "BTCUSDT", "2026-08-14")
    assert result.complete and result.missing == 0


def test_a_short_day_reports_how_many_minutes_are_gone():
    result = klines.summarise(_frame(1400), "BTCUSDT", "2026-08-14")
    assert not result.complete
    assert result.missing == 40


def test_an_empty_day_summarises_without_crashing():
    """A delisted symbol returns nothing. That has to produce a report, not a traceback, or the
    task fails for the wrong reason and the log says the wrong thing."""
    result = klines.summarise(pd.DataFrame(), "GONEUSDT", "2026-08-14")
    assert result.rows == 0 and result.first_minute is None


# --- Bronze keeps the source's own values ----------------------------------------------

def test_prices_are_not_cast_on_the_way_in():
    """Bronze stores what Binance sent. Casting here would round a value before anyone has looked
    at it, and the string is the evidence. Typing belongs in the next layer, where a bad cast is
    visible and reversible."""
    assert "open" in klines.KLINE_FIELDS
    assert "close_time" in klines.DROP_FIELDS and "ignore" in klines.DROP_FIELDS


def test_the_field_names_are_declared_not_positional():
    """Binance returns twelve positional fields. Naming them once is the only thing standing
    between this project and a silent column shift if the API ever reorders them."""
    assert len(klines.KLINE_FIELDS) == 12
    assert klines.KLINE_FIELDS[:6] == ["open_time", "open", "high", "low", "close", "volume"]
