"""The speed layer's only logic, and the two things it got wrong in production.

**Lateness meant the wrong thing.** The first version called a trade late when the clock had moved
past its window plus a grace period. Then the aggregator crash-looped for twenty minutes while the
consumer kept publishing; on recovery it read 130,000 messages out of Kafka, judged every one of
them stale, and filed 120,919 as late — building no bars at all for the period it had been down.
None of those minutes had been written. Lateness has to mean *the window is gone*, which is the
only definition that survives a restart.

**A clean restart lost two thirds of a minute.** Kafka keeps one offset per partition, so
committing after writing the minute that closed also committed past the trades held in the
minute that had not. The restart at 07:06:36 resumed inside 07:06 and rebuilt it from the tail:
all twenty symbols light, no error raised, every bar plausible. Only the comparison against
Binance's own candle for that minute showed it.

**Binance's maker flag reads backwards.** `m` marks whether the **buyer** was the maker, so taker
buy volume is the trades where it is false. Getting it the intuitive way round inverts the single
most-used derived measure in the warehouse, and nothing about the output looks wrong.
"""

import datetime as dt
import time

import pytest

from bmp import aggregator as agg


def trade(price=100.0, qty=1.0, buyer_is_maker=False, event_ms=None, trade_id=1, symbol="BTCUSDT"):
    return {"data": {"s": symbol, "p": str(price), "q": str(qty),
                     "T": event_ms if event_ms is not None else int(time.time() * 1000),
                     "t": trade_id, "m": buyer_is_maker}}


# --- the bar arithmetic ---------------------------------------------------------------

def test_open_is_the_first_trade_and_close_the_last():
    bar = agg.Bar()
    for i, price in enumerate([100, 110, 90], start=1):
        bar.add(price, 1.0, False, i)
    row = bar.as_row("BTCUSDT", agg.minute_of(0))
    assert (row["open"], row["high"], row["low"], row["close"]) == (100, 110, 90, 90)


def test_taker_buy_volume_counts_trades_where_the_buyer_was_not_the_maker():
    """Binance's `m` is true when the BUYER was the maker, so a taker *buy* has m = false.
    Reading it the intuitive way inverts taker_buy_ratio everywhere downstream."""
    bar = agg.Bar()
    bar.add(100, 1.0, False, 1)   # taker bought
    bar.add(100, 2.0, True, 2)    # taker sold
    bar.add(100, 3.0, False, 3)   # taker bought

    row = bar.as_row("BTCUSDT", agg.minute_of(0))
    assert row["volume"] == 6.0
    assert row["taker_base"] == 4.0


def test_an_empty_bar_never_reaches_the_output():
    """high starts at -inf and low at +inf, which would be written verbatim for a bar that
    received nothing. Only bars that received a trade are ever created."""
    assert agg.Bar().trades == 0


def test_trade_ids_bracket_the_window():
    bar = agg.Bar()
    bar.add(100, 1.0, False, 500)
    bar.add(101, 1.0, False, 900)
    row = bar.as_row("BTCUSDT", agg.minute_of(0))
    assert (row["first_trade_id"], row["last_trade_id"]) == (500, 900)


# --- which minute a trade belongs to ---------------------------------------------------

def test_the_minute_comes_from_the_exchange_timestamp_not_arrival():
    """A message delayed by the network still belongs to the minute it was traded in. Using
    arrival time would smear every delay into the wrong bar."""
    event_ms = int(dt.datetime(2026, 8, 14, 12, 34, 56, tzinfo=dt.UTC).timestamp() * 1000)
    assert agg.minute_of(event_ms) == dt.datetime(2026, 8, 14, 12, 34, tzinfo=dt.UTC)


def test_minutes_are_truncated_in_utc():
    minute = agg.minute_of(int(dt.datetime(2026, 8, 14, 0, 30, tzinfo=dt.UTC).timestamp() * 1000))
    assert minute.tzinfo == dt.UTC and minute.second == 0 and minute.microsecond == 0


# --- lateness -------------------------------------------------------------------------

def test_a_backlog_replayed_after_an_outage_builds_bars():
    """The regression test for 120,919 trades filed as late. Twenty-minute-old trades whose
    windows have never been written are not late — they are a backlog, and the whole point of
    putting the raw feed through a broker is that a backlog can still be processed."""
    a = agg.Aggregator()
    now = time.time()

    for i in range(5):
        a.ingest(trade(event_ms=int((now - 1200 + i) * 1000), trade_id=i), now=now)

    assert a.stats.consumed == 5
    assert a.stats.late == 0


def test_a_trade_for_a_window_already_written_is_late():
    a = agg.Aggregator()
    now = time.time()
    event_ms = int((now - 1200) * 1000)

    a.ingest(trade(event_ms=event_ms, trade_id=1), now=now)
    a.written.add(("BTCUSDT", agg.minute_of(event_ms)))
    a.windows.clear()
    a.ingest(trade(event_ms=event_ms, trade_id=2), now=now)

    assert a.stats.late == 1


def test_late_trades_are_recorded_rather_than_dropped():
    """A dropped trade makes the stream disagree with the batch layer and takes its own
    explanation with it. Recorded, the same difference arrives with its cause attached."""
    a = agg.Aggregator()
    now = time.time()
    event_ms = int((now - 600) * 1000)
    a.written.add(("BTCUSDT", agg.minute_of(event_ms)))

    a.ingest(trade(event_ms=event_ms, trade_id=7), now=now)

    assert len(a.late) == 1
    assert a.late[0]["trade_id"] == 7
    assert a.late[0]["lateness_s"] == pytest.approx(600, abs=5)


# --- when a window may be written ------------------------------------------------------

def test_a_window_is_not_written_while_its_minute_can_still_receive_trades():
    a = agg.Aggregator()
    now = time.time()
    a.ingest(trade(event_ms=int(now * 1000)), now=now)
    assert a.due() == []


def test_a_window_closes_once_the_data_has_moved_past_it():
    """The trigger is the newest exchange timestamp seen, not the clock."""
    a = agg.Aggregator()
    base = int(dt.datetime(2026, 8, 18, 7, 5, 0, tzinfo=dt.UTC).timestamp() * 1000)

    a.ingest(trade(event_ms=base))
    assert a.due() == []

    a.ingest(trade(event_ms=base + 66_000, symbol="ETHUSDT"))
    assert [minute for _, minute, _ in a.due()] == [agg.minute_of(base)]


def test_a_replaying_backlog_does_not_close_windows_the_clock_has_run_past():
    """The other half of the 07:06 fault. On restart the backlog arrives from Kafka in seconds
    and by the clock every minute in it is long overdue. Closing on the clock writes the window
    currently being rebuilt when it is a third full, then rejects the rest of its own trades as
    late — reproducing the exact damage the restart was supposed to repair."""
    a = agg.Aggregator()
    # Half an hour behind the clock, and aligned so all thirty trades sit inside one minute:
    # the window stays open because the *data* has not passed it, however late the clock is.
    start = dt.datetime.fromtimestamp(time.time() - 1800, dt.UTC).replace(second=0, microsecond=0)
    base = int(start.timestamp() * 1000)

    for i in range(30):
        a.ingest(trade(event_ms=base + i * 1000, trade_id=i))

    assert a.due() == [], "khong duoc dong cua so khi du lieu chua di qua no"
    assert a.stats.consumed == 30
    assert a.stats.late == 0


def test_a_trade_for_a_minute_the_data_has_passed_is_late_even_in_a_fresh_process():
    """`written` dies with the process. Without an event-time check, a replayed trade for a
    minute already written would rebuild that minute from its tail and overwrite the whole bar
    with the fragment."""
    a = agg.Aggregator()
    base = dt.datetime(2026, 8, 18, 7, 6, tzinfo=dt.UTC)

    a.ingest(trade(event_ms=int((base + dt.timedelta(minutes=5)).timestamp() * 1000)))
    a.ingest(trade(event_ms=int(base.timestamp() * 1000), trade_id=99))

    assert a.stats.late == 1
    assert not a.written, "phat hien duoc ma khong can nho gi tu tien trinh truoc"


# --- offsets ---------------------------------------------------------------------------

def test_an_offset_is_never_committed_past_a_window_still_open():
    """The defect itself. The read position is past every trade handed over, including the ones
    in windows not yet written; committing it makes a restart resume inside a minute."""
    a = agg.Aggregator()
    base = int(dt.datetime(2026, 8, 18, 7, 5, 0, tzinfo=dt.UTC).timestamp() * 1000)

    a.ingest(trade(event_ms=base, trade_id=1), partition=0, offset=100)
    a.ingest(trade(event_ms=base + 61_000, trade_id=2), partition=0, offset=101)

    assert a.safe_offsets({0: 102}) == {0: 100}


def test_a_written_window_stops_holding_its_offset_back():
    a = agg.Aggregator()
    base = int(dt.datetime(2026, 8, 18, 7, 5, 0, tzinfo=dt.UTC).timestamp() * 1000)

    a.ingest(trade(event_ms=base, trade_id=1), partition=0, offset=100)
    a.ingest(trade(event_ms=base + 130_000, trade_id=2), partition=0, offset=101)
    a.flush()

    assert a.safe_offsets({0: 102}) == {0: 101}, "chi con cua so 07:07 giu offset lai"


def test_each_partition_is_held_back_independently():
    """One partition lagging must not rewind another, or every restart replays the whole topic."""
    a = agg.Aggregator()
    base = int(dt.datetime(2026, 8, 18, 7, 5, 0, tzinfo=dt.UTC).timestamp() * 1000)

    a.ingest(trade(event_ms=base, symbol="BTCUSDT"), partition=0, offset=50)
    a.ingest(trade(event_ms=base, symbol="ETHUSDT"), partition=1, offset=900)

    assert a.safe_offsets({0: 51, 1: 901}) == {0: 50, 1: 900}


def test_a_partition_with_nothing_open_commits_its_read_position():
    a = agg.Aggregator()
    assert a.safe_offsets({0: 77, 1: 88}) == {0: 77, 1: 88}


def test_the_grace_period_is_long_enough_to_be_useful_and_short_enough_to_bound_memory():
    assert 1 <= agg.LATENESS_GRACE_S <= 30


# --- malformed input --------------------------------------------------------------------

@pytest.mark.parametrize("payload", [
    {}, {"data": {}}, None, {"data": {"s": "X", "p": "not-a-number", "q": "1",
                                      "T": 1, "t": 1, "m": True}},
])
def test_a_malformed_message_is_counted_and_does_not_stop_the_stream(payload):
    """One bad message must never take down a consumer that has to stay up: Binance serves no
    history for the trade feed, so every second down is unrecoverable."""
    a = agg.Aggregator()
    a.ingest(payload, now=time.time())
    assert a.stats.malformed == 1
    assert a.stats.consumed == 0


# --- partition layout --------------------------------------------------------------------

def test_bars_use_the_same_hive_layout_as_the_batch_layer():
    """One glob has to read either branch, so the partition keys must match."""
    minute = dt.datetime(2026, 8, 14, 6, 44, tzinfo=dt.UTC)
    key = agg.trades_key(minute, "BTCUSDT")
    assert key.startswith("bronze/trades_1m/dt=2026-08-14/symbol=BTCUSDT/")


def test_late_arrivals_are_kept_apart_from_the_bars():
    """They are evidence about the stream, not measurements of the market. Mixing them into the
    bars would corrupt the very comparison they exist to explain."""
    minute = dt.datetime(2026, 8, 14, 6, 44, tzinfo=dt.UTC)
    assert agg.LATE_PREFIX not in agg.trades_key(minute, "BTCUSDT")
    assert agg.late_key(minute, "BTCUSDT").startswith(agg.LATE_PREFIX)
