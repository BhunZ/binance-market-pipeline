"""Trades from Kafka into one-minute candles on object storage.

This is the speed layer's only piece of logic, and it computes the same thing the batch layer
fetches ready-made from Binance. That duplication is the point: two independent paths to one
number is the only way to find out whether the streaming path is right.

**Windows close late, on purpose.** A trade's minute comes from its own exchange timestamp, not
from when it arrived, so a message delayed by the network still lands in the minute it belongs
to. A window is therefore held open for `LATENESS_GRACE_S` after the minute ends, and only then
written.

**Trades later than the grace period are recorded, never dropped.** They go to a separate
`late_arrivals` partition. Discarding them would make the stream quietly disagree with the batch
layer, and the reconciliation would report a difference with no way to explain it. Counted, the
same difference arrives with its own cause attached.

**An offset is never committed past a window still open.** Committing the read position looks
right and is not: Kafka keeps one offset per partition, so committing because the minute that
just closed was written also commits past the trades sitting in the minute that has not. A
restart then resumes *inside* a minute and rebuilds it from whatever was left — which is how a
clean restart at 07:06:36 left all twenty symbols holding about a third of that minute, with no
error anywhere and a bar that looked entirely plausible. The reconciliation against Binance's
own candles is what found it. So the commit is held back to the oldest offset feeding an open
window, and a restart replays that minute from its first trade.

**Windows close on event time, not on the clock.** After an outage the backlog comes out of
Kafka in seconds, and by the clock every minute in it is long overdue — so a flush landing
mid-replay would write a half-built window and then reject the rest of its own trades as late.
The trigger is the watermark: the newest exchange timestamp seen, which advances with the data
instead of running ahead of it.
"""

from __future__ import annotations

import collections
import datetime as dt
import io
import json
import logging
import signal
import time
from dataclasses import dataclass, field

import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition

from . import kafka_conf, objstore
from .config import DATA_DIR

logger = logging.getLogger(__name__)

TRADES_PREFIX = "bronze/trades_1m"
LATE_PREFIX = "bronze/late_arrivals"

#: How long a minute stays open after it ends. Measured against Binance's own event time, so this
#: is tolerance for network and broker delay, not for clock skew. Five seconds is generous at
#: ~82 messages a second and still bounds memory to a handful of windows.
LATENESS_GRACE_S = 5.0

#: How often to check for windows ready to close, and write whatever is due.
FLUSH_INTERVAL_S = 10.0

#: Written after every flush, carrying the cumulative window count. The count is the part that
#: matters: a timestamp alone only proves the process is alive, and a process that is alive and
#: building nothing is the failure worth catching. `bmp.health` compares it against its previous
#: reading.
HEARTBEAT_PATH = "/tmp/bmp_agg_heartbeat"


@dataclass
class Bar:
    """One symbol-minute under construction.

    Accumulated rather than stored: keeping every trade in memory to compute an average at the
    end would hold tens of thousands of dicts per minute for a number four counters can carry.
    """

    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0
    quote_volume: float = 0.0
    trades: int = 0
    taker_base: float = 0.0
    taker_quote: float = 0.0
    first_trade_id: int | None = None
    last_trade_id: int | None = None

    def add(self, price: float, qty: float, is_buyer_maker: bool, trade_id: int) -> None:
        if self.trades == 0:
            self.open = price
            self.first_trade_id = trade_id
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += qty
        self.quote_volume += price * qty
        self.trades += 1
        self.last_trade_id = trade_id
        # Binance's flag marks whether the *buyer* was the maker. Taker buy volume is therefore
        # the trades where the buyer was NOT the maker — the inverse of how it first reads, and
        # the single easiest field in this feed to get backwards.
        if not is_buyer_maker:
            self.taker_base += qty
            self.taker_quote += price * qty

    def as_row(self, symbol: str, minute: dt.datetime) -> dict:
        return {
            "symbol": symbol,
            "minute": minute,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trades": self.trades,
            "taker_base": self.taker_base,
            "taker_quote": self.taker_quote,
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
        }


@dataclass
class Stats:
    consumed: int = 0
    windows_written: int = 0
    late: int = 0
    malformed: int = 0
    started: float = field(default_factory=time.monotonic)

    def line(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        return (f"{self.consumed:,} trades - {self.windows_written} windows - "
                f"{self.late} late - {self.malformed} malformed - "
                f"{self.consumed / elapsed:.0f}/s")


def minute_of(event_ms: int) -> dt.datetime:
    """The UTC minute a trade belongs to, truncated from its own exchange timestamp."""
    ts = dt.datetime.fromtimestamp(event_ms / 1000, dt.UTC)
    return ts.replace(second=0, microsecond=0)


def trades_key(minute: dt.datetime, symbol: str) -> str:
    """Same Hive layout as the batch layer, under a different prefix, so one glob reads either."""
    return (f"{TRADES_PREFIX}/dt={minute:%Y-%m-%d}/symbol={symbol}/"
            f"part-{minute:%H%M}.parquet")


def late_key(minute: dt.datetime, symbol: str) -> str:
    return f"{LATE_PREFIX}/dt={minute:%Y-%m-%d}/symbol={symbol}/part-{minute:%H%M}.parquet"


def to_parquet_bytes(rows: list[dict]) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False, compression="zstd")
    return buf.getvalue()


class Aggregator:
    """Holds open windows and writes them once they can no longer receive trades."""

    def __init__(self) -> None:
        # (symbol, minute) -> Bar. Bounded by the grace period: at most a couple of minutes of
        # windows for twenty symbols, so a few dozen objects rather than a growing buffer.
        self.windows: dict[tuple[str, dt.datetime], Bar] = {}
        # Per open window, the lowest Kafka offset per partition that fed it. This is what the
        # commit is clamped to, so no trade in an unwritten window is ever marked consumed.
        self.offsets: dict[tuple[str, dt.datetime], dict[int, int]] = {}
        #: Newest exchange timestamp seen. Everything closes against this rather than the clock.
        self.watermark: dt.datetime | None = None
        self.late: list[dict] = []
        # Minutes already written. A trade is late because its window is **gone**, not because
        # the clock has moved on — the distinction matters after any outage, when the backlog
        # replayed out of Kafka is entirely older than the grace period and yet none of it has
        # been written. Judging by the clock filed 120,919 trades as late and built no bars at
        # all for the twenty minutes the aggregator had been crash-looping.
        self.written: set[tuple[str, dt.datetime]] = set()
        self.stats = Stats()

    def ingest(self, payload: dict, now: float | None = None,
               partition: int | None = None, offset: int | None = None) -> None:
        """One trade into its window, or into the late pile if that window has already gone."""
        now = now if now is not None else time.time()
        try:
            data = payload.get("data", payload)
            symbol = data["s"]
            price = float(data["p"])
            qty = float(data["q"])
            event_ms = int(data["T"])
            trade_id = int(data["t"])
            is_buyer_maker = bool(data["m"])
        except (KeyError, TypeError, ValueError, AttributeError):
            self.stats.malformed += 1
            return

        minute = minute_of(event_ms)
        key = (symbol, minute)

        event_time = dt.datetime.fromtimestamp(event_ms / 1000, dt.UTC)
        if self.watermark is None or event_time > self.watermark:
            self.watermark = event_time

        # Late means the window is gone, by either of the two ways it can go. `written` covers
        # this process; the watermark covers the one before it, whose `written` set died with it
        # and whose replayed trades would otherwise rebuild a finished minute from its tail
        # alone and overwrite the complete bar with it.
        gone = key in self.written or (self.horizon() is not None
                                       and minute + dt.timedelta(minutes=1) <= self.horizon())

        if key not in self.windows and gone:
            self.stats.late += 1
            self.late.append({
                "symbol": symbol,
                "minute": minute,
                "trade_id": trade_id,
                "price": price,
                "qty": qty,
                "event_time_ms": event_ms,
                "lateness_s": (now * 1000 - event_ms) / 1000,
            })
            return

        self.windows.setdefault(key, Bar()).add(price, qty, is_buyer_maker, trade_id)
        if partition is not None and offset is not None:
            seen = self.offsets.setdefault(key, {})
            seen[partition] = min(seen.get(partition, offset), offset)
        self.stats.consumed += 1

    def horizon(self) -> dt.datetime | None:
        """The instant below which a minute is considered finished.

        Derived from the data, not the clock. `None` until the first trade, because nothing can
        be known to be complete before anything has been seen.
        """
        if self.watermark is None:
            return None
        return self.watermark - dt.timedelta(seconds=LATENESS_GRACE_S)

    def due(self) -> list[tuple[str, dt.datetime, Bar]]:
        """Windows the data has moved past.

        Event time, deliberately: while a backlog replays, the clock is minutes ahead of the
        trades coming out of Kafka, and closing a window because the clock says so writes it
        half-built and discards the rest.
        """
        horizon = self.horizon()
        if horizon is None:
            return []
        return [(sym, minute, bar) for (sym, minute), bar in self.windows.items()
                if minute + dt.timedelta(minutes=1) <= horizon]

    def safe_offsets(self, position: dict[int, int]) -> dict[int, int]:
        """Where each partition may be committed to: the read position, held back to the oldest
        offset still feeding a window that has not been written."""
        safe = dict(position)
        for seen in self.offsets.values():
            for part, off in seen.items():
                if part in safe:
                    safe[part] = min(safe[part], off)
        return safe

    def flush(self) -> int:
        """Write every due window, then forget it. Returns windows written."""
        ready = self.due()
        written = 0

        for symbol, minute, bar in ready:
            objstore.write_partition(
                key=trades_key(minute, symbol),
                payload=to_parquet_bytes([bar.as_row(symbol, minute)]),
                local_path=DATA_DIR / trades_key(minute, symbol),
            )
            del self.windows[(symbol, minute)]
            self.offsets.pop((symbol, minute), None)
            self.written.add((symbol, minute))
            written += 1

        # The written set is only needed for as long as a trade could still arrive for a minute.
        # Two hours is far beyond any plausible delay and keeps the set to a few thousand entries
        # instead of growing for the life of the process.
        if len(self.written) > 20_000:
            cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
            self.written = {(s, m) for s, m in self.written if m >= cutoff}

        if self.late:
            grouped: dict[tuple[str, dt.datetime], list[dict]] = collections.defaultdict(list)
            for row in self.late:
                grouped[(row["symbol"], row["minute"])].append(row)
            for (symbol, minute), rows in grouped.items():
                objstore.write_partition(
                    key=late_key(minute, symbol),
                    payload=to_parquet_bytes(rows),
                    local_path=DATA_DIR / late_key(minute, symbol),
                )
            self.late.clear()

        self.stats.windows_written += written
        return written


def commit_safely(consumer: Consumer, agg: "Aggregator", position: dict[int, int]) -> None:
    """Commit each partition no further than the oldest trade still held in an open window.

    `consumer.commit()` with no arguments commits the read position, which is past every trade
    already handed to the aggregator — including the ones in windows that have not been written.
    """
    safe = agg.safe_offsets(position)
    if not safe:
        return
    consumer.commit(offsets=[TopicPartition(kafka_conf.TOPIC_TRADES, part, off)
                             for part, off in safe.items()],
                    asynchronous=False)


def touch_heartbeat(stats: Stats, path: str = HEARTBEAT_PATH) -> None:
    try:
        with open(path, "w") as fh:
            fh.write(f"{time.time():.0f} {stats.windows_written}\n")
    except OSError:
        # A heartbeat that cannot be written must never take the aggregator down with it.
        logger.debug("could not write heartbeat", exc_info=True)


def run(group_id: str = "aggregator", duration_s: float | None = None) -> Stats:
    """Consume trades and write minute bars until stopped."""
    if not kafka_conf.is_configured():
        raise RuntimeError(f"Kafka is not configured; missing {kafka_conf.missing()}")

    consumer = Consumer(kafka_conf.consumer_conf(group_id))
    consumer.subscribe([kafka_conf.TOPIC_TRADES])

    agg = Aggregator()
    stopping = False
    deadline = time.monotonic() + duration_s if duration_s else None

    def _stop(*_):
        nonlocal stopping
        logger.info("stop requested")
        stopping = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _stop)

    last_flush = time.monotonic()
    # Next offset to read per partition. Kafka commits the position to resume *from*, so it is
    # the offset just consumed plus one.
    position: dict[int, int] = {}

    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("consume error: %s", msg.error())
                else:
                    try:
                        agg.ingest(json.loads(msg.value()),
                                   partition=msg.partition(), offset=msg.offset())
                    except json.JSONDecodeError:
                        agg.stats.malformed += 1
                    position[msg.partition()] = msg.offset() + 1

            if time.monotonic() - last_flush >= FLUSH_INTERVAL_S:
                touch_heartbeat(agg.stats)
                if agg.flush():
                    # Only after the windows are on object storage, and only up to the oldest
                    # trade still held open. A crash before this replays those trades; a crash
                    # after would have lost them.
                    commit_safely(consumer, agg, position)
                    logger.info(agg.stats.line())
                last_flush = time.monotonic()
    finally:
        # Whatever is still open stays unwritten on purpose. Its offsets were never committed,
        # so the next process replays those minutes from their first trade and builds them whole
        # — where writing them here would write a fraction of a minute and call it a candle.
        agg.flush()
        try:
            commit_safely(consumer, agg, position)
        except Exception:
            logger.warning("final commit failed - those trades will be replayed", exc_info=True)
        consumer.close()

    logger.info("final: %s", agg.stats.line())
    return agg.stats


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
