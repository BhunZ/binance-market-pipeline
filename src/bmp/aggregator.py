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

**Offsets are committed after a write, not before.** A crash between reading and writing then
replays those trades rather than losing them. The cost is that a window can be written twice —
harmless, because writing a partition replaces it with identical content.
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
from confluent_kafka import Consumer, KafkaError

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
        self.late: list[dict] = []
        # Minutes already written. A trade is late because its window is **gone**, not because
        # the clock has moved on — the distinction matters after any outage, when the backlog
        # replayed out of Kafka is entirely older than the grace period and yet none of it has
        # been written. Judging by the clock filed 120,919 trades as late and built no bars at
        # all for the twenty minutes the aggregator had been crash-looping.
        self.written: set[tuple[str, dt.datetime]] = set()
        self.stats = Stats()

    def ingest(self, payload: dict, now: float | None = None) -> None:
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

        if key not in self.windows and key in self.written:
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
        self.stats.consumed += 1

    def due(self, now: float | None = None) -> list[tuple[str, dt.datetime, Bar]]:
        """Windows whose minute ended more than the grace period ago."""
        cutoff = (now if now is not None else time.time()) - LATENESS_GRACE_S
        return [(sym, minute, bar) for (sym, minute), bar in self.windows.items()
                if (minute + dt.timedelta(minutes=1)).timestamp() <= cutoff]

    def flush(self, now: float | None = None) -> int:
        """Write every due window, then forget it. Returns windows written."""
        ready = self.due(now)
        written = 0

        for symbol, minute, bar in ready:
            objstore.write_partition(
                key=trades_key(minute, symbol),
                payload=to_parquet_bytes([bar.as_row(symbol, minute)]),
                local_path=DATA_DIR / trades_key(minute, symbol),
            )
            del self.windows[(symbol, minute)]
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

    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("consume error: %s", msg.error())
                else:
                    try:
                        agg.ingest(json.loads(msg.value()))
                    except json.JSONDecodeError:
                        agg.stats.malformed += 1

            if time.monotonic() - last_flush >= FLUSH_INTERVAL_S:
                if agg.flush():
                    # Commit only after the windows are on object storage. A crash before this
                    # replays those trades; a crash after would have lost them.
                    consumer.commit(asynchronous=False)
                    logger.info(agg.stats.line())
                last_flush = time.monotonic()
    finally:
        agg.flush()
        try:
            consumer.commit(asynchronous=False)
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
