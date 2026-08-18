"""Binance trade stream into Kafka. Receives and forwards, and does nothing else.

**Deliberately dumb.** Every transformation this program could do is a transformation that would
be wrong for the trades already published before the bug was found. Keeping it to receive-and-
forward means the raw feed in Kafka is always exactly what Binance sent, so a mistake in the
aggregator downstream costs a replay rather than a hole in the record.

The connection is one combined stream carrying all twenty symbols rather than twenty sockets:
Binance limits connections per address, and one socket that reconnects cleanly beats twenty that
each have their own way of dying.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass, field

import websockets
from confluent_kafka import Producer

from . import kafka_conf
from .config import SYMBOLS

logger = logging.getLogger(__name__)

WS_BASE = "wss://stream.binance.com:9443/stream"

#: Binance closes an idle connection after 24 hours whatever happens, so a reconnect is a normal
#: event rather than an error. Backoff is capped low because being down is worse than being
#: impolite: every second disconnected is a second of trades that cannot be recovered.
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0

#: Written on every message. A separate health check reads it to decide whether the stream has
#: gone quiet — which looks identical to a quiet market until you compare against the clock.
HEARTBEAT_PATH = "/tmp/bmp_ws_heartbeat"


@dataclass
class Stats:
    """Counted rather than logged per message: 82 a second would bury everything else."""

    received: int = 0
    published: int = 0
    failed: int = 0
    reconnects: int = 0
    started: float = field(default_factory=time.monotonic)

    def line(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        return (f"{self.received:,} received · {self.published:,} published · "
                f"{self.failed} failed · {self.reconnects} reconnects · "
                f"{self.received / elapsed:.0f}/s")


def stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return f"{WS_BASE}?streams={streams}"


def _on_delivery(stats: Stats):
    def cb(err, _msg):
        if err is None:
            stats.published += 1
        else:
            stats.failed += 1
            logger.error("delivery failed: %s", err)
    return cb


async def run(symbols: list[str] | None = None, duration_s: float | None = None) -> Stats:
    """Stream trades into Kafka until stopped, or for `duration_s` when testing."""
    symbols = symbols or SYMBOLS
    if not kafka_conf.is_configured():
        raise RuntimeError(f"Kafka is not configured; missing {kafka_conf.missing()}")

    producer = Producer(kafka_conf.producer_conf())
    stats = Stats()
    on_delivery = _on_delivery(stats)
    stop = asyncio.Event()
    deadline = time.monotonic() + duration_s if duration_s else None

    def _stop(*_):
        logger.info("stop requested")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
        except NotImplementedError:      # Windows has no add_signal_handler
            signal.signal(sig, _stop)

    backoff = RECONNECT_MIN_S
    url = stream_url(symbols)

    while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                          max_queue=4096) as ws:
                logger.info("connected: %d symbols", len(symbols))
                backoff = RECONNECT_MIN_S      # only after a connection actually succeeds

                while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        # Thirty seconds without a single trade across twenty pairs means the
                        # socket is dead, not that the market stopped. Ping keeps it honest;
                        # this catches the case where ping succeeds and data does not flow.
                        logger.warning("no messages for 30s — reconnecting")
                        break

                    payload = json.loads(raw).get("data")
                    if not payload:
                        continue

                    stats.received += 1
                    # Keyed by symbol so every trade for a pair lands on one partition and stays
                    # in order. Order within a symbol is what the one-minute windows depend on.
                    producer.produce(
                        kafka_conf.TOPIC_TRADES,
                        key=payload["s"].encode(),
                        value=raw.encode() if isinstance(raw, str) else raw,
                        callback=on_delivery,
                    )
                    producer.poll(0)

                    if stats.received % 500 == 0:
                        _touch_heartbeat(stats)
                        logger.info(stats.line())

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats.reconnects += 1
            logger.warning("stream error (%s), reconnecting in %.0fs",
                           type(exc).__name__, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    producer.flush(30)
    logger.info("final: %s", stats.line())
    return stats


def _touch_heartbeat(stats: Stats) -> None:
    try:
        with open(HEARTBEAT_PATH, "w") as fh:
            fh.write(f"{time.time():.0f} {stats.received}\n")
    except OSError:
        # A heartbeat that cannot be written must never take the stream down with it.
        logger.debug("could not write heartbeat", exc_info=True)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
