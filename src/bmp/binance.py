"""HTTP client for Binance public market data.

Only two things happen here: pacing and retry classification. Everything about what the data
means belongs in `klines.py`.

**Why retries are classified.** A 429 means slow down and try again; a 400 means the request was
wrong and will be wrong every time. Retrying the second kind wastes the rate budget that the
first kind needs, and turns a clear error into a slow one. The same distinction is why an
exhausted quota must never look like an empty result.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import (BINANCE_BASE, HTTP_TIMEOUT_S, MAX_RETRIES, REQUEST_SPACING_S,
                     RETRY_BACKOFF_S)

logger = logging.getLogger(__name__)


class BinanceError(RuntimeError):
    """A request failed in a way retrying will not fix."""


class RateLimited(BinanceError):
    """Binance asked us to slow down. Distinct because it is the one worth waiting out."""


_last_call = 0.0


def _pace() -> None:
    """Keep a floor between calls.

    Module-level rather than per-client on purpose: the limit belongs to the API key and the
    source address, not to whichever object happens to hold it. Two clients in one process must
    share one budget.
    """
    global _last_call
    wait = REQUEST_SPACING_S - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def get(path: str, params: dict[str, Any] | None = None) -> Any:
    """One GET against the public API, paced and retried. Returns the decoded JSON.

    Raises `BinanceError` when the request is wrong, and only retries what is worth retrying:
    429 and 418 (rate limited), 5xx (their side), and connection errors. A 4xx of any other kind
    fails immediately with the body attached, because that body is the only thing that explains
    a malformed request.
    """
    url = f"{BINANCE_BASE}{path}"
    last: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        _pace()
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT_S)
        except requests.RequestException as exc:
            last = exc
            logger.warning("%s: %s (attempt %d/%d)", path, type(exc).__name__, attempt,
                           MAX_RETRIES)
            time.sleep(RETRY_BACKOFF_S * attempt)
            continue

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 418):
            # Binance sends Retry-After on 418 and sometimes on 429. Honour it — guessing a
            # shorter wait is how a soft throttle becomes an IP ban.
            wait = float(r.headers.get("Retry-After", RETRY_BACKOFF_S * attempt * 2))
            logger.warning("rate limited on %s, waiting %.0fs (attempt %d/%d)", path, wait,
                           attempt, MAX_RETRIES)
            last = RateLimited(f"{r.status_code} on {path}")
            time.sleep(wait)
            continue

        if 500 <= r.status_code < 600:
            last = BinanceError(f"{r.status_code} on {path}")
            time.sleep(RETRY_BACKOFF_S * attempt)
            continue

        raise BinanceError(f"{r.status_code} on {path}: {r.text[:200]}")

    raise BinanceError(f"{path} failed after {MAX_RETRIES} attempts: {last}")


def ping() -> bool:
    """Is the API answering? Used by the DAG before it spends a run on a dead endpoint."""
    try:
        get("/api/v3/ping")
        return True
    except BinanceError:
        return False


def server_time_ms() -> int:
    return int(get("/api/v3/time")["serverTime"])


def exchange_symbols() -> set[str]:
    """Symbols currently open for spot trading.

    The DAG checks its universe against this before a run: a delisted symbol returns an empty
    kline list, which is indistinguishable from a day with no trades unless you ask first.
    """
    info = get("/api/v3/exchangeInfo")
    return {
        s["symbol"] for s in info["symbols"]
        if s["status"] == "TRADING" and s.get("isSpotTradingAllowed")
    }


def klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int) -> list[list]:
    """Raw candles in [start_ms, end_ms). Binance returns them oldest first."""
    return get("/api/v3/klines", {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    })
