"""Settings and the symbol universe.

Everything configurable lives here so the DAG file stays about orchestration and the ingest
module stays about fetching. Nothing here reads a network or a database.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

# `data-api.binance.vision` is Binance's read-only public host for market data. It serves the
# same klines as api.binance.com but needs no key and is not subject to the same regional
# blocking, which matters because this runs from a cloud VM.
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://data-api.binance.vision")

#: One call returns at most 1000 candles, so a full day of 1-minute bars (1440) takes two.
KLINES_MAX_LIMIT = 1000
MINUTES_PER_DAY = 1440

#: Spacing between calls. Binance's published limit is far more generous than this, but a
#: scheduled backfill fires thousands of requests unattended and a 429 mid-run leaves a partial
#: day that looks like a complete one. Slow and finished beats fast and half-written.
REQUEST_SPACING_S = 0.25
MAX_RETRIES = 4
RETRY_BACKOFF_S = 2.0
HTTP_TIMEOUT_S = 30

#: The twenty pairs this project tracks.
#:
#: Chosen on 2026-08-13 from live 24-hour quote volume, then filtered twice. Every pair must
#: have traded for more than 120 days, because a symbol listed last month cannot be backfilled
#: 90 days and would leave holes that look like ingestion bugs. Pegged pairs (USDC, FDUSD,
#: RLUSD, USD1) were dropped despite high volume: a price series that never moves makes every
#: downstream aggregate meaningless while still costing storage and API budget.
#:
#: The mix is deliberate — large caps whose minutes are always populated, and thinner alt pairs
#: that genuinely have minutes with zero trades. Both cases have to be handled, and a universe
#: of only BTC and ETH would never surface the second one.
SYMBOLS: list[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "TRXUSDT", "LINKUSDT", "ZECUSDT",
    "WLDUSDT", "PLUMEUSDT", "PORTALUSDT", "ALLOUSDT", "TUTUSDT",
    "ACEUSDT", "XPLUSDT", "GPSUSDT", "CHIPUSDT", "PUMPUSDT",
]

#: Bronze layout in object storage. Partitioned by date first so a single day can be replaced
#: or re-read without touching the rest, and by symbol second so one symbol's backfill does not
#: rewrite another's.
BRONZE_PREFIX = "bronze/klines_1m"


def bronze_key(run_date: str, symbol: str) -> str:
    """Where one symbol-day lands. Hive-style so DuckDB and Spark both read the partitions."""
    return f"{BRONZE_PREFIX}/dt={run_date}/symbol={symbol}/part-0.parquet"


def local_bronze_path(run_date: str, symbol: str) -> Path:
    """Same layout on local disk, so a run without credentials still produces something real."""
    return DATA_DIR / bronze_key(run_date, symbol)
