"""Daily snapshot of symbol metadata — the source of truth for the symbol dimension.

Candles say what a pair traded at. This says what the pair *is*: whether it is still open for
trading, the smallest price increment, the smallest order size. Those change — Binance retunes
tick and step sizes as a price moves through an order of magnitude, and a pair can be suspended
or delisted outright.

That is what makes a Type 2 dimension worth building rather than a schema decoration. Without
this, `dim_symbol` has no attribute that can change, and `valid_from` / `valid_to` would be
columns describing history that does not exist.

One snapshot per day, keyed by date like every other Bronze partition, so the same idempotence
holds: re-running a date replaces that date and nothing else.
"""

from __future__ import annotations

import io
import logging

import pandas as pd

from . import binance
from .config import SYMBOLS

logger = logging.getLogger(__name__)

BRONZE_PREFIX = "bronze/exchange_info"

#: Pulled out of the nested `filters` array into flat columns. These are the values that move,
#: and a dimension row is worth versioning only when one of them does.
_FILTER_FIELDS = {
    "PRICE_FILTER": ["tickSize", "minPrice", "maxPrice"],
    "LOT_SIZE": ["stepSize", "minQty", "maxQty"],
    "NOTIONAL": ["minNotional", "maxNotional"],
}


def bronze_key(run_date: str) -> str:
    """One object per day. No symbol partition — the whole universe fits in a few kilobytes."""
    return f"{BRONZE_PREFIX}/dt={run_date}/part-0.parquet"


def _flatten(entry: dict) -> dict:
    """One symbol's record, with the filters it actually has lifted to the top level.

    A missing filter yields None rather than being omitted, so every row has the same columns.
    Ragged rows would make the Parquet schema depend on which symbols happened to be listed that
    day, and two days would then fail to read together.
    """
    row = {
        "symbol": entry["symbol"],
        "status": entry["status"],
        "base_asset": entry["baseAsset"],
        "quote_asset": entry["quoteAsset"],
        "base_precision": entry.get("baseAssetPrecision"),
        "quote_precision": entry.get("quotePrecision"),
        "spot_trading_allowed": entry.get("isSpotTradingAllowed"),
        "margin_trading_allowed": entry.get("isMarginTradingAllowed"),
    }
    by_type = {f["filterType"]: f for f in entry.get("filters", [])}
    for filter_type, fields in _FILTER_FIELDS.items():
        found = by_type.get(filter_type, {})
        for field in fields:
            # camelCase to snake_case, because everything else in this warehouse is snake_case
            # and a single camelCase column is a paper cut in every query that touches it.
            name = "".join("_" + c.lower() if c.isupper() else c for c in field)
            row[name] = found.get(field)
    return row


def fetch(run_date: str) -> pd.DataFrame:
    """Metadata for the configured universe, as of now.

    Binance serves only the current state — there is no historical endpoint. So a day missed is a
    day of dimension history that cannot be recovered, which is the argument for snapshotting on
    a schedule rather than on demand.
    """
    info = binance.get("/api/v3/exchangeInfo")
    wanted = set(SYMBOLS)
    rows = [_flatten(e) for e in info["symbols"] if e["symbol"] in wanted]

    missing = wanted - {r["symbol"] for r in rows}
    if missing:
        # A configured symbol absent from exchangeInfo has been delisted. Recorded rather than
        # dropped: "this pair no longer exists" is the single most important thing a symbol
        # dimension can say, and silence would leave the last known row looking current forever.
        logger.warning("not in exchangeInfo, recording as DELISTED: %s", sorted(missing))
        rows.extend({"symbol": s, "status": "DELISTED"} for s in sorted(missing))

    df = pd.DataFrame(rows)
    df["snapshot_date"] = run_date
    return df.sort_values("symbol").reset_index(drop=True)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="zstd")
    return buf.getvalue()
