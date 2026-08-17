"""Backfill Bronze for a date range, without Airflow.

The DAG is the intended way to run this — it gives per-cell retries and a record of what ran.
This exists for two situations the DAG cannot serve: bringing history in before the scheduler is
configured, and re-running a stretch of dates from a laptop when the scheduler is not available.

It calls the same `klines.fetch_day` and writes the same partitions, so nothing here is a second
implementation that could drift from the first. The only thing it adds is a loop and a progress
line.

    python scripts/backfill.py --days 90
    python scripts/backfill.py --start 2026-05-15 --end 2026-08-16
    python scripts/backfill.py --days 7 --force
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# The DAG gets its credentials from the container environment; a script run by hand has to load
# them itself. Without this the backfill silently writes to local disk instead of the bucket —
# which looks like success and is why it is loaded before anything else.
from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from bmp import binance, klines, objstore  # noqa: E402
from bmp.config import SYMBOLS, bronze_key, local_bronze_path  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")


def dates(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=90, help="how many days back from yesterday")
    p.add_argument("--start", help="YYYY-MM-DD; overrides --days")
    p.add_argument("--end", help="YYYY-MM-DD; defaults to yesterday")
    p.add_argument("--force", action="store_true",
                   help="rewrite partitions that already exist")
    args = p.parse_args()

    yesterday = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)
    end = dt.date.fromisoformat(args.end) if args.end else yesterday
    start = (dt.date.fromisoformat(args.start) if args.start
             else end - dt.timedelta(days=args.days - 1))

    if not binance.ping():
        print("Binance is not responding")
        return 1

    day_list = list(dates(start, end))
    where = f"s3://{objstore.bucket()}" if objstore.is_configured() else "data/ (local)"
    print(f"{len(day_list)} days x {len(SYMBOLS)} symbols -> {where}")
    print(f"{start} .. {end}\n")

    started = time.time()
    written = skipped = failed = 0

    for i, day in enumerate(day_list, 1):
        run_date = day.isoformat()
        rows_today = 0

        for symbol in SYMBOLS:
            key = bronze_key(run_date, symbol)

            # Already-present partitions are skipped, which makes a re-run cheap and lets an
            # interrupted backfill be resumed by issuing the same command again.
            if not args.force and objstore.is_configured() and objstore.exists(key):
                skipped += 1
                continue

            try:
                df = klines.fetch_day(symbol, run_date)
                result = klines.summarise(df, symbol, run_date)
                if df.empty:
                    print(f"  {run_date} {symbol}: no candles")
                    failed += 1
                    continue
                if not result.complete:
                    print(f"  {run_date} {symbol}: {result.rows}/{result.expected} minutes "
                          f"({result.missing} missing)")
                objstore.write_partition(key, klines.to_parquet_bytes(df),
                                         local_bronze_path(run_date, symbol))
                written += 1
                rows_today += result.rows
            except Exception as exc:
                print(f"  {run_date} {symbol}: {type(exc).__name__}: {exc}")
                failed += 1

        elapsed = time.time() - started
        eta = elapsed / i * (len(day_list) - i)
        print(f"[{i:3d}/{len(day_list)}] {run_date}  {rows_today:>6,} rows  "
              f"elapsed {elapsed/60:5.1f}m  eta {eta/60:5.1f}m")

    print(f"\nwritten {written} · skipped {skipped} · failed {failed} · "
          f"{(time.time() - started)/60:.1f} minutes")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
