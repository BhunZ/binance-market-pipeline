"""Load Bronze partitions into the Postgres warehouse.

The counterpart to `backfill.py`: that one fills object storage, this one fills the warehouse
from it. Both exist for the same reason — to bring history in before the scheduler is wired, and
to re-run a stretch of dates from a laptop.

Only complete days are loaded by default. A partition holding 12 of 20 symbols is a backfill
still in progress, not a day the market was quiet, and loading it would put a hole into the
warehouse that every later aggregate inherits.

    python scripts/load_warehouse.py
    python scripts/load_warehouse.py --force        # reload dates already present
    python scripts/load_warehouse.py --allow-partial
"""

from __future__ import annotations

import argparse
import collections
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from bmp import objstore, warehouse  # noqa: E402
from bmp.config import SYMBOLS  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")


def bronze_dates() -> dict[str, int]:
    """Dates present in Bronze, and how many symbols each holds."""
    counts: collections.Counter[str] = collections.Counter()
    for key in objstore.list_keys("bronze/klines_1m/"):
        counts[key.split("dt=")[1].split("/")[0]] += 1
    return dict(counts)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true", help="reload dates already in the warehouse")
    p.add_argument("--allow-partial", action="store_true",
                   help="load days holding fewer than all symbols")
    args = p.parse_args()

    warehouse.ensure_schema()

    available = bronze_dates()
    if not available:
        print("no Bronze partitions found")
        return 1

    already = set() if args.force else set(warehouse.loaded_dates())
    expected = len(SYMBOLS)

    todo, partial = [], []
    for date, n in sorted(available.items()):
        if date in already:
            continue
        (todo if n == expected or args.allow_partial else partial).append((date, n))

    print(f"bronze: {len(available)} dates · warehouse already holds {len(already)}")
    if partial:
        print(f"skipping {len(partial)} incomplete: "
              + ", ".join(f"{d} ({n}/{expected})" for d, n in partial[:5])
              + (" ..." if len(partial) > 5 else ""))
    if not todo:
        print("nothing to load")
        return 0

    print(f"loading {len(todo)} dates\n")
    started, total = time.time(), 0
    for i, (date, _) in enumerate(todo, 1):
        rows = warehouse.load_day(date)
        total += rows
        elapsed = time.time() - started
        print(f"[{i:3d}/{len(todo)}] {date}  {rows:>6,} rows  "
              f"elapsed {elapsed/60:4.1f}m  eta {elapsed/i*(len(todo)-i)/60:4.1f}m")

    print(f"\n{total:,} rows across {len(todo)} dates in {(time.time()-started)/60:.1f} minutes")
    for date, symbols, rows in warehouse.row_counts()[-3:]:
        print(f"  {date}  {symbols} symbols  {rows:,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
