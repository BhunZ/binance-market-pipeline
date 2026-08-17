"""Daily ingest of 1-minute klines into Bronze, one partition per symbol-day.

**The unit of work is one symbol on one date**, and every design choice follows from that.
A DAG run owns exactly one date; inside it, `fetch_klines` expands to one mapped task per symbol.
So the backfill of 90 days is 90 independent runs, each of 20 independent tasks — any one of
which can fail, be retried, or be re-run months later without touching the rest.

That is the whole reason to reach for a scheduler here rather than a loop in a script. A loop
over dates has one failure mode: it dies somewhere in the middle and leaves you to work out
where. Mapped tasks over a dated run give per-cell retries, per-cell logs, and a re-run that
targets exactly the cell that broke.

**Why the run date is the previous day.** A DAG scheduled daily fires *after* its interval
closes, so a run stamped 2026-08-14 executes on the 15th and ingests a day that is complete.
Ingesting today's date would always produce a partial partition that looks identical to a
partition damaged by an outage.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bmp import binance, klines, objstore  # noqa: E402
from bmp.config import SYMBOLS, bronze_key, local_bronze_path  # noqa: E402

logger = logging.getLogger(__name__)

#: Below this share of the expected minutes, the partition is treated as damaged rather than
#: thin. Set at 99% because Binance emits a candle even for minutes with no trades — measured on
#: CHIPUSDT, 310 of 1440 minutes had zero trades and all 1440 candles were still returned. A real
#: shortfall therefore means lost data, not a quiet market.
MIN_COMPLETENESS = 0.99


@dag(
    dag_id="binance_klines_ingest",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 5, 15, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=2)},
    tags=["bronze", "binance", "backfill"],
    doc_md=__doc__,
)
def binance_klines_ingest():

    @task
    def check_api() -> list[str]:
        """Fail the run before it spends anything if the API is down or a symbol has delisted.

        Returns the symbols to fetch. A delisted symbol returns an empty kline list, which is
        indistinguishable from a day with no trading unless the universe is checked first — so it
        is checked first, and dropped loudly.
        """
        if not binance.ping():
            raise AirflowFailException("Binance API is not responding")

        listed = binance.exchange_symbols()
        missing = [s for s in SYMBOLS if s not in listed]
        if missing:
            logger.warning("no longer trading, skipping: %s", missing)
        active = [s for s in SYMBOLS if s in listed]
        if not active:
            raise AirflowFailException("none of the configured symbols are trading")
        return active

    @task
    def fetch_klines(symbol: str, **context) -> dict:
        """One symbol-day: fetch, write the partition, report what landed.

        Writing the whole day as a single object is deliberate. A partial write cannot exist,
        because the object either replaces the previous one or does not — so a task that dies
        mid-run leaves the previous partition intact rather than half of a new one.
        """
        run_date = context["data_interval_start"].to_date_string()

        df = klines.fetch_day(symbol, run_date)
        result = klines.summarise(df, symbol, run_date)

        if result.expected == 0:
            raise AirflowSkipException(f"{run_date} is in the future")

        if df.empty:
            raise AirflowFailException(
                f"{symbol} returned no candles for {run_date} — the symbol is listed but the "
                f"day is empty, which should not happen for a trading pair")

        destination = objstore.write_partition(
            key=bronze_key(run_date, symbol),
            payload=klines.to_parquet_bytes(df),
            local_path=local_bronze_path(run_date, symbol),
        )
        logger.info("%s %s: %d/%d minutes -> %s", symbol, run_date, result.rows,
                    result.expected, destination)

        return {
            "symbol": symbol,
            "run_date": run_date,
            "rows": result.rows,
            "expected": result.expected,
            "complete": result.complete,
        }

    @task
    def validate_day(results: list[dict], **context) -> dict:
        """Refuse to mark a date good when a partition is short.

        This is the gate that separates a thin day from a broken one. Without it a run whose
        symbols each landed 60% of their minutes still goes green, and the hole only surfaces
        weeks later inside an aggregate nobody can reconcile.
        """
        run_date = context["data_interval_start"].to_date_string()
        damaged = [
            r for r in results
            if r["expected"] and r["rows"] / r["expected"] < MIN_COMPLETENESS
        ]
        if damaged:
            detail = ", ".join(f"{r['symbol']} {r['rows']}/{r['expected']}" for r in damaged)
            raise AirflowFailException(f"incomplete partitions for {run_date}: {detail}")

        total = sum(r["rows"] for r in results)
        logger.info("%s: %d symbols, %d rows, all complete", run_date, len(results), total)
        return {"run_date": run_date, "symbols": len(results), "rows": total}

    @task
    def register(summary: dict) -> None:
        """Record what this run produced.

        A line per run is what makes a gap answerable later. `M2` moves this into a table in the
        warehouse; for now the Airflow log is the record, and saying so beats implying a
        durability that does not exist yet.
        """
        logger.info("registered %(run_date)s: %(symbols)d symbols, %(rows)d rows", summary)

    symbols = check_api()
    fetched = fetch_klines.expand(symbol=symbols)
    register(validate_day(fetched))


binance_klines_ingest()
