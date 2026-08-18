"""Bronze to warehouse: load the day, snapshot the symbols, rebuild the models.

The second half of the chain. `binance_klines_ingest` puts a day into object storage; this puts
that day into Postgres and rebuilds what sits on top of it.

**Two DAGs rather than one, on purpose.** Ingest is rate-limited network work that fails in ways
retrying fixes. Transformation is local compute that fails in ways retrying does not — a broken
model is broken on the second attempt too. Sharing a DAG would mean either retrying dbt against
an API outage or giving up on Binance because a SQL model has a typo. Splitting them lets each
half fail on its own terms, and lets the warehouse be rebuilt without re-fetching anything.

The dependency between them is data, not schedule: this DAG waits for the day's partitions to
exist rather than for the other DAG to have run. A partition backfilled by hand counts just the
same, which is what let the warehouse be populated while the scheduler was still being wired.

**Nothing to load is not a reason to do nothing.** `load_pending` skips when the warehouse
already holds every complete Bronze day, and a skip in Airflow propagates: downstream tasks skip
too and the run still reports success. That put the dimension snapshot behind a condition it has
nothing to do with — and Binance serves only the current state, so a day it skips is a day of
symbol history that cannot be recovered later. It also left the dbt models unbuilt on any day
klines did not advance, while the streaming branch kept writing minutes that then never reached
the reconciliation. The snapshot now runs on its own, and dbt runs whenever nothing upstream has
actually failed.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bmp import exchange_info, objstore, warehouse  # noqa: E402
from bmp.config import SYMBOLS  # noqa: E402

logger = logging.getLogger(__name__)

DBT_DIR = "/opt/airflow/dbt"

#: dbt runs against the same Postgres by service name; the host default of localhost would be the
#: worker container, where nothing is listening.
DBT_ENV = {
    "DBT_HOST": "postgres",
    "DBT_PORT": "5432",
    "DBT_USER": "airflow",
    "DBT_PASSWORD": "airflow",
    "DBT_DBNAME": "airflow",
    "DBT_PROFILES_DIR": DBT_DIR,
}


@dag(
    dag_id="binance_warehouse",
    schedule="30 1 * * *",
    start_date=pendulum.datetime(2026, 5, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=3)},
    tags=["warehouse", "dbt", "silver", "gold"],
    doc_md=__doc__,
)
def binance_warehouse():

    @task
    def load_pending() -> dict:
        """Copy every complete Bronze day the warehouse does not already hold.

        Incomplete days are skipped rather than loaded. A partition holding 12 of 20 symbols is a
        backfill still running, not a day the market was quiet, and loading it would put a hole
        into the warehouse that every aggregate above inherits — visible only as numbers that are
        slightly too small, which nobody notices.
        """
        warehouse.ensure_schema()

        counts: dict[str, int] = {}
        for key in objstore.list_keys("bronze/klines_1m/"):
            date = key.split("dt=")[1].split("/")[0]
            counts[date] = counts.get(date, 0) + 1

        already = set(warehouse.loaded_dates())
        pending = sorted(d for d, n in counts.items() if n == len(SYMBOLS) and d not in already)
        incomplete = sorted(d for d, n in counts.items() if n < len(SYMBOLS))

        if incomplete:
            logger.info("skipping %d incomplete day(s): %s", len(incomplete), incomplete[:5])
        if not pending:
            raise AirflowSkipException("warehouse already holds every complete Bronze day")

        rows = sum(warehouse.load_day(d) for d in pending)
        logger.info("loaded %d dates, %d rows", len(pending), rows)
        return {"dates": len(pending), "rows": rows}

    @task
    def snapshot_symbols() -> int:
        """Today's symbol metadata into Bronze and the warehouse.

        Runs every day even when nothing changed. Binance serves only the current state — there
        is no historical endpoint — so a day not captured is a day of dimension history that
        cannot be recovered afterwards. The dbt snapshot downstream writes a new version only
        when an attribute actually differs, so a quiet day costs one row in `raw` and none in the
        dimension.
        """
        run_date = pendulum.now("UTC").to_date_string()
        df = exchange_info.fetch(run_date)
        if df.empty:
            raise AirflowFailException("exchangeInfo returned nothing for the configured universe")

        objstore.write_partition(
            key=exchange_info.bronze_key(run_date),
            payload=exchange_info.to_parquet_bytes(df),
            local_path=Path("/opt/airflow/data") / exchange_info.bronze_key(run_date),
        )
        warehouse.ensure_exchange_schema()
        return warehouse.load_exchange_info(df, run_date)

    # dbt as shell commands rather than through a Python API: the CLI is the interface dbt
    # supports, and its exit code is the only signal that means the same thing in CI, in a
    # terminal, and here.
    # `none_failed` and not the default `all_success`: `load_pending` skips on a day with no new
    # Bronze partition, and under the default rule that skip cascades here and the run goes green
    # having built nothing. A skip upstream means there was no work, not that the work failed.
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --no-use-colors",
        env=DBT_ENV,
        append_env=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # `build` rather than `run` then `test`: build interleaves them, so a model whose test fails
    # blocks everything downstream of it instead of letting the failure propagate into tables
    # that then look fine.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --no-use-colors",
        env=DBT_ENV,
        append_env=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # The snapshot is a root task, not a consequence of loading klines. The two read different
    # endpoints for different things and neither is a precondition of the other; chaining them
    # only meant one could silently cancel the other.
    [load_pending(), snapshot_symbols()] >> dbt_snapshot >> dbt_build


binance_warehouse()
