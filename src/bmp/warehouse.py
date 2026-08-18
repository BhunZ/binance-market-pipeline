"""Load Bronze partitions into Postgres, one day at a time.

Bronze is Parquet on object storage and stays that way — it is the record of what Binance served
and nothing rewrites it. This copies a day of it into a relational table so dbt can model it, and
so the serving layer answers SQL rather than file globs.

**The load is idempotent by deletion, not by upsert.** A day is a whole partition: delete every
row for that date, then insert it fresh. An upsert keyed on (symbol, minute) would leave behind
any row that used to exist and no longer does, which is precisely the case a re-run is meant to
correct. Delete-then-insert states the intent — *this date now looks like this* — and the two
statements share a transaction, so a failure halfway leaves the previous day intact rather than
half of it.

DuckDB does the reading. It queries Parquet straight from object storage without downloading the
file, which keeps this module free of temporary directories and of the cleanup they need.
"""

from __future__ import annotations

import io
import logging
import os

import duckdb
import pandas as pd

from . import objstore
from .config import BRONZE_PREFIX, DATA_DIR

logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "klines_1m"

#: Bronze holds Binance's own strings. They become numeric here, at the one place where a bad
#: cast is visible and reversible — the row count and the source file are both still to hand.
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
    symbol        text        NOT NULL,
    minute        timestamptz NOT NULL,
    dt            date        NOT NULL,
    open          numeric(38, 12) NOT NULL,
    high          numeric(38, 12) NOT NULL,
    low           numeric(38, 12) NOT NULL,
    close         numeric(38, 12) NOT NULL,
    volume        numeric(38, 12) NOT NULL,
    quote_volume  numeric(38, 12) NOT NULL,
    trades        integer     NOT NULL,
    taker_base    numeric(38, 12) NOT NULL,
    taker_quote   numeric(38, 12) NOT NULL,
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, minute)
);

-- Every load and every incremental dbt model filters by date, and the primary key leads with
-- symbol so it cannot serve that.
CREATE INDEX IF NOT EXISTS idx_klines_dt ON {RAW_SCHEMA}.{RAW_TABLE} (dt);
"""


def dsn() -> str:
    """Connection string. Defaults to the compose Postgres, overridable for a managed one."""
    return os.getenv(
        "WAREHOUSE_DSN",
        "postgresql://airflow:airflow@postgres:5432/airflow",
    )


def connect():
    import psycopg

    return psycopg.connect(dsn())


def ensure_schema() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()


def _duckdb_reader() -> duckdb.DuckDBPyConnection:
    """A DuckDB session that can read the bucket, or plain local files without credentials."""
    con = duckdb.connect()
    if objstore.is_configured():
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE SECRET r2 (
                TYPE r2,
                KEY_ID '{os.environ["R2_ACCESS_KEY_ID"]}',
                SECRET '{os.environ["R2_SECRET_ACCESS_KEY"]}',
                ACCOUNT_ID '{os.environ["R2_ACCOUNT_ID"]}'
            )
        """)
    return con


def bronze_glob(run_date: str) -> str:
    if objstore.is_configured():
        return f"r2://{objstore.bucket()}/{BRONZE_PREFIX}/dt={run_date}/*/*.parquet"
    return (DATA_DIR / BRONZE_PREFIX / f"dt={run_date}" / "*" / "*.parquet").as_posix()


def read_day(run_date: str) -> pd.DataFrame:
    """One day of Bronze, typed, ready to insert.

    `symbol` comes from the Hive partition rather than the file body. Both exist and agree, but
    the partition is what the query engine filtered on, so trusting the column instead would let
    a mislabelled file land under a date it does not belong to.
    """
    con = _duckdb_reader()
    df = con.execute(f"""
        SELECT
            symbol,
            minute,
            DATE '{run_date}'                 AS dt,
            CAST(open         AS DECIMAL(38,12)) AS open,
            CAST(high         AS DECIMAL(38,12)) AS high,
            CAST(low          AS DECIMAL(38,12)) AS low,
            CAST(close        AS DECIMAL(38,12)) AS close,
            CAST(volume       AS DECIMAL(38,12)) AS volume,
            CAST(quote_volume AS DECIMAL(38,12)) AS quote_volume,
            CAST(trades       AS INTEGER)        AS trades,
            CAST(taker_base   AS DECIMAL(38,12)) AS taker_base,
            CAST(taker_quote  AS DECIMAL(38,12)) AS taker_quote
        FROM read_parquet('{bronze_glob(run_date)}', hive_partitioning = true)
        ORDER BY symbol, minute
    """).df()
    con.close()
    return df


def load_day(run_date: str) -> int:
    """Replace one date in the warehouse. Returns rows written.

    Delete and insert share a transaction. If the insert fails the delete rolls back with it, so
    the table never holds a date that was emptied and not refilled — the state a partial load
    would otherwise leave, which reads downstream as a day the market did not trade.
    """
    df = read_day(run_date)
    if df.empty:
        logger.warning("no Bronze for %s — nothing loaded", run_date)
        return 0

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols = ", ".join(df.columns)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE dt = %s", (run_date,))
        with cur.copy(
            f"COPY {RAW_SCHEMA}.{RAW_TABLE} ({cols}) FROM STDIN WITH (FORMAT csv)"
        ) as copy:
            copy.write(buf.read())
        conn.commit()

    logger.info("loaded %s: %d rows", run_date, len(df))
    return len(df)


def loaded_dates() -> list[str]:
    """Which dates the warehouse already holds. Lets a load skip work already done."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT dt FROM {RAW_SCHEMA}.{RAW_TABLE} ORDER BY dt")
        return [r[0].isoformat() for r in cur.fetchall()]


def row_counts() -> list[tuple]:
    """Rows per date, for checking a load did what it claimed."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT dt, count(DISTINCT symbol) AS symbols, count(*) AS rows
            FROM {RAW_SCHEMA}.{RAW_TABLE} GROUP BY dt ORDER BY dt
        """)
        return cur.fetchall()


# ----------------------------------------------------------------------------------
# Symbol metadata — the source the Type 2 dimension is built from
# ----------------------------------------------------------------------------------

EXCHANGE_TABLE = "exchange_info"

EXCHANGE_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{EXCHANGE_TABLE} (
    snapshot_date          date NOT NULL,
    symbol                 text NOT NULL,
    status                 text NOT NULL,
    base_asset             text,
    quote_asset            text,
    base_precision         integer,
    quote_precision        integer,
    spot_trading_allowed   boolean,
    margin_trading_allowed boolean,
    tick_size              numeric(38, 12),
    min_price              numeric(38, 12),
    max_price              numeric(38, 12),
    step_size              numeric(38, 12),
    min_qty                numeric(38, 12),
    max_qty                numeric(38, 12),
    min_notional           numeric(38, 12),
    max_notional           numeric(38, 12),
    loaded_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, symbol)
);
"""


def ensure_exchange_schema() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(EXCHANGE_DDL)
        conn.commit()


def load_exchange_info(df, run_date: str) -> int:
    """Replace one day of symbol metadata. Same delete-then-insert as the candles.

    The dbt snapshot reads the *latest* row per symbol from here, so this table holds raw daily
    observations and the snapshot holds the versioned history. Keeping those separate means a
    re-loaded day corrects the observation without rewriting the version history built from it.
    """
    if df.empty:
        return 0

    cols = [c for c in df.columns if c != "loaded_at"]
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False)
    buf.seek(0)

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {RAW_SCHEMA}.{EXCHANGE_TABLE} WHERE snapshot_date = %s", (run_date,))
        with cur.copy(
            f"COPY {RAW_SCHEMA}.{EXCHANGE_TABLE} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)"
        ) as copy:
            copy.write(buf.read())
        conn.commit()
    return len(df)
