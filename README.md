# Binance market data pipeline

A batch and streaming pipeline over Binance public market data, built to answer one question
honestly: **is the stream right?**

Two layers compute the same thing from different sources — one-minute candles, once from the
official REST endpoint and once by aggregating the live trade feed. They should agree. Where they
do not, the stream lost messages, or a window closed on the wrong boundary, or a trade arrived
after its minute was already written. All three are real failures of a streaming system, and
without a second opinion none of them is visible: the pipeline keeps running and keeps producing
numbers that are wrong.

Twenty USDT pairs at one-minute resolution, with ninety days of history behind them.

---

## How it fits together

```
                 ┌── REST klines ──> Airflow ──> Bronze ──> Postgres ──> star schema ──┐
Binance ─────────┤                                                                     ├──> reconciliation
                 └── trade stream ──> Kafka ──> aggregator ──> Bronze ─────────────────┘
```

The top path is scheduled, replayable and allowed to be slow. The bottom path is continuous,
unrepeatable and allowed to be lossy. They meet at the right, where the same minute computed twice
either agrees or explains itself.

Everything above `Bronze` differs between the two branches. Everything below it is shared: the
same Hive layout, the same partition keys, the same warehouse.

---

## The batch branch

### Ingest

`dags/binance_klines_ingest.py` — daily, with `catchup=True` so a ninety-day backfill is ninety
ordinary runs rather than a special mode.

```
check_api ──> fetch_klines (×20 symbols) ──> validate_day ──> register
```

**The unit of work is one symbol on one date**, and everything follows from that. A DAG run owns
one date; inside it, `fetch_klines` expands to one mapped task per symbol. A backfill is therefore
a series of independent runs of independent tasks, and any single cell can fail, retry, or be
re-run months later without touching its neighbours.

That is the reason to use a scheduler here instead of a loop over dates. A loop has one failure
mode — it dies in the middle and leaves you to work out where. Mapped tasks over a dated run give
per-cell retries, per-cell logs, and a re-run that targets exactly the cell that broke.

`validate_day` measures completeness before anything downstream sees the day. Binance emits a
candle even for minutes with no trades, so a full day is a fixed number of minutes regardless of
how quiet the market was — which is what makes a shortfall mean lost data rather than a thin day.

### Bronze

Each partition is written as a single object, so a partial write cannot exist: the object either
replaces the previous one or it does not.

```
bronze/klines_1m/dt=2026-08-14/symbol=BTCUSDT/part-0.parquet
```

Hive-style, so DuckDB and Spark both read it with one glob, and a single day can be replaced
without touching the rest. Storage is S3-compatible, which keeps the layout portable between R2,
S3 and a local directory.

### Warehouse

`dags/binance_warehouse.py` loads Bronze into Postgres, snapshots the symbol universe, and
rebuilds the models above it.

```
load_pending ─────┐
                  ├──> dbt_snapshot ──> dbt_build
snapshot_symbols ─┘
```

**Two DAGs rather than one, on purpose.** Ingest is rate-limited network work that fails in ways
retrying fixes. Transformation is local compute that fails in ways retrying does not — a broken
model is broken on the second attempt too. Sharing a DAG would mean either retrying dbt against an
API outage or giving up on Binance because a SQL model has a typo.

The dependency between the two is data, not schedule: the warehouse waits for a day's partitions
to exist rather than for the other DAG to have run. A partition backfilled by hand counts just the
same.

Loading a day is a delete and an insert inside one transaction. That makes re-running a date
replace it rather than double it, which is the property that lets any date be reloaded at any time
without first working out whether it is already there.

The symbol snapshot is a root task rather than a consequence of loading klines. Binance serves
only the current state of the exchange — there is no historical endpoint — so a day of symbol
metadata not captured is a day that cannot be recovered afterwards, and it must not be conditional
on whether klines happened to advance.

### The star schema

`dbt/` — staging views over the raw load, then a fact and its dimensions.

| Model | What it is |
|---|---|
| `stg_klines` | One row per symbol-minute, typed and keyed |
| `fact_ohlcv_1m` | The grain of the warehouse, loaded incrementally |
| `dim_symbol` | The symbol universe, versioned |
| `dim_date` | One row per date present in the data |
| `dim_time` | One row per minute of the day, generated |

Surrogate keys are built once in staging rather than in each mart. Two models deriving "the key
for this minute" independently is how a fact table ends up with rows no dimension matches.

`dim_date` is built from the dates that exist; `dim_time` is generated over every minute of the
day. The asymmetry is deliberate. A date dimension covering days never ingested invites joins that
silently produce rows with no facts, which reads as "the market was closed" rather than "we have
no data". A minute dimension has the opposite problem: every minute is valid whether or not a
candle landed in it, and a gap there would hide the case most worth finding — a minute missing
across every symbol at once.

`dim_symbol` is a **slowly changing dimension, type 2**. A dbt snapshot writes a new version of a
row only when an attribute actually differs, and the dimension exposes half-open validity windows
over those versions, so a fact joins the attributes that were true at its own timestamp rather
than the ones true today. The first version of every symbol is backdated ahead of any fact,
because a dimension whose history begins later than the facts it describes matches nothing at all.

---

## The streaming branch

### Receiving

`src/bmp/ws_consumer.py` holds a WebSocket connection to the public trade stream and forwards what
it receives to Kafka. It does nothing else — no parsing beyond what routing requires, no
aggregation, no storage.

That restraint is the point. The trade stream cannot be replayed: Binance serves no history for
it, so a second disconnected is a second lost for good. Anything that can crash the receiver
therefore does not belong in the receiver. Messages are keyed by symbol, so every trade for a pair
lands on one partition and its order is preserved.

### Aggregating

`src/bmp/aggregator.py` reads those trades back out of Kafka and builds one-minute candles.

A trade's minute comes from its own exchange timestamp, not from when it arrived, so a message
delayed in transit still lands in the minute it belongs to. Windows therefore stay open past the
end of their minute and close on a **watermark** — the newest exchange timestamp seen — rather
than on the wall clock. The distinction matters most exactly when it is hardest to notice: while a
backlog replays out of Kafka the clock runs minutes ahead of the data, and a window closed because
the clock says so is written half-built.

Trades that arrive after their window is gone go to a separate `late_arrivals` partition rather
than being dropped. Discarding them would make the stream quietly disagree with the batch layer
and take the explanation with it.

**Offsets are committed after a write, and never past a window still open.** Kafka keeps one
offset per partition, so committing the read position because the minute that closed was written
also commits past the trades held in the minute that has not — and a restart then resumes inside a
minute and rebuilds it from whatever was left. The commit is clamped to the oldest offset feeding
an open window, so a restart replays that minute from its first trade.

### Running unattended

`deploy/` holds a systemd unit per service and an installer. Both restart automatically and carry
memory limits, and the receiver restarts faster than the aggregator: rejoining a consumer group
every few seconds triggers a rebalance storm that costs more than the restart saves, while a
second of disconnection from Binance is unrecoverable.

The units are deliberately independent. Neither requires the other, because the whole reason the
raw feed goes through a broker is so the two halves can fail apart.

---

## The reconciliation

`fct_layer_reconciliation` joins the two branches on symbol and minute and compares them.

Agreement is evidence. Disagreement is a fault report, and the differences are stored rather than
reduced to a pass or a fail, because distinct faults produce distinct signatures:

| Verdict | What it means |
|---|---|
| `exact` | Trade counts and prices identical |
| `stream_missed_trades` | Fewer trades in the stream, volume light in proportion |
| `stream_double_counted` | More trades in the stream than the exchange reported |
| `boundary_mismatch` | Open or close differs — the minute was cut differently |
| `volume_only` | Counts agree, volumes do not |

Only minutes present in both layers are compared. A minute the stream never saw is a coverage gap,
which `fct_stream_coverage` reports separately; mixing the two would let an outage look like an
accuracy problem. The newest minute is excluded, because a live stream is always part way through
writing it.

Without this, a broken stream is invisible. A consumer that drops one message in fifty keeps
running, keeps writing bars, and keeps producing numbers that are slightly wrong — no exception,
no gap, no row-count anomaly. The only way to see it is to compute the same minute twice.

---

## What the tests hold

dbt tests run inside `dbt build` rather than after it, so a model whose test fails blocks
everything downstream instead of letting the failure propagate into tables that then look fine.

- **No millisecond belongs to two partitions.** Binance's `endTime` is inclusive, so a
  midnight-to-midnight request returns the whole day plus the first minute of the next one. Inside
  one partition that is invisible; across two it is a duplicate key, and it surfaces weeks later
  inside an aggregate.
- **Every raw row reaches the fact table.** An incremental model can quietly stop covering history
  while every other test still passes, because those tests describe the rows that arrived and say
  nothing about the ones that did not.
- **A symbol has exactly one current version.** Two open validity windows would multiply every
  fact that joins them.
- **A minute belongs to one date**, and a complete trading day has every minute in it.

Python tests cover the client's retry classification, the day boundary, and the aggregator's
window, lateness and offset handling.

---

## Running it

```bash
cp .env.example .env          # optional — see below
docker compose -f docker/docker-compose.yml up -d
```

Airflow is on http://localhost:8080 (admin / admin). Unpause `binance_klines_ingest` and it
backfills from its start date; `binance_warehouse` picks up whatever Bronze holds.

Without R2 credentials **everything still runs**, writing partitions to `data/` under the
identical layout. A cloud account is not a condition of executing the DAG — a repository that
cannot be run cannot be reviewed. The streaming branch additionally needs a Kafka cluster; without
one, the batch branch is unaffected.

```bash
pip install -e ".[dev]"
pytest -q
```

Reading what landed, straight from the partitions:

```sql
SELECT symbol, count(*) AS minutes, min(minute), max(minute)
FROM read_parquet('data/bronze/klines_1m/**/*.parquet', hive_partitioning = true)
GROUP BY 1;
```

---

## Choices worth defending

**`data-api.binance.vision`, not `api.binance.com`.** Same klines, no key, and not regionally
blocked the same way — which matters because this is meant to run from a cloud VM. The trade
stream uses `data-stream.binance.vision` for the same reason.

**Bronze keeps the source's own strings.** Prices arrive as text and are stored as text. Casting
on the way in would round a value before anyone has looked at it, and the string is the evidence
of what Binance actually sent. Typing belongs in the next layer, where a bad cast is visible and
reversible.

**Twenty symbols, chosen twice over.** Ranked by live 24-hour volume, then filtered: every pair
must have traded for more than 120 days, because a symbol listed last month cannot be backfilled
ninety days and leaves holes that look like ingestion bugs. Pegged pairs were dropped despite high
volume — a price series that never moves makes every downstream aggregate meaningless while still
costing storage and API budget. The mix is deliberate: large caps whose minutes are always
populated, and thinner pairs that genuinely have minutes with no trades. A universe of only BTC
and ETH would never surface the second case.

**Pacing well under the published limit.** A scheduled backfill fires thousands of requests
unattended, and a 429 in the middle leaves a partial day that looks like a complete one. Slow and
finished beats fast and half-written.

**dbt through the CLI, not a Python API.** The command line is the interface dbt supports, and its
exit code means the same thing in CI, in a terminal, and inside a scheduler.

**A skipped load does not cancel the run.** Airflow propagates skips by default, so a day with no
new Bronze partition would take the symbol snapshot and the model build down with it and still
report success. A skip upstream means there was no work, not that the work failed.
