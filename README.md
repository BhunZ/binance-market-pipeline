# Binance market data pipeline

A batch and streaming pipeline over Binance public market data, built to answer one question
honestly: **is the stream right?**

Two layers compute the same thing from different sources — one-minute candles, once from the
official REST endpoint and once by aggregating the live trade feed. They should agree. Where they
do not, the stream lost messages, or a window closed on the wrong boundary, or a trade arrived
after its minute was already written. All three are real failures of a streaming system, and
without a second opinion none of them is visible: the pipeline keeps running and keeps producing
numbers that are wrong.

Twenty USDT pairs, one-minute resolution, 90 days of history — about 2.6 million rows.

---

## Status

| Milestone | What it adds | State |
|---|---|---|
| **M1** | Airflow DAG, backfill, Bronze on object storage | **done** |
| **M2** | Postgres warehouse, dbt star schema, tests | **done** |
| **M3** | Kafka, WebSocket consumer, one-minute windows | **done** |
| **M4** | Cross-layer reconciliation | **done** |
| M5 | Health checks and alerting | next |

---

## The ingest DAG

`dags/binance_klines_ingest.py` — daily, with `catchup=True` so the 90-day backfill is 90
ordinary runs rather than a special mode.

```
check_api ──> fetch_klines (×20 symbols) ──> validate_day ──> register
```

**The unit of work is one symbol on one date**, and everything follows from that. A DAG run owns
one date; inside it, `fetch_klines` expands to one mapped task per symbol. The backfill is
therefore 90 independent runs of 20 independent tasks, and any single cell can fail, retry, or be
re-run months later without touching the other 1,799.

That is the reason to use a scheduler here instead of a loop over dates. A loop has one failure
mode — it dies in the middle and leaves you to work out where. Mapped tasks over a dated run give
per-cell retries, per-cell logs, and a re-run that targets exactly the cell that broke.

Each partition is written as a single object, so a partial write cannot exist: the object either
replaces the previous one or it does not.

```
bronze/klines_1m/dt=2026-08-14/symbol=BTCUSDT/part-0.parquet
```

Hive-style, so DuckDB and Spark both read it with one glob, and a single day can be replaced
without touching the rest.

---

## Two things this had to get right

**The day boundary.** The first working version returned **1441 candles**. Binance's `endTime`
includes the candle opening exactly at that instant, so midnight-to-midnight returns the whole day
plus the first minute of the next one. Nothing about that is visible in one partition — 1441 rows
in a file nobody counts looks like 1440. It becomes a duplicate key only when two partitions are
read together, weeks later, inside an aggregate: **1,800 duplicated rows** across the backfill.
`tests/test_klines.py` states the invariant as *no millisecond belongs to two partitions*.

**A quiet market is not a broken pipeline.** Binance emits a candle even for minutes with no
trades — measured on CHIPUSDT, 310 of 1440 minutes had zero trades and all 1440 candles were
still returned. So "1440 minutes" is a sound completeness check, and a genuine shortfall means
lost data rather than a thin day. `validate_day` fails the run below 99%.

---

## Running it

```bash
cp .env.example .env          # optional — see below
docker compose -f docker/docker-compose.yml up -d
```

Airflow is on http://localhost:8080 (admin / admin). Unpause `binance_klines_ingest` and it
backfills from its start date.

Without R2 credentials **everything still runs**, writing partitions to `data/` under the
identical layout. A cloud account is not a condition of executing the DAG — a repository that
cannot be run cannot be reviewed.

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
blocked the same way — which matters because this is meant to run from a cloud VM.

**Bronze keeps the source's own strings.** Prices arrive as text and are stored as text. Casting
on the way in would round a value before anyone has looked at it, and the string is the evidence
of what Binance actually sent. Typing belongs in the next layer, where a bad cast is visible and
reversible.

**Twenty symbols, chosen twice over.** Ranked by live 24-hour volume, then filtered: every pair
must have traded for more than 120 days, because a symbol listed last month cannot be backfilled
90 days and leaves holes that look like ingestion bugs. Pegged pairs were dropped despite high
volume — a price series that never moves makes every downstream aggregate meaningless while still
costing storage and API budget. The mix is deliberate: large caps whose minutes are always
populated, and thinner pairs that genuinely have minutes with no trades. A universe of only BTC
and ETH would never surface the second case.

**Pacing well under the published limit.** A scheduled backfill fires thousands of requests
unattended, and a 429 in the middle leaves a partial day that looks like a complete one. Slow and
finished beats fast and half-written.
