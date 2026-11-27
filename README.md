# Retail Orders Parquet Lakehouse

A local Data Engineering pipeline that turns fictional order-change files into a bronze/silver/gold analytical lakehouse using DuckDB and Parquet. Every customer and order ID is generated; no commercial or personal data is included.

## Pipeline

```text
deterministic JSONL batches → bronze Parquet → validated latest-order silver Parquet → daily gold Parquet
                                  ↘ quarantine of invalid records
                         DuckDB manifest and quality state
```

The input represents order changes. `event_id` identifies a unique change, while `(order_id, event_version)` defines an order's version history. A file can arrive twice without duplicating a change. A later batch can contain an older `updated_at` than events already ingested, so event time cannot be used as the ingestion cursor.

Gold revenue counts orders whose **latest** status is `paid`, grouped by order date and currency. USD and EUR amounts stay separate. Prices are integer cents to avoid floating-point rounding in ingestion and reconciliation.

```text
contracts/  JSON Schema for one order change
src/        generator, incremental loader, quality checks, benchmark
tests/      replay, quarantine, partition, and recovery tests
docs/       architecture, metrics, fixture, performance, verification
scripts/    one-command local demo
data/       generated JSONL input (ignored by Git)
warehouse/  DuckDB state and Parquet layers (ignored by Git)
```

## Current milestone

- [x] Define the v1 order-event contract.
- [x] Generate deterministic source batches with updates, refunds, duplicate deliveries, and late event time.
- [x] Implement bronze, silver, and gold Parquet layers with an incremental file manifest.
- [x] Quarantine bad records and detect schema drift.
- [x] Verify replay, partition updates, reconciliation, and a reproducible benchmark.
- [x] Measure both cold ingestion and an unchanged three-batch replay.
- [x] Add a CI workflow for fresh builds and unchanged replays.
- [x] Make the Docker Compose demo reliable on Docker Desktop with named data volumes.
- [x] Complete the local release review with 13 passing tests.
- [ ] Verify the first hosted CI run after public publication.

With Docker running, generate local source files and build the lakehouse from a fresh directory:

```bash
docker compose run --build --rm pipeline sh scripts/run_demo.sh
```

The generator writes three ordered batch files. Repeating the demo skips unchanged files and checks the same expected output. Docker Compose stores generated input and lakehouse state in named volumes so the command behaves consistently on Linux and Docker Desktop. A direct Python run still uses the ignored local `data/` and `warehouse/` directories. The default fixture has 100 orders, 190 unique change events, and 10 intentional duplicate deliveries. The [CI workflow](.github/workflows/verify.yml) runs the same sequence on every push and pull request.

## Default fixture result

The latest order state has **40 paid**, **30 placed**, **20 refunded**, and **10 cancelled** orders. Gold Parquet contains 23 currently paid EUR orders worth **578,748 cents** and 17 currently paid USD orders worth **479,572 cents**. These are generated test values, not sales findings.

See [fixture details](docs/fixture.md), [architecture and recovery](docs/architecture.md), [metric definitions](docs/metrics.md), [local verification](docs/verification.md), [performance note](docs/performance.md), and the [release checklist](docs/release-checklist.md) for expected outcomes, limitations, and release evidence.
