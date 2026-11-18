# Retail Orders Parquet Lakehouse

A Data Engineering project in progress. It turns fictional order-change files into a bronze/silver/gold analytical lakehouse using DuckDB and Parquet. Every customer and order ID is generated; no commercial or personal data is included.

## Target pipeline

```text
deterministic JSONL batches → bronze Parquet → validated latest-order silver Parquet → daily gold Parquet
                                  ↘ quarantine of invalid records
                         DuckDB manifest and quality state
```

The input represents order changes. `event_id` identifies a unique change, while `(order_id, event_version)` defines an order's version history. A file can arrive twice without duplicating a change. A later batch can contain an older `updated_at` than events already ingested, so event time cannot be used as the ingestion cursor.

Gold revenue will count orders whose **latest** status is `paid`, grouped by order date and currency. USD and EUR amounts stay separate. Prices are integer cents to avoid floating-point rounding in ingestion and reconciliation.

## Current milestone

- [x] Define the v1 order-event contract.
- [x] Generate deterministic source batches with updates, refunds, duplicate deliveries, and late event time.
- [ ] Implement bronze, silver, and gold Parquet tables with an incremental file manifest.
- [ ] Quarantine bad records and detect schema drift.
- [ ] Verify replay, partition updates, reconciliation, and a reproducible benchmark.
- [ ] Review and publish privately on a later date than the first DE project.

Generate local source files:

```bash
python3 src/generate_orders.py --output-dir data/raw
```

`data/` and `warehouse/` are ignored by Git. The generator writes three ordered batch files and does not need external packages. The default fixture has 100 orders, 190 unique change events, and 10 intentional duplicate deliveries.

See [fixture details](docs/fixture.md) for the expected status counts and data-quality rules.
