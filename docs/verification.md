# Local verification, 18 November 2026

`docker compose run --rm pipeline sh scripts/run_demo.sh` was run with a new `warehouse/` directory, then run again unchanged:

| Check | First run | Replay |
| --- | --- | --- |
| Source files | Three processed | Three skipped by matching SHA-256 |
| Delivered / unique / duplicate events | 200 / 190 / 10 | Unchanged |
| Latest orders | 100 | 100 |
| Silver and gold date partitions | 29 each | Unchanged |
| Quarantined fixture lines | 0 | 0 |
| Integration and generator tests | 8 passed | 8 passed |

The quality check compared manifest counts with DuckDB tables, bronze and quarantine Parquet counts, silver rows with latest order state, and gold status/revenue values with grouped silver state. The default gold output has **23 paid EUR orders with 578,748 cents** and **17 paid USD orders with 479,572 cents**. The Parquet type of `paid_revenue_cents` was inspected as **BIGINT**; a prior draft had exported it as `DOUBLE` and was corrected before commit.

Integration tests also passed for malformed JSON, unexpected fields, invalid amounts, conflicting duplicate IDs, changed already-processed source files, one-date partition updates, an invalid-only batch, and recovery when the database commit precedes Parquet export. The hosted GitHub Actions workflow has not run yet because this project is still local.
