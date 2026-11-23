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

## Clean release rehearsal, 19 November 2026

The repository was copied without `.git`, `data/`, or `warehouse/` into a new temporary directory. The first Docker build attempt stopped while Docker Registry returned an `EOF` for the Python base-image metadata; an immediate retry resolved the same pinned image digest and completed successfully.

The clean run processed **200** deliveries into **190** unique events and **100** latest orders across **29** date partitions. All **9** tests passed, including the check that detects changed bronze content when row counts remain equal. Running the complete demo again skipped all three source files by their recorded SHA-256 digests, preserved the exact fixture counts, and passed all **9** tests again.

## Tracked-file review, 19 November 2026

`git ls-files` returned **21** source, contract, workflow, test, and documentation files. No tracked path was inside `data/` or `warehouse/`, and no `.env`, DuckDB, Parquet, or JSONL file was tracked. A case-insensitive scan found no password, secret, API key, access token, or private-key marker. No project file outside generated directories exceeded **1 MiB**. The worktree was clean before this verification record was added.

## Order-history invariants, 23 November 2026

Three integration cases were added for invariants already enforced by the loader: a different event cannot claim an existing `(order_id, event_version)`, stable order attributes cannot change in a later version, and an older version arriving after a newer one is retained in history without regressing the current order state. The first two failure cases also verify that the entire input file rolls back.

The image was rebuilt and the complete demo ran from an empty container filesystem. It processed **200 deliveries** into **190 unique events**, **100 current orders**, and **29 date partitions**; all **12 tests passed**. A Docker Desktop bind mount returned macOS error 35 while reading the schema, so the authoritative clean run used the exact built image without host volume mounts. The same image layout is used by hosted CI after checkout.
