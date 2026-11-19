# Private release checklist

Use this checklist for the project's separate private GitHub release. Keep each checked item tied to reproducible evidence.

## Local evidence

- [x] All orders, customers, events, and amounts are deterministic synthetic fixtures.
- [x] Generated JSONL, DuckDB state, and Parquet output are ignored by Git.
- [x] A fresh run processed three batches with 200 deliveries, 190 unique events, and 10 intentional duplicates.
- [x] An unchanged replay skipped all three files by SHA-256 digest.
- [x] Schema drift and invalid records are quarantined with a reason.
- [x] Conflicting IDs and changed processed files fail without committing the batch.
- [x] Only affected order-date partitions are rewritten.
- [x] Pending exports recover after a database commit that precedes Parquet publication.
- [x] Bronze, silver, gold, and quarantine exports are reconciled with DuckDB state.
- [x] The documented 100-order benchmark is reproducible with `python -m src.benchmark`.

See [verification.md](verification.md) and [performance.md](performance.md) for observed results and limitations.

## Before the first push

- [ ] Run the complete Docker demo and all nine tests from a clean `data/` and `warehouse/` directory.
- [ ] Review `git status` and the complete tracked-file list for credentials, generated data, and large files.
- [ ] Create the GitHub repository as private and push `main` on its planned release date.

## After the first push

- [ ] Confirm the GitHub Actions workflow rebuilds and replays the lakehouse successfully.
- [ ] Open every README documentation link and confirm the Mermaid diagram renders.
- [ ] Keep the repository private until Anna makes a separate visibility decision.
