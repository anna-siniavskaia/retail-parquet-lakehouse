# Local benchmark and cost note

Measured again on 25 November 2026 with DuckDB 2.0.0 in the Dockerized Python 3.12 runner. `python -m src.benchmark --orders 100 1000` generated input before timing, then measured both cold ingestion and an unchanged three-batch replay. The output directory was fresh for each order count. Container startup and fixture generation were outside the timer.

| Orders | Delivered JSONL lines | Cold ingestion | Unchanged replay | Skipped batches | Parquet bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 200 | 0.525 s | 0.014 s | 3 | 96,782 |
| 1,000 | 1,910 | 3.258 s | 0.015 s | 3 | 181,429 |

These small single-run measurements are illustrative, not a throughput promise. The replay timing demonstrates manifest-based skipping rather than reprocessing, and the benchmark fails if any unchanged batch is not reported as skipped. Cold processing is row-wise so every event can be validated and checked against existing IDs; this adds DuckDB round trips. Silver and gold each create one file per affected order date, which adds small-file overhead. For larger data, batching lookups and writes and compacting partitions would be the next engineering steps. The project keeps exact integer cents and stable replay behavior ahead of peak throughput.
