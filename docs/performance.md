# Local benchmark and cost note

Measured on 18 November 2026 with DuckDB 2.0.0 in the Dockerized Python 3.12 runner. `python -m src.benchmark --orders 100 1000` generated input before timing, then timed ingestion, validation, DuckDB writes, and Parquet export. The output directory was fresh for each run. Container startup and fixture generation were outside the timer.

| Orders | Delivered JSONL lines | Timed ingestion | Parquet bytes |
| ---: | ---: | ---: | ---: |
| 100 | 200 | 0.539 s | 96,782 |
| 1,000 | 1,910 | 3.499 s | 181,429 |

These small single-run measurements are illustrative, not a throughput promise. Processing is row-wise so every event can be validated and checked against existing IDs; this adds DuckDB round trips. Silver and gold each create one file per affected order date, which adds small-file overhead. For larger data, batching lookups and writes and compacting partitions would be the next engineering steps. The project keeps exact integer cents and stable replay behavior ahead of peak throughput.
