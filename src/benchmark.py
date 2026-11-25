"""Run a small, reproducible local ingestion benchmark in temporary directories."""

from __future__ import annotations

import argparse
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from src.check_quality import check_quality
from src.generate_orders import generate_batches, write_batches
from src.lakehouse import run


@dataclass(frozen=True)
class BenchmarkResult:
    orders: int
    deliveries: int
    cold_seconds: float
    replay_seconds: float
    replay_skipped_batches: int
    parquet_bytes: int


def benchmark(order_count: int) -> BenchmarkResult:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        batches = generate_batches(42, order_count)
        write_batches(root / "raw", batches)
        start = time.perf_counter()
        run(root / "raw", root / "warehouse")
        cold_seconds = time.perf_counter() - start
        replay_start = time.perf_counter()
        replay = run(root / "raw", root / "warehouse")
        replay_seconds = time.perf_counter() - replay_start
        if any(outcome != "skipped" for _, outcome in replay):
            raise RuntimeError(f"unchanged replay processed a batch: {replay}")
        summary = check_quality(root / "warehouse")
        artifact_bytes = sum(path.stat().st_size for path in (root / "warehouse").rglob("*.parquet"))
        return BenchmarkResult(
            orders=order_count,
            deliveries=summary["delivered"],
            cold_seconds=cold_seconds,
            replay_seconds=replay_seconds,
            replay_skipped_batches=len(replay),
            parquet_bytes=artifact_bytes,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, nargs="+", default=[100, 1000])
    args = parser.parse_args()
    for order_count in args.orders:
        result = benchmark(order_count)
        print(
            f"orders={result.orders} deliveries={result.deliveries} "
            f"cold_seconds={result.cold_seconds:.3f} "
            f"replay_seconds={result.replay_seconds:.3f} "
            f"replay_skipped_batches={result.replay_skipped_batches} "
            f"parquet_bytes={result.parquet_bytes}"
        )


if __name__ == "__main__":
    main()
