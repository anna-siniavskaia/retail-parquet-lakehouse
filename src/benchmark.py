"""Run a small, reproducible local ingestion benchmark in temporary directories."""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from src.check_quality import check_quality
from src.generate_orders import generate_batches, write_batches
from src.lakehouse import run


def benchmark(order_count: int) -> tuple[int, float, int]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        batches = generate_batches(42, order_count)
        write_batches(root / "raw", batches)
        start = time.perf_counter()
        run(root / "raw", root / "warehouse")
        elapsed = time.perf_counter() - start
        summary = check_quality(root / "warehouse")
        artifact_bytes = sum(path.stat().st_size for path in (root / "warehouse").rglob("*.parquet"))
        return summary["delivered"], elapsed, artifact_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, nargs="+", default=[100, 1000])
    args = parser.parse_args()
    for order_count in args.orders:
        deliveries, elapsed, artifact_bytes = benchmark(order_count)
        print(
            f"orders={order_count} deliveries={deliveries} "
            f"ingest_seconds={elapsed:.3f} parquet_bytes={artifact_bytes}"
        )


if __name__ == "__main__":
    main()
