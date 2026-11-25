import unittest

from src.benchmark import benchmark


class BenchmarkTests(unittest.TestCase):
    def test_benchmark_measures_cold_load_and_unchanged_replay(self):
        result = benchmark(10)
        self.assertEqual(result.orders, 10)
        self.assertEqual(result.deliveries, 29)
        self.assertEqual(result.replay_skipped_batches, 3)
        self.assertGreater(result.cold_seconds, 0)
        self.assertGreater(result.replay_seconds, 0)
        self.assertGreater(result.parquet_bytes, 0)


if __name__ == "__main__":
    unittest.main()
