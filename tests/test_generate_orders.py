import unittest
from collections import Counter
from datetime import datetime

from src.generate_orders import generate_batches


class GenerateOrdersTests(unittest.TestCase):
    def test_fixture_is_deterministic_with_updates_and_duplicate_deliveries(self):
        batches = generate_batches(42, 100)
        self.assertEqual(batches, generate_batches(42, 100))
        self.assertNotEqual(batches, generate_batches(43, 100))
        self.assertEqual([len(batch) for batch in batches.values()], [100, 75, 25])
        delivered = [event for batch in batches.values() for event in batch]
        self.assertEqual(len(delivered), 200)
        self.assertEqual(len({event["event_id"] for event in delivered}), 190)
        self.assertEqual(len({event["order_id"] for event in delivered}), 100)
        self.assertEqual(Counter(event["status"] for event in delivered)["refunded"], 20)
        latest = {}
        for event in delivered:
            if event["order_id"] not in latest or event["event_version"] > latest[event["order_id"]]["event_version"]:
                latest[event["order_id"]] = event
        self.assertEqual(
            Counter(event["status"] for event in latest.values()),
            Counter({"paid": 40, "placed": 30, "refunded": 20, "cancelled": 10}),
        )

    def test_versions_keep_identity_and_late_event_time(self):
        batches = generate_batches(42, 100)
        unique = {event["event_id"]: event for batch in batches.values() for event in batch}
        by_order = {}
        for event in unique.values():
            by_order.setdefault(event["order_id"], []).append(event)
        for changes in by_order.values():
            changes.sort(key=lambda event: event["event_version"])
            self.assertEqual([event["event_version"] for event in changes], list(range(1, len(changes) + 1)))
            self.assertEqual(len({(event["customer_id"], event["order_date"], event["amount_cents"], event["currency"]) for event in changes}), 1)
        first_times = [datetime.fromisoformat(event["updated_at"].replace("Z", "+00:00")) for event in batches["batch-001.jsonl"]]
        late_times = [datetime.fromisoformat(event["updated_at"].replace("Z", "+00:00")) for event in batches["batch-003.jsonl"]]
        self.assertTrue(any(timestamp < max(first_times) for timestamp in late_times))


if __name__ == "__main__":
    unittest.main()
