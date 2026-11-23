import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from src.check_quality import check_quality
from src.generate_orders import generate_batches, write_batches
from src.lakehouse import initialize, load_validator, process_file, run


class LakehouseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        self.output = self.root / "warehouse"
        self.batches = generate_batches(42, 10)
        write_batches(self.raw, {"batch-001.jsonl": self.batches["batch-001.jsonl"]})

    def write_batch(self, filename, lines):
        (self.raw / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_quarantines_malformed_schema_drift_and_invalid_values(self):
        run(self.raw, self.output)
        original = self.batches["batch-001.jsonl"][0]
        extra = {**original, "new_field": "unexpected"}
        bad_amount = {**original, "amount_cents": 0}
        bad_version = {**original, "schema_version": 2}
        bad_status = {**original, "status": "paid"}
        self.write_batch("batch-002.jsonl", [
            "{bad-json", json.dumps(extra), json.dumps(bad_amount),
            json.dumps(bad_version), json.dumps(bad_status), json.dumps(original),
        ])
        run(self.raw, self.output)
        result = check_quality(self.output)
        self.assertEqual(result["delivered"], 16)
        self.assertEqual(result["unique_events"], 10)
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(result["quarantined"], 5)
        with duckdb.connect(str(self.output / "lakehouse.duckdb"), read_only=True) as connection:
            reasons = connection.execute(
                "SELECT reason FROM core.quarantine ORDER BY line_number"
            ).fetchall()
        self.assertEqual(reasons, [
            ("invalid_json",), ("schema_drift",), ("invalid_value",),
            ("schema_drift",), ("invalid_value",),
        ])

    def test_conflicting_duplicate_rolls_back_file(self):
        run(self.raw, self.output)
        original = self.batches["batch-001.jsonl"][0]
        changed = {**original, "amount_cents": original["amount_cents"] + 1}
        self.write_batch("batch-002.jsonl", [json.dumps(changed)])
        with self.assertRaisesRegex(ValueError, "conflicting content"):
            run(self.raw, self.output)
        self.assertEqual(check_quality(self.output)["delivered"], 10)
        with duckdb.connect(str(self.output / "lakehouse.duckdb"), read_only=True) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM meta.files").fetchone()[0], 1)

    def test_different_event_cannot_claim_an_existing_order_version(self):
        run(self.raw, self.output)
        original = self.batches["batch-001.jsonl"][0]
        conflicting = {
            **original,
            "event_id": "9b12a8aa-a553-4bca-9242-bd93f8239f62",
        }
        self.write_batch("batch-002.jsonl", [json.dumps(conflicting)])
        with self.assertRaisesRegex(ValueError, "conflicting version"):
            run(self.raw, self.output)
        self.assertEqual(check_quality(self.output)["delivered"], 10)

    def test_order_identity_change_rolls_back_the_entire_file(self):
        run(self.raw, self.output)
        original = self.batches["batch-001.jsonl"][0]
        changed_identity = {
            **original,
            "event_id": "3d6d74a3-31c7-49d0-b89d-9a8fb678b67c",
            "event_version": 2,
            "status": "paid",
            "amount_cents": original["amount_cents"] + 1,
        }
        self.write_batch("batch-002.jsonl", [json.dumps(changed_identity)])
        with self.assertRaisesRegex(ValueError, "stable order attributes changed"):
            run(self.raw, self.output)
        self.assertEqual(check_quality(self.output)["delivered"], 10)

    def test_late_older_version_is_retained_without_regressing_current_state(self):
        run(self.raw, self.output)
        version_two = next(
            event for event in self.batches["batch-002.jsonl"] if event["event_version"] == 2
        )
        version_three = {
            **version_two,
            "event_id": "55ed030b-6f38-4767-a08d-b45576c78b56",
            "event_version": 3,
            "status": "refunded",
        }
        self.write_batch("batch-002.jsonl", [json.dumps(version_three)])
        run(self.raw, self.output)
        self.write_batch("batch-003.jsonl", [json.dumps(version_two)])
        run(self.raw, self.output)

        with duckdb.connect(str(self.output / "lakehouse.duckdb"), read_only=True) as connection:
            current = connection.execute(
                "SELECT event_version, status FROM core.orders WHERE order_id = ?",
                [version_two["order_id"]],
            ).fetchone()
            history = connection.execute(
                "SELECT event_version FROM core.events WHERE order_id = ? ORDER BY event_version",
                [version_two["order_id"]],
            ).fetchall()
        self.assertEqual(current, (3, "refunded"))
        self.assertEqual(history, [(1,), (2,), (3,)])
        self.assertEqual(check_quality(self.output)["unique_events"], 12)

    def test_replay_skips_unchanged_file_and_rejects_changed_file(self):
        run(self.raw, self.output)
        self.assertEqual(run(self.raw, self.output), [("batch-001.jsonl", "skipped")])
        with (self.raw / "batch-001.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("\n")
        with self.assertRaisesRegex(ValueError, "processed file changed"):
            run(self.raw, self.output)
        self.assertEqual(check_quality(self.output)["delivered"], 10)

    def test_only_affected_date_partition_is_rewritten(self):
        run(self.raw, self.output)
        change = next(event for event in self.batches["batch-002.jsonl"] if event["event_version"] == 2)
        affected = change["order_date"]
        other_date = next(event["order_date"] for event in self.batches["batch-001.jsonl"] if event["order_date"] != affected)
        affected_path = self.output / "silver" / f"order_date={affected}" / "orders.parquet"
        other_path = self.output / "silver" / f"order_date={other_date}" / "orders.parquet"
        before_affected = affected_path.read_bytes()
        before_other = other_path.stat().st_mtime_ns
        self.write_batch("batch-002.jsonl", [json.dumps(change)])
        run(self.raw, self.output)
        self.assertNotEqual(affected_path.read_bytes(), before_affected)
        self.assertEqual(other_path.stat().st_mtime_ns, before_other)
        self.assertEqual(check_quality(self.output)["unique_events"], 11)

    def test_bad_only_batch_writes_empty_bronze_and_quarantine(self):
        run(self.raw, self.output)
        self.write_batch("batch-002.jsonl", ["{bad-json"])
        run(self.raw, self.output)
        result = check_quality(self.output)
        self.assertEqual(result["delivered"], 11)
        self.assertEqual(result["quarantined"], 1)
        self.assertTrue((self.output / "bronze/batch_id=batch-002/events.parquet").is_file())
        self.assertTrue((self.output / "quarantine/batch_id=batch-002/invalid.parquet").is_file())

    def test_quality_detects_bronze_content_change_without_count_change(self):
        run(self.raw, self.output)
        bronze = self.output / "bronze/batch_id=batch-001/events.parquet"
        replacement = bronze.with_name("replacement.parquet")
        with duckdb.connect() as connection:
            connection.execute(
                "COPY (SELECT * REPLACE ('cancelled' AS status) FROM read_parquet(?)) "
                "TO ? (FORMAT PARQUET)",
                [str(bronze), str(replacement)],
            )
        replacement.replace(bronze)
        with self.assertRaisesRegex(RuntimeError, "bronze Parquet differs"):
            check_quality(self.output)

    def test_pending_exports_are_repaired_after_database_commit(self):
        self.output.mkdir()
        with duckdb.connect(str(self.output / "lakehouse.duckdb")) as connection:
            initialize(connection)
            process_file(connection, self.raw / "batch-001.jsonl", load_validator())
        self.assertFalse((self.output / "bronze/batch_id=batch-001/events.parquet").exists())
        self.assertEqual(run(self.raw, self.output), [("batch-001.jsonl", "skipped")])
        self.assertEqual(check_quality(self.output)["orders"], 10)


if __name__ == "__main__":
    unittest.main()
