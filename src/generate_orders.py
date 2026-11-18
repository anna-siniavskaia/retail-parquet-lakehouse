"""Generate deterministic fictional order-change files for lakehouse tests."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


START = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def generate_batches(seed: int = 42, order_count: int = 100) -> dict[str, list[dict[str, object]]]:
    if order_count < 10:
        raise ValueError("order_count must be at least 10")
    rng = random.Random(seed)
    orders: list[dict[str, object]] = []
    for number in range(1, order_count + 1):
        order_date = (START + timedelta(days=rng.randrange(30))).date()
        orders.append({
            "order_id": f"order-{number:05d}",
            "customer_id": f"customer-{rng.randint(1, 40):04d}",
            "order_date": order_date.isoformat(),
            "amount_cents": rng.randint(100, 50_000),
            "currency": rng.choice(("USD", "EUR")),
        })

    def change(number: int, version: int, status: str) -> dict[str, object]:
        base = orders[number - 1]
        updated_at = datetime.fromisoformat(base["order_date"]).replace(tzinfo=timezone.utc, hour=12)
        updated_at += timedelta(days=version - 1)
        return {
            "schema_version": 1,
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"retail-orders:{seed}:{number}:{version}")),
            "order_id": base["order_id"],
            "event_version": version,
            "customer_id": base["customer_id"],
            "order_date": base["order_date"],
            "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
            "status": status,
            "amount_cents": base["amount_cents"],
            "currency": base["currency"],
        }

    first = [change(number, 1, "placed") for number in range(1, order_count + 1)]
    updated_numbers = rng.sample(range(1, order_count + 1), int(order_count * 0.7))
    paid_numbers = set(rng.sample(updated_numbers, int(order_count * 0.6)))
    second_unique = [change(number, 2, "paid" if number in paid_numbers else "cancelled") for number in updated_numbers]
    refunded_numbers = rng.sample(sorted(paid_numbers), int(order_count * 0.2))
    third_unique = [change(number, 3, "refunded") for number in refunded_numbers]
    second = second_unique + rng.sample(first, min(5, order_count))
    third = third_unique + rng.sample(second_unique, min(5, len(second_unique)))
    rng.shuffle(second)
    rng.shuffle(third)
    return {
        "batch-001.jsonl": first,
        "batch-002.jsonl": second,
        "batch-003.jsonl": third,
    }


def write_batches(output_dir: Path, batches: dict[str, list[dict[str, object]]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, events in batches.items():
        with (output_dir / filename).open("w", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    batches = generate_batches(args.seed, args.orders)
    write_batches(args.output_dir, batches)
    counts = Counter(event["status"] for events in batches.values() for event in events)
    print(f"Wrote {sum(map(len, batches.values()))} deliveries in {len(batches)} batches; status counts: {dict(counts)}")


if __name__ == "__main__":
    main()
