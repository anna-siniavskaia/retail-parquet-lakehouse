# Deterministic test fixture

The default `--seed 42 --orders 100` generator writes three JSON Lines files:

| File | Contents | Deliveries |
| --- | --- | ---: |
| `batch-001.jsonl` | Initial `placed` version for 100 orders | 100 |
| `batch-002.jsonl` | 60 paid and 10 cancelled version-2 changes, plus five repeated version-1 deliveries | 75 |
| `batch-003.jsonl` | 20 refunded version-3 changes, plus five repeated version-2 deliveries | 25 |

There are 200 delivered lines, 190 distinct `event_id` values, and 100 distinct orders. After all batches, the latest statuses should be **40 paid**, **30 placed**, **20 refunded**, and **10 cancelled**. These counts are fixtures for reconciliation tests, not business observations.

The third batch includes changes whose `updated_at` is earlier than timestamps already seen in the first batch. The ingestion manifest should follow file arrival order; silver should select the highest `event_version` for each order. Duplicate deliveries must preserve identical event content. A conflicting copy of an `event_id` or `(order_id, event_version)` is a data-quality failure.

`amount_cents` is integer money in the original currency. Gold tables should group USD and EUR separately. A refunded order contributes zero to current paid revenue; it should remain visible in order-status counts.
