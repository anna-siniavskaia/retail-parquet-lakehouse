# Gold metric definitions

All metrics use the **latest accepted event version** for each order. A later refund removes that order from current paid revenue. These are current-state metrics, not a ledger of money received and returned over time.

| Field | Definition |
| --- | --- |
| `order_date` | Date the fictional order was placed, independent of file arrival and change time |
| `currency` | Original order currency; USD and EUR are never added into one monetary total |
| `orders_total` | Count of current orders for this order date and currency |
| `placed_orders` | Count whose latest status is `placed` |
| `paid_orders` | Count whose latest status is `paid` |
| `cancelled_orders` | Count whose latest status is `cancelled` |
| `refunded_orders` | Count whose latest status is `refunded` |
| `paid_revenue_cents` | Sum of integer `amount_cents` for current `paid` orders only, stored as Parquet `BIGINT` |

The five status counts sum to `orders_total`. The default fixture ends with 40 paid, 30 placed, 20 refunded, and 10 cancelled orders. Gold status and revenue values are reconciled against the silver/latest-order state after each run.
