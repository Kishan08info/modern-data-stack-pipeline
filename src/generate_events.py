"""Generate a synthetic order-event stream (Kafka-style JSONL).

Simulates what a streaming source would deliver: one JSON event per order
update, delivered in received_at order, with the defects real streams have:
  * duplicate deliveries (same event_id re-sent later),
  * late-arriving events (event_time days before received_at),
  * malformed rows (null/negative amount, unknown status, missing order_id).

Seeded (20261102) so a re-run reproduces the file byte-for-byte.
SYNTHETIC DATA — portfolio demo only, not real company data.
"""
import json
import random
from datetime import datetime, timedelta, timezone

SEED = 20261102
OUT = "data/raw_events.jsonl"
N_BASE = 6000
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports"]
CHANNELS = ["web", "mobile_app", "marketplace"]
STATUSES = ["placed", "paid", "cancelled", "refunded"]


def main() -> None:
    rng = random.Random(SEED)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = []
    for i in range(N_BASE):
        event_time = start + timedelta(
            days=rng.randrange(0, 180),
            seconds=rng.randrange(0, 86400),
        )
        # Most events arrive promptly; ~1% arrive 5-20 days late.
        late = rng.random() < 0.01
        received = event_time + timedelta(
            days=rng.randrange(5, 21) if late else rng.randrange(0, 2),
            seconds=rng.randrange(0, 3600),
        )
        status = rng.choices(
            STATUSES, weights=[0.30, 0.55, 0.10, 0.05]
        )[0]
        amount = round(rng.uniform(9.99, 499.99), 2)
        events.append(
            {
                "event_id": f"evt_{i:06d}",
                "event_time": event_time.isoformat(),
                "received_at": received.isoformat(),
                "order_id": f"ord_{rng.randrange(0, 4200):05d}",
                "customer_id": f"cust_{rng.randrange(0, 1500):04d}",
                "product_category": rng.choice(CATEGORIES),
                "channel": rng.choice(CHANNELS),
                "status": status,
                "amount": amount,
                "currency": "USD",
            }
        )
    # Duplicate deliveries: 150 events re-sent verbatim, later received_at.
    for ev in rng.sample(events, 150):
        dup = dict(ev)
        dup["received_at"] = (
            datetime.fromisoformat(ev["received_at"]) + timedelta(hours=3)
        ).isoformat()
        events.append(dup)
    # Malformed rows: 80, each with exactly one defect.
    defects = ["null_amount", "negative_amount", "unknown_status", "missing_order"]
    for j in range(80):
        ev = dict(rng.choice(events))
        ev["event_id"] = f"evt_bad_{j:03d}"
        kind = defects[j % len(defects)]
        if kind == "null_amount":
            ev["amount"] = None
        elif kind == "negative_amount":
            ev["amount"] = -round(rng.uniform(1, 100), 2)
        elif kind == "unknown_status":
            ev["status"] = "mystery"
        else:
            ev["order_id"] = None
        events.append(ev)
    # Delivery order = received_at order, as a stream would deliver.
    events.sort(key=lambda e: e["received_at"])
    with open(OUT, "w") as f:
        for ev in events:
            f.write(json.dumps(ev, sort_keys=True) + "\n")
    print(f"wrote {len(events)} events to {OUT}")


if __name__ == "__main__":
    main()
