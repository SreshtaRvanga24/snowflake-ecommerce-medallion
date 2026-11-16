"""
Daily batch generator for the e-commerce pipeline.

Writes one day's files, in exactly the same format as the original dataset,
to  datasets/batches/<batch-date>/ :

    CUSTOMERS.csv            -> S3     (new customers + a few changed customers, for SCD2)
    ORDERS_RAW.csv           -> S3     (new orders + status updates of earlier orders)
    ORDER_ITEMS.csv          -> S3     (items of the new orders)
    PAYMENTS_RAW.csv         -> Azure  (payment attempts: failures, retries, a duplicate)
    ORDER_STATUS_EVENTS.csv  -> Azure  (CREATED for new orders, progress for open ones)

The file names match what azurite_upload.py and s3_upload.py expect, so:

    python ingestion/generator/generate_daily_batch.py                 # today's batch
    python ingestion/azure/azurite_upload.py --src datasets/batches/<date> --batch-date <date>
    python ingestion/s3/s3_upload.py         --src datasets/batches/<date> --batch-date <date>

Continuity: IDs continue from the last ones used, orders reference real customers
and products, and open orders move CREATED -> PROCESSING -> SHIPPED -> DELIVERED
(sometimes CANCELLED or RETURNED) over the following days. State is kept in
datasets/batches/_state/generator_state.json, built from datasets/raw on the first run.
Standard library only.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

RAW_DIR = Path("datasets/raw")
BATCH_ROOT = Path("datasets/batches")
STATE_FILE = BATCH_ROOT / "_state" / "generator_state.json"
TS_FMT = "%Y-%m-%d %H:%M:%S"

LIFECYCLE = ["CREATED", "PROCESSING", "SHIPPED", "DELIVERED"]
TERMINAL = {"CANCELLED", "RETURNED"}
RETURN_WINDOW_DAYS = 7

HEADERS = {
    "CUSTOMERS.csv": ["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "REGION", "CITY", "SEGMENT",
                      "SIGNUP_DATE", "IS_ACTIVE", "UPDATED_AT"],
    "ORDERS_RAW.csv": ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "ORDER_TS", "ORDER_STATUS",
                       "SALES_CHANNEL", "SUBTOTAL", "DISCOUNT_AMOUNT", "ORDER_AMOUNT",
                       "CURRENCY", "UPDATED_AT"],
    "ORDER_ITEMS.csv": ["ORDER_ITEM_ID", "ORDER_ID", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY",
                        "QUANTITY", "UNIT_PRICE", "LINE_AMOUNT", "UPDATED_AT"],
    "PAYMENTS_RAW.csv": ["PAYMENT_ID", "ORDER_ID", "CUSTOMER_ID", "PAYMENT_TS", "PAYMENT_METHOD",
                         "PAYMENT_STATUS", "AMOUNT", "CURRENCY", "UPDATED_AT"],
    "ORDER_STATUS_EVENTS.csv": ["EVENT_ID", "ORDER_ID", "STATUS", "EVENT_TS", "SOURCE_SYSTEM",
                                "UPDATED_AT"],
}


# ------------------------------------------------------------------ helpers
def read_csv(name: str) -> list[dict]:
    with open(RAW_DIR / name, newline="") as fh:
        return list(csv.DictReader(fh))


def ts(dt: datetime) -> str:
    return dt.strftime(TS_FMT)


def money(x: float) -> str:
    return f"{x:.2f}"


def random_time_on(day: date, start_hour: int = 6, end_hour: int = 20) -> datetime:
    seconds = random.randint(start_hour * 3600, end_hour * 3600)
    return datetime.combine(day, time()) + timedelta(seconds=seconds - seconds % 60)


def later_on(day: date, after: datetime) -> datetime:
    """A time on `day` that is strictly after `after` (or a random time if `after` is earlier)."""
    t = random_time_on(day)
    if t <= after:
        t = after + timedelta(minutes=random.randint(10, 180))
    return t


def common(values) -> list[str]:
    """Distinct non-empty values, most frequent first."""
    return [v for v, _ in Counter(v for v in values if v).most_common()]


# ------------------------------------------------------------------ state
def build_initial_state() -> dict:
    """Learn reference values and the last IDs from the original dataset."""
    customers = read_csv("CUSTOMERS.csv")
    orders = read_csv("ORDERS_RAW.csv")
    items = read_csv("ORDER_ITEMS.csv")
    payments = read_csv("PAYMENTS_RAW.csv")
    events = read_csv("ORDER_STATUS_EVENTS.csv")

    latest_customer = {}
    for c in customers:
        cid = c["CUSTOMER_ID"].strip().upper()
        if cid and (cid not in latest_customer or c["UPDATED_AT"] > latest_customer[cid]["UPDATED_AT"]):
            latest_customer[cid] = c

    region_cities = defaultdict(set)
    for c in latest_customer.values():
        if c["REGION"] and c["CITY"]:
            region_cities[c["REGION"].strip()].add(c["CITY"].strip())

    prices = defaultdict(list)
    product_info = {}
    for i in items:
        pid = i["PRODUCT_ID"].strip().upper()
        try:
            prices[pid].append(float(i["UNIT_PRICE"]))
        except ValueError:
            continue
        product_info[pid] = {"name": i["PRODUCT_NAME"].strip(), "category": i["CATEGORY"].strip()}
    products = {pid: {**info, "base_price": round(statistics.median(prices[pid]), 2)}
                for pid, info in product_info.items() if prices[pid]}

    def max_num(ids, prefix):
        nums = [int(x.strip().upper().replace(prefix, "")) for x in ids
                if x.strip().upper().startswith(prefix) and x.strip().upper().replace(prefix, "").isdigit()]
        return max(nums) if nums else 0

    return {
        "last_batch_date": None,
        "next_customer": max_num(latest_customer.keys(), "C") + 1,
        "next_order": max_num((o["ORDER_ID"] for o in orders), "O") + 1,
        "customers": {
            cid: {"name": c["CUSTOMER_NAME"], "email": c["EMAIL"], "region": c["REGION"],
                  "city": c["CITY"], "segment": c["SEGMENT"], "signup_date": c["SIGNUP_DATE"],
                  "is_active": c["IS_ACTIVE"] or "TRUE"}
            for cid, c in latest_customer.items()
        },
        "products": products,
        "region_cities": {r: sorted(cs) for r, cs in region_cities.items()},
        "segments": common(c["SEGMENT"] for c in customers),
        "channels": common(o["SALES_CHANNEL"] for o in orders),
        "currency": (common(o["CURRENCY"] for o in orders) or ["INR"])[0],
        "payment_methods": common(p["PAYMENT_METHOD"] for p in payments),
        "event_sources": common(e["SOURCE_SYSTEM"] for e in events),
        # Only orders created by this generator are progressed; historical stuck
        # orders are left alone (they are a data finding, not something to fix).
        "open_orders": {},
    }


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    state = build_initial_state()
    print(f"Initial state built from {RAW_DIR}: next customer C{state['next_customer']:04d}, "
          f"next order O{state['next_order']:05d}, {len(state['products'])} products")
    return state


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=1))


# ------------------------------------------------------------------ generation
def generate(state: dict, day: date, n_orders: int, inject_issues: bool) -> dict[str, list[list]]:
    out = {name: [] for name in HEADERS}
    now_str = lambda dt: ts(dt)

    # ---- customers: a few new ones
    for _ in range(random.randint(1, 3)):
        num = state["next_customer"]
        state["next_customer"] += 1
        cid = f"C{num:04d}"
        region = random.choice(list(state["region_cities"]))
        cust = {"name": f"Customer {num:03d}", "email": f"customer{num:03d}@example.com",
                "region": region, "city": random.choice(state["region_cities"][region]),
                "segment": random.choice(state["segments"]), "signup_date": day.isoformat(),
                "is_active": "TRUE"}
        if inject_issues and random.random() < 0.2:
            cust["email"] = ""                              # same issue as the original data
        state["customers"][cid] = cust
        updated = random_time_on(day)
        out["CUSTOMERS.csv"].append([cid, cust["name"], cust["email"], cust["region"], cust["city"],
                                     cust["segment"], cust["signup_date"], cust["is_active"], now_str(updated)])

    # ---- customers: 1-2 existing ones change segment or city (drives the SCD2 snapshot)
    existing = [c for c in state["customers"] if c in state["customers"]]
    for cid in random.sample(existing, k=min(len(existing), random.randint(1, 2))):
        cust = state["customers"][cid]
        if random.random() < 0.5 and len(state["segments"]) > 1:
            cust["segment"] = random.choice([s for s in state["segments"] if s != cust["segment"]])
        else:
            region = random.choice(list(state["region_cities"]))
            cust["region"], cust["city"] = region, random.choice(state["region_cities"][region])
        updated = random_time_on(day)
        out["CUSTOMERS.csv"].append([cid, cust["name"], cust["email"], cust["region"], cust["city"],
                                     cust["segment"], cust["signup_date"], cust["is_active"], now_str(updated)])

    def emit_order(order_id: str, o: dict, status: str, updated: datetime) -> None:
        out["ORDERS_RAW.csv"].append([order_id, o["customer_id"], o["order_date"], o["order_ts"], status,
                                      o["channel"], o["subtotal"], o["discount"], o["amount"],
                                      state["currency"], now_str(updated)])

    def emit_event(order_id: str, o: dict, status: str, when: datetime) -> None:
        o["event_seq"] += 1
        out["ORDER_STATUS_EVENTS.csv"].append([
            f"EV-{order_id}-{o['event_seq']}", order_id, status, now_str(when),
            random.choice(state["event_sources"]),
            now_str(when + timedelta(minutes=random.randint(1, 20)))])
        o["status"], o["last_event_ts"] = status, now_str(when)

    # ---- progress orders created on earlier days
    advance_prob = {"CREATED": 0.9, "PROCESSING": 0.7, "SHIPPED": 0.6}
    for order_id, o in list(state["open_orders"].items()):
        last = datetime.strptime(o["last_event_ts"], TS_FMT)
        if o["status"] == "DELIVERED":
            if (day - last.date()).days > RETURN_WINDOW_DAYS:
                del state["open_orders"][order_id]          # return window closed
            elif random.random() < 0.03:
                when = later_on(day, last)
                emit_event(order_id, o, "RETURNED", when)
                emit_order(order_id, o, "RETURNED", when)
                del state["open_orders"][order_id]
            continue
        if o["status"] in ("CREATED", "PROCESSING") and random.random() < 0.02:
            when = later_on(day, last)
            emit_event(order_id, o, "CANCELLED", when)
            emit_order(order_id, o, "CANCELLED", when)
            del state["open_orders"][order_id]
            continue
        if random.random() < advance_prob.get(o["status"], 0):
            nxt = LIFECYCLE[LIFECYCLE.index(o["status"]) + 1]
            when = later_on(day, last)
            emit_event(order_id, o, nxt, when)
            emit_order(order_id, o, nxt, when)               # later version of the order row

    # ---- new orders
    customer_ids = list(state["customers"])
    product_ids = list(state["products"])
    for _ in range(n_orders):
        num = state["next_order"]
        state["next_order"] += 1
        order_id = f"O{num:05d}"
        cid = random.choice(customer_ids)
        order_ts = random_time_on(day)

        subtotal = 0.0
        for line in range(1, random.randint(1, 3) + 1):
            pid = random.choice(product_ids)
            prod = state["products"][pid]
            qty = random.randint(1, 3)
            unit = round(prod["base_price"] * random.uniform(0.9, 1.1), 2)
            line_amount = round(qty * unit, 2)
            subtotal += line_amount
            out["ORDER_ITEMS.csv"].append([f"{order_id}-I{line}", order_id, pid, prod["name"], prod["category"],
                                           qty, money(unit), money(line_amount), now_str(order_ts)])
        subtotal = round(subtotal, 2)
        discount = round(subtotal * random.choice([0, 0, 0.05, 0.1, 0.15]), 2)
        amount = round(subtotal - discount, 2)

        o = {"customer_id": cid, "order_date": day.isoformat(), "order_ts": now_str(order_ts),
             "channel": random.choice(state["channels"]), "subtotal": money(subtotal),
             "discount": money(discount), "amount": money(amount), "event_seq": 0,
             "status": "CREATED", "last_event_ts": now_str(order_ts)}
        emit_order(order_id, o, "CREATED", order_ts)
        emit_event(order_id, o, "CREATED", order_ts + timedelta(minutes=random.randint(1, 30)))
        state["open_orders"][order_id] = o

        # payments: usually one success; sometimes a failure then a retry; rarely a duplicate row
        method = random.choice(state["payment_methods"])
        pay_ts = order_ts + timedelta(minutes=random.randint(2, 60))
        attempt = 1
        if random.random() < 0.12:
            out["PAYMENTS_RAW.csv"].append([f"PAY{num:05d}-{attempt}", order_id, cid, now_str(pay_ts), method,
                                            "FAILED", money(amount), state["currency"], now_str(pay_ts)])
            attempt += 1
            pay_ts += timedelta(minutes=random.randint(3, 30))
        success_row = [f"PAY{num:05d}-{attempt}", order_id, cid, now_str(pay_ts), method,
                       "SUCCESS", money(amount), state["currency"], now_str(pay_ts)]
        out["PAYMENTS_RAW.csv"].append(success_row)
        if inject_issues and random.random() < 0.03:
            out["PAYMENTS_RAW.csv"].append(list(success_row))   # exact duplicate (Silver dedups it)

    return out


# ------------------------------------------------------------------ main
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", default=date.today().isoformat(), help="YYYY-MM-DD (default: today)")
    parser.add_argument("--orders", type=int, default=40, help="new orders in this batch")
    parser.add_argument("--no-issues", action="store_true", help="skip deliberate data-quality issues")
    parser.add_argument("--seed", type=int, help="random seed, for reproducible batches")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    day = date.fromisoformat(args.batch_date)
    state = load_state()

    if state["last_batch_date"] and day.isoformat() <= state["last_batch_date"]:
        raise SystemExit(f"Batch {day} is not after the last generated batch "
                         f"({state['last_batch_date']}). Batches must move forward in time.")

    out_dir = BATCH_ROOT / day.isoformat()
    if out_dir.exists():
        raise SystemExit(f"{out_dir} already exists; refusing to overwrite.")

    batch = generate(state, day, args.orders, inject_issues=not args.no_issues)

    out_dir.mkdir(parents=True)
    for name, rows in batch.items():
        with open(out_dir / name, "w", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")   # LF, like the source files
            writer.writerow(HEADERS[name])
            writer.writerows(rows)

    state["last_batch_date"] = day.isoformat()
    save_state(state)                                        # only after the files are written

    print(f"Batch {day} written to {out_dir}")
    for name, rows in batch.items():
        print(f"  {name:<26} {len(rows):>4} rows")
    print(f"  open orders carried forward: {len(state['open_orders'])}")


if __name__ == "__main__":
    main()