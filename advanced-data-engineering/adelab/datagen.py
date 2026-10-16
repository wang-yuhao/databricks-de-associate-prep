"""Deterministic synthetic data for the course demos and labs (no external downloads needed)."""
from __future__ import annotations

import csv
import json
import os
import random
import zlib
from datetime import datetime, timedelta
from pathlib import Path

STATUSES = ["ACTIVE", "INACTIVE", "PENDING", "ERROR"]
FIRST = ["Ana", "Ben", "Chen", "Dara", "Eli", "Fatima", "Gus", "Hana", "Ivan", "Jada", "Kofi", "Lena",
         "Mateo", "Nora", "Omar", "Priya", "Quinn", "Rosa", "Sven", "Tara", "Uma", "Vik", "Wen", "Yara"]
LAST = ["Ahmed", "Brown", "Costa", "Dubois", "Evans", "Fischer", "Garcia", "Hansen", "Ito", "Jensen",
        "Kim", "Lopez", "Muller", "Novak", "Okafor", "Patel", "Rossi", "Silva", "Tanaka", "Weber"]
CITIES = {"EU": [("Berlin", "10115"), ("Paris", "75001"), ("Madrid", "28001"), ("Amsterdam", "1011"),
                 ("Vienna", "1010")],
          "US": [("Seattle", "98101"), ("Austin", "73301"), ("Boston", "02108"), ("Denver", "80202"),
                 ("Chicago", "60601")]}
CATEGORIES = {"Laptops": (650, 2400), "Phones": (180, 1300), "Audio": (25, 450), "Monitors": (120, 900),
              "Accessories": (5, 120)}


def _next_index(folder: Path, suffix: str) -> int:
    return len([f for f in folder.glob(f"*{suffix}")]) if folder.exists() else 0


def _write_ndjson(path: Path, rows: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return str(path)


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return str(path)


# =========================================================================== Course 1


def land_events(folder: str, flow: str, rows: int = 50, bad_ratio: float = 0.2,
                null_event_id: bool = False, seed: int | None = None) -> str:
    """Device events for the multi-flow demo. ~bad_ratio of rows are dirty:
    40% null event_ts, 35% invalid status, 25% negative amount (warn-only)."""
    folder = Path(folder)
    idx = _next_index(folder, ".json")
    rng = random.Random(seed if seed is not None else zlib.crc32(f"{flow}-{idx}".encode()))
    base, out = datetime(2026, 10, 1), []
    for r in range(rows):
        row = {"event_id": f"{flow}-{idx}-{r}", "device_id": f"dev-{rng.randint(1, 20):03d}",
               "status": rng.choice(STATUSES), "amount": round(rng.uniform(1, 500), 2),
               "event_ts": (base + timedelta(days=rng.randint(0, 4), minutes=rng.randint(0, 1439)))
               .strftime("%Y-%m-%d %H:%M:%S")}
        roll = rng.random()
        if roll < bad_ratio * 0.40:
            row["event_ts"] = None
        elif roll < bad_ratio * 0.75:
            row["status"] = rng.choice(["UNKNOWN", "", "active!"])
        elif roll < bad_ratio:
            row["amount"] = -row["amount"]
        out.append(row)
    if null_event_id:
        out.append({"event_id": None, "device_id": "dev-999", "status": "ACTIVE", "amount": 1.0,
                    "event_ts": "2026-10-01 00:00:00"})
    return _write_ndjson(folder / f"part-{idx:03d}.json", out)


def land_multiplex(folder: str, rows: int = 300, seed: int | None = None) -> str:
    """Kafka-style multiplexed records: one stream, several topics with different payload schemas."""
    folder = Path(folder)
    idx = _next_index(folder, ".json")
    rng = random.Random(seed if seed is not None else 1000 + idx)
    base = datetime(2026, 10, 1) + timedelta(hours=6 * idx)
    out = []
    for r in range(rows):
        ts = base + timedelta(seconds=rng.randint(0, 6 * 3600))
        topic = rng.choices(["orders", "customers", "clickstream"], weights=[4, 1, 5])[0]
        if topic == "orders":
            payload = {"order_id": f"o-{idx}-{r}", "customer_id": f"c-{rng.randint(1, 60):03d}",
                       "items": rng.randint(1, 5), "total": round(rng.uniform(10, 900), 2),
                       "order_ts": ts.isoformat(sep=" ")}
            key = payload["order_id"]
        elif topic == "customers":
            region = rng.choice(["EU", "US"])
            city, _ = rng.choice(CITIES[region])
            cid = f"c-{rng.randint(1, 60):03d}"
            payload = {"customer_id": cid, "name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                       "city": city, "region": region, "updated_ts": ts.isoformat(sep=" ")}
            key = cid
        else:
            payload = {"session_id": f"s-{rng.randint(1, 120)}", "page": rng.choice(
                ["/", "/search", "/product", "/cart", "/checkout"]), "device": rng.choice(
                ["mobile", "desktop", "tablet"]), "event_ts": ts.isoformat(sep=" ")}
            key = payload["session_id"]
        if rng.random() < 0.03:  # a few corrupt payloads to watch in the quarantine
            payload = "{not valid json"
        out.append({"topic": topic, "key": key, "partition": rng.randint(0, 3), "offset": idx * rows + r,
                    "timestamp": ts.isoformat(sep=" "),
                    "value": payload if isinstance(payload, str) else json.dumps(payload)})
    return _write_ndjson(folder / f"multiplex-{idx:03d}.json", out)


CUSTOMER_CDC_BATCHES = [
    # batch 0: initial load
    [{"customer_id": 1, "name": "Ana Silva", "email": "ana@example.com", "city": "Lisbon", "operation": "INSERT", "sequence_num": 1},
     {"customer_id": 2, "name": "Ben Kim", "email": "ben@example.com", "city": "Seoul", "operation": "INSERT", "sequence_num": 1},
     {"customer_id": 3, "name": "Chen Ito", "email": "chen@example.com", "city": "Osaka", "operation": "INSERT", "sequence_num": 1},
     {"customer_id": 4, "name": "Dara Novak", "email": "dara@example.com", "city": "Prague", "operation": "INSERT", "sequence_num": 1}],
    # batch 1: updates, a delete, a new customer, and two changes for one key in the same batch
    [{"customer_id": 1, "name": "Ana Silva", "email": "ana@example.com", "city": "Porto", "operation": "UPDATE", "sequence_num": 2},
     {"customer_id": 2, "name": "Ben Kim", "email": "ben.kim@example.com", "city": "Seoul", "operation": "UPDATE", "sequence_num": 2},
     {"customer_id": 2, "name": "Ben Kim", "email": "ben.kim@example.com", "city": "Busan", "operation": "UPDATE", "sequence_num": 4},
     {"customer_id": 3, "name": None, "email": None, "city": None, "operation": "DELETE", "sequence_num": 3},
     {"customer_id": 5, "name": "Eli Weber", "email": "eli@example.com", "city": "Zurich", "operation": "INSERT", "sequence_num": 1}],
    # batch 2: LATE events (sequence 3 for customer 2 arrives after 4; sequence 2 for customer 3
    # arrives after its delete at 3), a normal update, and a duplicate with no tracked change
    [{"customer_id": 2, "name": "Ben Kim", "email": "ben@example.com", "city": "Incheon", "operation": "UPDATE", "sequence_num": 3},
     {"customer_id": 3, "name": "Chen Ito", "email": "chen@example.com", "city": "Kyoto", "operation": "UPDATE", "sequence_num": 2},
     {"customer_id": 4, "name": "Dara Novak", "email": "dara.n@example.com", "city": "Brno", "operation": "UPDATE", "sequence_num": 2},
     {"customer_id": 1, "name": "Ana Silva", "email": "ana@example.com", "city": "Porto", "operation": "UPDATE", "sequence_num": 3}],
]


def land_customer_cdc(folder: str, batch: int) -> str:
    """Land one of the scripted CDC batches (0, 1, 2) for the AUTO CDC demo."""
    return _write_ndjson(Path(folder) / f"cdc-{batch:03d}.json", CUSTOMER_CDC_BATCHES[batch])


def land_ecommerce(root: str, batch: int = 0, seed: int | None = None) -> dict[str, str]:
    """Multi-source e-commerce lab data.

    root/orders_eu/*.csv       EU web shop, CSV, columns order_id,customer_id,product_id,qty,unit_price,order_ts
    root/orders_us/*.json      US mobile app, JSON, different column names (orderId, custId, sku, quantity, price, ts)
    root/customers_cdc/*.json  customer CDC feed (operation, sequence_num)
    root/products/*.csv        product catalog (snapshot)
    """
    rng = random.Random(seed if seed is not None else 500 + batch)
    r = Path(root)
    base = datetime(2026, 9, 28) + timedelta(days=batch)
    products = [{"product_id": f"p-{i:03d}", "name": f"{cat[:-1] if cat.endswith('s') else cat} {i}",
                 "category": cat, "list_price": round(random.Random(i).uniform(*CATEGORIES[cat]), 2)}
                for i, cat in enumerate([c for c in CATEGORIES for _ in range(6)], start=1)]
    out = {}
    if batch == 0:
        out["products"] = _write_csv(r / "products" / "products.csv", products,
                                     ["product_id", "name", "category", "list_price"])
    eu, us = [], []
    for i in range(120):
        p = rng.choice(products)
        ts = base + timedelta(minutes=rng.randint(0, 1439))
        row = {"order_id": f"EU-{batch}-{i:04d}", "customer_id": f"{rng.randint(1, 40)}",
               "product_id": p["product_id"], "qty": str(rng.randint(1, 4)),
               "unit_price": f"{p['list_price']:.2f}", "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S")}
        roll = rng.random()
        if roll < 0.04:
            row["qty"] = "0"
        elif roll < 0.07:
            row["customer_id"] = ""
        elif roll < 0.09:
            row["unit_price"] = "N/A"
        eu.append(row)
    for i in range(100):
        p = rng.choice(products)
        ts = base + timedelta(minutes=rng.randint(0, 1439))
        row = {"orderId": f"US-{batch}-{i:04d}", "custId": rng.randint(1, 40), "sku": p["product_id"],
               "quantity": rng.randint(1, 4), "price": p["list_price"], "ts": ts.strftime("%Y-%m-%dT%H:%M:%S")}
        roll = rng.random()
        if roll < 0.04:
            row["quantity"] = -1
        elif roll < 0.07:
            row["sku"] = "p-999"  # unknown product
        elif batch >= 1 and roll < 0.12:
            row["coupon"] = "FALL26"  # schema drift from batch 1: new field -> rescued data
        elif batch >= 1 and roll < 0.15:
            row["quantity"] = "two"   # bad type -> rescued data (quantity is hinted as INT)
        us.append(row)
    out["orders_eu"] = _write_csv(r / "orders_eu" / f"orders-{batch:03d}.csv", eu,
                                  ["order_id", "customer_id", "product_id", "qty", "unit_price", "order_ts"])
    out["orders_us"] = _write_ndjson(r / "orders_us" / f"orders-{batch:03d}.json", us)

    cdc = []
    for cid in range(1, 41) if batch == 0 else rng.sample(range(1, 46), 12):
        region = "EU" if cid % 2 else "US"
        city, _ = rng.choice(CITIES[region])
        op = "INSERT" if batch == 0 or cid > 40 else rng.choice(["UPDATE", "UPDATE", "UPDATE", "DELETE"])
        cdc.append({"customer_id": cid, "name": f"{FIRST[cid % len(FIRST)]} {LAST[cid % len(LAST)]}",
                    "email": f"customer{cid}@example.com", "city": city, "region": region,
                    "tier": rng.choice(["bronze", "silver", "gold"]), "operation": op,
                    "sequence_num": batch * 10 + rng.randint(1, 9)})
    out["customers_cdc"] = _write_ndjson(r / "customers_cdc" / f"customers-{batch:03d}.json", cdc)
    return out


# =========================================================================== Course 2


def make_users(n: int = 200, seed: int = 7) -> list[dict]:
    """Users with direct identifiers (name, email, phone, ssn) and quasi-identifiers (dob, zip, gender)."""
    rng = random.Random(seed)
    users = []
    for uid in range(1, n + 1):
        region = "EU" if uid % 3 else "US"
        city, zipc = rng.choice(CITIES[region])
        first, last = rng.choice(FIRST), rng.choice(LAST)
        dob = datetime(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 55))
        users.append({
            "user_id": uid, "first_name": first, "last_name": last,
            "email": f"{first}.{last}{uid}@example.com".lower(),
            "phone": f"+{'49' if region == 'EU' else '1'}-555-{rng.randint(1000000, 9999999)}",
            "ssn": f"{rng.randint(100, 899)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
            "dob": dob.strftime("%Y-%m-%d"), "gender": rng.choice(["F", "M", "X"]),
            "street": f"{rng.randint(1, 250)} {rng.choice(['Main', 'Oak', 'Lake', 'Park', 'Hill'])} St",
            "city": city, "zip": zipc, "region": region, "salary": rng.randint(28, 210) * 1000,
            "signup_ts": (datetime(2024, 1, 1) + timedelta(minutes=rng.randint(0, 900_000))).strftime(
                "%Y-%m-%d %H:%M:%S")})
    return users


def land_users(folder: str, n: int = 200, seed: int = 7) -> str:
    return _write_ndjson(Path(folder) / "users-000.json", make_users(n, seed))


def land_user_updates(folder: str, batch: int, seed: int = 11) -> str:
    """CDC-style user changes (updates and deletes) for the CDF propagation demo/lab."""
    rng = random.Random(seed + batch)
    users = {u["user_id"]: u for u in make_users()}
    rows = []
    for uid in rng.sample(sorted(users), 15):
        u = dict(users[uid])
        if rng.random() < 0.3:
            rows.append({"user_id": uid, "operation": "DELETE", "updated_ts": f"2026-10-{10 + batch:02d} 09:00:00"})
            continue
        city, zipc = rng.choice(CITIES[u["region"]])
        rows.append({**u, "city": city, "zip": zipc, "email": u["email"].replace("@", f"+{batch}@"),
                     "operation": "UPDATE", "updated_ts": f"2026-10-{10 + batch:02d} 09:00:00"})
    return _write_ndjson(Path(folder) / f"user-updates-{batch:03d}.json", rows)


def land_user_events(folder: str, n: int = 2000, seed: int = 3) -> str:
    """Clickstream events referencing user_id, used to show deletes must propagate downstream."""
    rng = random.Random(seed)
    base = datetime(2026, 10, 1)
    rows = [{"event_id": i, "user_id": rng.randint(1, 200), "event_type": rng.choice(
        ["page_view", "add_to_cart", "purchase", "login"]), "event_ts": (base + timedelta(
            seconds=rng.randint(0, 14 * 86400))).strftime("%Y-%m-%d %H:%M:%S"),
        "ip_address": f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"}
        for i in range(1, n + 1)]
    return _write_ndjson(Path(folder) / "user-events-000.json", rows)


def clear(folder: str) -> None:
    import shutil

    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(folder, exist_ok=True)
