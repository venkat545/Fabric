# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "56475ef2-9188-4ae5-b425-3c2821af0ecd",
# META       "default_lakehouse_name": "Demo_LH",
# META       "default_lakehouse_workspace_id": "133f8506-14d5-4df8-89fc-dc5391fbb7d8",
# META       "known_lakehouses": [
# META         {
# META           "id": "56475ef2-9188-4ae5-b425-3c2821af0ecd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
import json
import math
import uuid
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def to_jsonl(filepath, records):
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def random_phone(rng):
    return f"+91-{rng.integers(6000000000, 9999999999)}"

def random_email(first, last, rng):
    domains = ["gmail.com", "outlook.com", "contoso.com", "yahoo.com"]
    base = f"{first}.{last}".lower()
    if rng.random() < 0.25:
        base += str(rng.integers(1, 9999))
    return f"{base}@{rng.choice(domains)}"

def add_noise_whitespace(val, rng, p=0.02):
    if rng.random() < p and isinstance(val, str):
        return (" " * rng.integers(1, 3)) + val + (" " * rng.integers(1, 3))
    return val

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# -------------------------
# CONFIG (edit as needed)
# -------------------------
SEED = 42
START_DATE = "2026-01-01"
END_DATE   = "2026-01-07"   # start small, then increase (e.g., month)

N_STORES    = 25
N_PRODUCTS  = 500
N_CUSTOMERS = 8000
N_PROMOS    = 40

AVG_ORDERS_PER_DAY = 600
MAX_LINES_PER_ORDER = 6

LATE_ARRIVAL_RATE = 0.02
RETURN_RATE = 0.03
INVENTORY_EVERY_N_DAYS = 7

# -------------------------
# Fabric Lakehouse Files Path
# -------------------------
# This writes into your Lakehouse -> Files -> retail_project
BASE_PATH = "/lakehouse/default/Files/retail_project"

BRONZE_ROOT = f"{BASE_PATH}/bronze"
MASTER_DIR  = f"{BRONZE_ROOT}/master"

print("Writing to:", BRONZE_ROOT)

# Seed
rng = np.random.default_rng(SEED)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def gen_stores(n_stores, rng):
    cities = [
        ("Hyderabad", "TG"), ("Bengaluru", "KA"), ("Chennai", "TN"),
        ("Mumbai", "MH"), ("Delhi", "DL"), ("Pune", "MH"),
        ("Kolkata", "WB"), ("Ahmedabad", "GJ")
    ]
    formats = ["Superstore", "Express", "Outlet"]
    rows = []
    for i in range(1, n_stores + 1):
        city, state = cities[int(rng.integers(0, len(cities)))]
        fmt = rng.choice(formats, p=[0.55, 0.35, 0.10])
        store_id = f"S{i:04d}"
        rows.append({
            "store_id": store_id,
            "store_name": f"{city} {fmt} #{i}",
            "format": fmt,
            "city": city,
            "state": state,
            "opened_date": (date.today() - timedelta(days=int(rng.integers(365, 3650)))).isoformat(),
            "is_active": True if rng.random() > 0.03 else False
        })
    return pd.DataFrame(rows)

def gen_products(n_products, rng):
    categories = [
        ("Grocery", ["Rice", "Flour", "Snacks", "Beverages", "Spices"]),
        ("HomeCare", ["Detergent", "Cleaner", "Tissues", "Freshener"]),
        ("PersonalCare", ["Shampoo", "Soap", "Toothpaste", "Deodorant"]),
        ("Electronics", ["Earbuds", "Charger", "Cable", "PowerBank"]),
        ("Apparel", ["TShirt", "Jeans", "Socks", "Jacket"])
    ]
    brands = ["Fabrikam", "Contoso", "Northwind", "AdventureWorks", "Tailspin"]
    rows = []
    for i in range(1, n_products + 1):
        cat, subcats = categories[int(rng.integers(0, len(categories)))]
        subcat = rng.choice(subcats)
        brand = rng.choice(brands)
        product_id = f"P{i:06d}"
        base_price = float(np.round(rng.uniform(20, 5000), 2))
        cost = float(np.round(base_price * rng.uniform(0.45, 0.8), 2))
        rows.append({
            "product_id": product_id,
            "product_name": f"{brand} {subcat} {i}",
            "brand": brand,
            "category": cat,
            "subcategory": subcat,
            "base_price": base_price,
            "unit_cost": cost,
            "is_discontinued": True if rng.random() < 0.02 else False
        })
    return pd.DataFrame(rows)

def gen_customers(n_customers, rng):
    first_names = ["Aarav", "Vivaan", "Aditya", "Sai", "Arjun", "Isha", "Ananya", "Diya", "Meera", "Saanvi"]
    last_names = ["Reddy", "Sharma", "Kumar", "Patel", "Gupta", "Nair", "Iyer", "Rao", "Singh", "Das"]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    tier_weights = [0.55, 0.30, 0.12, 0.03]

    rows = []
    for i in range(1, n_customers + 1):
        first = rng.choice(first_names)
        last = rng.choice(last_names)
        customer_id = f"C{i:07d}"
        signup = date.today() - timedelta(days=int(rng.integers(1, 3650)))
        tier = rng.choice(tiers, p=np.array(tier_weights)/sum(tier_weights))
        rows.append({
            "customer_id": customer_id,
            "first_name": first,
            "last_name": last,
            "email": random_email(first, last, rng),
            "phone": random_phone(rng),
            "signup_date": signup.isoformat(),
            "loyalty_tier": tier,
            "marketing_opt_in": True if rng.random() > 0.25 else False
        })
    return pd.DataFrame(rows)

def gen_promotions(n_promos, start_dt, end_dt, rng):
    promo_types = ["PERCENT_OFF", "BOGO", "FLAT_OFF", "CATEGORY_DISCOUNT"]
    rows = []
    total_days = (end_dt - start_dt).days + 1

    for i in range(1, n_promos + 1):
        promo_id = f"PR{i:05d}"
        promo_type = rng.choice(promo_types, p=[0.55, 0.10, 0.25, 0.10])
        duration = int(rng.integers(7, 30))
        start_offset = int(rng.integers(0, max(1, total_days - duration)))
        s = start_dt + timedelta(days=start_offset)
        e = min(end_dt, s + timedelta(days=duration))

        if promo_type == "PERCENT_OFF":
            val = int(rng.integers(5, 35))
        elif promo_type == "FLAT_OFF":
            val = int(rng.integers(20, 500))
        elif promo_type == "BOGO":
            val = 1
        else:
            val = int(rng.integers(5, 25))

        rows.append({
            "promo_id": promo_id,
            "promo_name": f"{promo_type}_{val}_{i}",
            "promo_type": promo_type,
            "promo_value": val,
            "start_date": s.isoformat(),
            "end_date": e.isoformat(),
            "is_active": True
        })
    return pd.DataFrame(rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def gen_orders_for_day(day, stores_df, customers_df, promos_df, rng,
                       avg_orders_per_day, online_share=0.35,
                       late_arrival_rate=0.02):
    store_ids = stores_df["store_id"].tolist()
    cust_ids = customers_df["customer_id"].tolist()
    promo_ids = promos_df["promo_id"].tolist()

    n_orders = int(rng.poisson(lam=avg_orders_per_day))
    if n_orders < 1:
        n_orders = int(rng.integers(1, 5))

    orders = []
    for _ in range(n_orders):
        order_id = f"O{uuid.uuid4().hex[:12].upper()}"
        channel = "Online" if rng.random() < online_share else "Store"
        store_id = None if channel == "Online" else rng.choice(store_ids)

        customer_id = rng.choice(cust_ids)
        if rng.random() < 0.005:  # missing customer sometimes
            customer_id = None

        order_ts = datetime.combine(day, datetime.min.time()) + timedelta(
            seconds=int(rng.integers(0, 86400))
        )

        promo_id = None
        if rng.random() < 0.22 and len(promo_ids) > 0:
            promo_id = rng.choice(promo_ids)
        if rng.random() < 0.01:
            promo_id = "PR99999"  # invalid

        payment_methods = ["UPI", "Card", "Cash", "Wallet", "NetBanking"]
        pay = rng.choice(payment_methods, p=[0.38, 0.32, 0.18, 0.08, 0.04])

        ingestion_date = day
        if rng.random() < late_arrival_rate:
            ingestion_date = day + timedelta(days=int(rng.integers(1, 5)))

        orders.append({
            "order_id": order_id,
            "event_date": day.isoformat(),
            "order_timestamp": order_ts.isoformat(timespec="seconds"),
            "ingestion_date": ingestion_date.isoformat(),
            "channel": channel,
            "store_id": store_id,
            "customer_id": customer_id,
            "payment_method": pay,
            "currency": "INR" if rng.random() > 0.003 else "USD",
            "order_status": rng.choice(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"], p=[0.25, 0.25, 0.45, 0.05]),
            "promo_id": promo_id
        })

    # occasional duplicate orders
    if len(orders) > 10 and rng.random() < 0.05:
        orders.append(orders[int(rng.integers(0, len(orders)))].copy())

    return orders


def gen_order_lines_for_orders(orders, products_df, rng, max_lines_per_order=6):
    product_ids = products_df["product_id"].tolist()
    base_price_map = dict(zip(products_df["product_id"], products_df["base_price"]))

    rows = []
    for o in orders:
        n_lines = int(rng.integers(1, max_lines_per_order + 1))
        chosen_products = rng.choice(product_ids, size=n_lines, replace=False if n_lines <= len(product_ids) else True)

        for line_num, pid in enumerate(chosen_products, start=1):
            qty = int(rng.integers(1, 6))
            base_price = float(base_price_map[pid])

            disc_pct = float(np.round(rng.uniform(0, 0.25), 4))
            if o.get("promo_id") is not None and rng.random() < 0.6:
                disc_pct = float(np.round(rng.uniform(0.05, 0.35), 4))

            unit_price = float(np.round(base_price * (1 - disc_pct), 2))
            line_total = float(np.round(unit_price * qty, 2))

            dirty_pid = pid
            if rng.random() < 0.01:
                dirty_pid = pid.lower()
            dirty_pid = add_noise_whitespace(dirty_pid, rng, p=0.02)

            rows.append({
                "order_id": o["order_id"],
                "line_number": line_num,
                "product_id": dirty_pid,
                "quantity": qty,
                "base_price": base_price,
                "discount_pct": disc_pct,
                "unit_price": unit_price,
                "line_total": line_total
            })

            # occasional duplicate line
            if rng.random() < 0.01:
                rows.append(rows[-1].copy())

    return rows


def gen_returns_for_day(day, orders, order_lines, rng, return_rate=0.03):
    delivered_orders = [o for o in orders if o.get("order_status") in ("DELIVERED", "SHIPPED")]
    if not delivered_orders:
        return pd.DataFrame([])

    n_returns = max(0, int(math.ceil(len(delivered_orders) * return_rate)))
    line_df = pd.DataFrame(order_lines)

    reasons = ["Damaged", "Wrong Item", "No Longer Needed", "Late Delivery", "Other"]
    rows = []

    for _ in range(n_returns):
        o = delivered_orders[int(rng.integers(0, len(delivered_orders)))]
        oid = o["order_id"]
        sub = line_df[line_df["order_id"] == oid]
        if sub.empty:
            continue
        line = sub.sample(1, random_state=int(rng.integers(1, 2**31-1))).iloc[0]

        returned_qty = int(rng.integers(1, int(line["quantity"]) + 1))
        refund = float(np.round(float(line["unit_price"]) * returned_qty, 2))

        if rng.random() < 0.01:
            oid = "O_INVALID_" + uuid.uuid4().hex[:6].upper()

        rows.append({
            "return_id": f"R{uuid.uuid4().hex[:10].upper()}",
            "event_date": day.isoformat(),
            "order_id": oid,
            "product_id": str(line["product_id"]),
            "returned_qty": returned_qty,
            "refund_amount": refund,
            "reason": rng.choice(reasons),
            "processed_timestamp": (datetime.combine(day, datetime.min.time()) +
                                    timedelta(seconds=int(rng.integers(0, 86400)))).isoformat(timespec="seconds")
        })

    return pd.DataFrame(rows)


def gen_inventory_snapshot(snapshot_day, stores_df, products_df, rng):
    store_ids = stores_df["store_id"].tolist()
    product_ids = products_df["product_id"].tolist()

    n_pairs = min(20000, len(store_ids) * len(product_ids))
    pairs = set()
    while len(pairs) < n_pairs:
        pairs.add((store_ids[int(rng.integers(0, len(store_ids)))],
                   product_ids[int(rng.integers(0, len(product_ids)))]))

    rows = []
    for s, p in pairs:
        on_hand = int(max(0, rng.normal(40, 25)))
        reserved = int(max(0, rng.normal(5, 4)))
        rows.append({
            "snapshot_date": snapshot_day.isoformat(),
            "store_id": s,
            "product_id": p,
            "on_hand_qty": on_hand,
            "reserved_qty": reserved,
            "available_qty": max(0, on_hand - reserved),
            "reorder_point": int(rng.integers(10, 30))
        })
    return pd.DataFrame(rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Parse dates
start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").date()
end_dt   = datetime.strptime(END_DATE, "%Y-%m-%d").date()

# Create folders
ensure_dir(MASTER_DIR)

orders_base    = f"{BRONZE_ROOT}/sales/orders"
lines_base     = f"{BRONZE_ROOT}/sales/order_lines"
returns_base   = f"{BRONZE_ROOT}/returns"
inventory_base = f"{BRONZE_ROOT}/inventory"

ensure_dir(orders_base)
ensure_dir(lines_base)
ensure_dir(returns_base)
ensure_dir(inventory_base)

# ---- Master data
stores_df    = gen_stores(N_STORES, rng)
products_df  = gen_products(N_PRODUCTS, rng)
customers_df = gen_customers(N_CUSTOMERS, rng)
promos_df    = gen_promotions(N_PROMOS, start_dt, end_dt, rng)

stores_df.to_csv(f"{MASTER_DIR}/stores.csv", index=False)
products_df.to_csv(f"{MASTER_DIR}/products.csv", index=False)
customers_df.to_csv(f"{MASTER_DIR}/customers.csv", index=False)
promos_df.to_csv(f"{MASTER_DIR}/promotions.csv", index=False)

print("✅ Master data written to:", MASTER_DIR)

# ---- Daily facts
for d in daterange(start_dt, end_dt):
    orders = gen_orders_for_day(
        d, stores_df, customers_df, promos_df, rng,
        avg_orders_per_day=AVG_ORDERS_PER_DAY,
        late_arrival_rate=LATE_ARRIVAL_RATE
    )
    order_lines = gen_order_lines_for_orders(
        orders, products_df, rng,
        max_lines_per_order=MAX_LINES_PER_ORDER
    )

    # Orders JSONL
    orders_part_dir = f"{orders_base}/event_date={d.isoformat()}"
    ensure_dir(orders_part_dir)
    orders_file = f"{orders_part_dir}/orders_{d.strftime('%Y%m%d')}.jsonl"
    to_jsonl(orders_file, orders)

    # Order lines CSV
    lines_part_dir = f"{lines_base}/event_date={d.isoformat()}"
    ensure_dir(lines_part_dir)
    lines_file = f"{lines_part_dir}/order_lines_{d.strftime('%Y%m%d')}.csv"
    pd.DataFrame(order_lines).to_csv(lines_file, index=False)

    # Returns CSV
    returns_df = gen_returns_for_day(d, orders, order_lines, rng, return_rate=RETURN_RATE)
    if not returns_df.empty:
        returns_part_dir = f"{returns_base}/event_date={d.isoformat()}"
        ensure_dir(returns_part_dir)
        returns_file = f"{returns_part_dir}/returns_{d.strftime('%Y%m%d')}.csv"
        returns_df.to_csv(returns_file, index=False)

    # Inventory snapshot every N days
    if INVENTORY_EVERY_N_DAYS > 0:
        day_idx = (d - start_dt).days
        if day_idx % INVENTORY_EVERY_N_DAYS == 0:
            inv_df = gen_inventory_snapshot(d, stores_df, products_df, rng)
            inv_part_dir = f"{inventory_base}/snapshot_date={d.isoformat()}"
            ensure_dir(inv_part_dir)
            inv_file = f"{inv_part_dir}/inventory_{d.strftime('%Y%m%d')}.csv"
            inv_df.to_csv(inv_file, index=False)

print("✅ Done generating Bronze data!")
print("👉 Go to Lakehouse -> Files -> retail_project/bronze to view the output.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# List a few generated files
import glob

some_orders = glob.glob(f"{orders_base}/event_date=*/orders_*.jsonl")[:3]
some_lines  = glob.glob(f"{lines_base}/event_date=*/order_lines_*.csv")[:3]

print("Sample orders files:", some_orders)
print("Sample line files:", some_lines)

# Read and preview one file
if some_lines:
    df_lines = pd.read_csv(some_lines[0])
    display(df_lines.head())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
