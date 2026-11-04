# Databricks notebook source
# MAGIC %md
# MAGIC # Generate RetailHub demo data
# MAGIC Synthetic data for the multi-agent demo: customers, products, stores and order events, written as files to the `raw` volume.
# MAGIC The pipeline in `src/pipeline/` ingests them with Auto Loader.

# COMMAND ----------

dbutils.widgets.text("catalog", "dataistheway")
dbutils.widgets.text("schema", "omni_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
RAW = f"/Volumes/{catalog}/{schema}/raw"

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

rng = random.Random(42)
first = ["Anna", "Piotr", "Ola", "Marek", "Ewa", "Tomasz", "Kasia", "Jan", "Zofia", "Adam"]
last = ["Nowak", "Kowalski", "Wisniewski", "Wojcik", "Kaminski", "Lewandowski", "Zielinski"]
cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz"]

customers = []
for i in range(1, 301):
    f, l = rng.choice(first), rng.choice(last)
    customers.append({
        "customer_id": i,
        "full_name": f"{f} {l}",
        "email": f"{f.lower()}.{l.lower()}{i}@example.com",
        "city": rng.choice(cities),
        "signup_date": str(datetime(2025, 1, 1).date() + timedelta(days=rng.randint(0, 600))),
    })

categories = {
    "Beverages": ["Espresso beans", "Green tea", "Sparkling water", "Orange juice"],
    "Bakery": ["Sourdough bread", "Croissant", "Rye bread"],
    "Dairy": ["Greek yogurt", "Cheddar", "Butter", "Oat milk"],
    "Household": ["Dish soap", "Paper towels", "Laundry pods"],
    "Snacks": ["Dark chocolate", "Salted almonds", "Rice crackers"],
    "Produce": ["Bananas", "Apples", "Tomatoes", "Avocados"],
}
products, pid = [], 1
for cat, names in categories.items():
    for n in names:
        products.append({"product_id": pid, "product_name": n, "category": cat,
                         "unit_price": round(rng.uniform(2, 40), 2)})
        pid += 1

stores = [{"store_id": s, "store_name": f"RetailHub {c}", "city": c} for s, c in enumerate(cities, start=1)]

# Order events. Returns arrive as separate events with status RETURNED and a POSITIVE quantity,
# which a naive SUM(quantity * unit_price) counts as revenue.
orders = []
start = datetime(2026, 9, 1)
for oid in range(1, 6001):
    p = rng.choice(products)
    ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 45))
    orders.append({
        "order_id": oid, "event_type": "SALE", "status": "COMPLETED",
        "customer_id": rng.randint(1, 300), "product_id": p["product_id"],
        "store_id": rng.choice(stores)["store_id"] if rng.random() > 0.01 else None,
        "quantity": rng.randint(1, 5), "unit_price": p["unit_price"],
        "event_ts": ts.isoformat(),
    })
for o in rng.sample(orders, 420):
    orders.append({**o, "event_type": "RETURN", "status": "RETURNED",
                   "event_ts": (datetime.fromisoformat(o["event_ts"]) + timedelta(days=rng.randint(1, 10))).isoformat()})

# COMMAND ----------

def write(name, rows, chunks=1):
    dbutils.fs.rm(f"{RAW}/{name}", True)
    size = len(rows) // chunks + 1
    for c in range(chunks):
        part = rows[c * size:(c + 1) * size]
        dbutils.fs.put(f"{RAW}/{name}/{name}_{c:03d}.json", "\n".join(json.dumps(r) for r in part), True)

write("customers", customers)
write("products", products)
write("stores", stores)
write("orders", sorted(orders, key=lambda r: r["event_ts"]), chunks=6)
print(f"customers={len(customers)} products={len(products)} stores={len(stores)} order_events={len(orders)}")
