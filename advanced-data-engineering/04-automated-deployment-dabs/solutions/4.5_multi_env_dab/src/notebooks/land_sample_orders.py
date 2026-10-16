# Databricks notebook source
# MAGIC %md
# MAGIC # Land sample orders
# MAGIC Creates the schema and landing volume if needed, then writes one JSON file of fake orders.

# COMMAND ----------

import json, random, uuid
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "orders_dev")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("rows", "100")
catalog, schema, volume = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))
rows = int(dbutils.widgets.get("rows"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------

now = datetime.utcnow()
orders = [{"order_id": str(uuid.uuid4()), "customer_id": random.randint(1, 500),
           "amount": round(random.uniform(-5, 400), 2),               # a few negative amounts on purpose
           "order_ts": (now - timedelta(minutes=random.randint(0, 1440))).isoformat()}
          for _ in range(rows)]
path = f"/Volumes/{catalog}/{schema}/{volume}/orders/orders-{now:%Y%m%d%H%M%S}.json"
dbutils.fs.put(path, "\n".join(json.dumps(o) for o in orders), overwrite=True)
print(f"landed {rows} orders -> {path}")
