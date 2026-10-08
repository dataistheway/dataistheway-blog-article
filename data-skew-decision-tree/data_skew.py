# Databricks notebook source
# MAGIC %md
# MAGIC # Data skew in Spark: a decision tree
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/data-skew-decision-tree/.
# MAGIC
# MAGIC **Data:** synthetic, generated in the notebook: 2 million fact rows, ~90% with key `CUST000001`, ~2% with `NULL`;
# MAGIC a dimension of 10,000 customers. No personal data.
# MAGIC
# MAGIC **Names:** tables are `<catalog>.<schema>.skew_facts` / `skew_dim` (defaults: `workspace.dataistheway`).
# MAGIC
# MAGIC After each cell with a join, open the query profile (serverless) or Spark UI → Stages (classic cluster).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

T = f"{catalog}.{schema}"
FACTS = f"{T}.skew_facts"
DIM = f"{T}.skew_dim"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: data with a skewed key

# COMMAND ----------

from pyspark.sql import functions as F

facts_df = (
    spark.range(2_000_000)
    .withColumn("r", F.rand(42))
    .select(
        F.concat(F.lit("ORD"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("order_id"),
        F.when(F.col("r") < 0.90, F.lit("CUST000001"))
         .when(F.col("r") < 0.92, F.lit(None).cast("string"))
         .otherwise(F.concat(F.lit("CUST"),
                             F.lpad(((F.col("id") % 9999) + 2).cast("string"), 6, "0")))
         .alias("customer_id"),
        F.concat(F.lit("PROD"), F.lpad((F.col("id") % 500).cast("string"), 4, "0")).alias("product_id"),
        F.timestamp_seconds(F.lit(1704067200) + (F.col("id") % 2592000)).alias("order_ts"),  # January 2024
        F.round(F.rand(7) * 490 + 10, 2).alias("amount"),
    )
)
facts_df.write.mode("overwrite").saveAsTable(FACTS)

dim_df = spark.range(1, 10_001).select(
    F.concat(F.lit("CUST"), F.lpad(F.col("id").cast("string"), 6, "0")).alias("customer_id"),
    F.element_at(F.array(F.lit("Basic"), F.lit("Silver"), F.lit("Gold")),
                 (F.col("id") % 3 + 1).cast("int")).alias("segment"),
)
dim_df.write.mode("overwrite").saveAsTable(DIM)

facts = spark.table(FACTS)
dim = spark.table(DIM)
print(facts.count(), "facts,", dim.count(), "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0a: is it really skew? Key distribution

# COMMAND ----------

display(spark.sql(f"""
SELECT customer_id,
       count(*)                                         AS cnt,
       round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM {FACTS}
GROUP BY customer_id
ORDER BY cnt DESC
LIMIT 20
"""))

# COMMAND ----------

# AQE thresholds (usually not readable on serverless)
for k in ["spark.sql.adaptive.enabled",
          "spark.sql.adaptive.skewJoin.enabled",
          "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
          "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]:
    try:
        print(f"{k:62s} = {spark.conf.get(k)}")
    except Exception as e:
        print(f"{k:62s} = not available ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: broadcast (the small side of the join)

# COMMAND ----------

bc = spark.sql(f"""
SELECT /*+ BROADCAST(d) */ f.*, d.segment
FROM {FACTS} f
JOIN {DIM} d ON f.customer_id = d.customer_id
""")
bc.explain()
print("rows:", bc.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: NULL as a hot key
# MAGIC Inner join: look for `isnotnull(customer_id)` in the plan. Left join: NULLs stay and go through the shuffle.

# COMMAND ----------

facts.join(dim.hint("merge"), "customer_id", "inner").explain()

# COMMAND ----------

nulls    = facts.filter(F.col("customer_id").isNull())
nonnulls = facts.filter(F.col("customer_id").isNotNull())

dim_cols = [c for c in dim.columns if c != "customer_id"]
left_joined = (
    nonnulls.join(dim, "customer_id", "left")
    .unionByName(nulls.select("*", *[F.lit(None).cast(dim.schema[c].dataType).alias(c)
                                     for c in dim_cols]))
)

plain_left = facts.join(dim, "customer_id", "left")
print("facts:", facts.count(), "| left_joined:", left_joined.count(), "| plain left join:", plain_left.count())
assert left_joined.count() == facts.count() == plain_left.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps 4-5: plain shuffle join vs salting the hot keys
# MAGIC The `merge` hint forces a sort-merge join so the small dimension is not broadcast.
# MAGIC Note: with 2 million rows AQE most likely will NOT treat a partition as skewed (256 MB threshold). That is a lesson too.

# COMMAND ----------

import time

t0 = time.time()
plain_cnt = facts.join(dim.hint("merge"), "customer_id").count()
t_plain = time.time() - t0

N = 16
hot_keys = [r["customer_id"] for r in
            facts.groupBy("customer_id").count()
                 .filter("count > 100000").select("customer_id").collect()]
print("hot keys:", hot_keys)

facts_s = facts.withColumn(
    "salt",
    F.when(F.col("customer_id").isin(hot_keys), (F.rand() * N).cast("int"))
     .otherwise(F.lit(0)))

dim_s = (dim.withColumn(
            "salts",
            F.when(F.col("customer_id").isin(hot_keys), F.sequence(F.lit(0), F.lit(N - 1)))
             .otherwise(F.array(F.lit(0))))
            .withColumn("salt", F.explode("salts"))
            .drop("salts"))

t0 = time.time()
salted = facts_s.join(dim_s.hint("merge"), ["customer_id", "salt"]).drop("salt")
salted_cnt = salted.count()
t_salted = time.time() - t0

print(f"plain join  : {plain_cnt} rows, {t_plain:.1f}s")
print(f"salting     : {salted_cnt} rows, {t_salted:.1f}s")
assert plain_cnt == salted_cnt, "salted join changed the row count"

# COMMAND ----------

# Row distribution after salting (an approximation of what each task gets)
display(facts_s.groupBy("customer_id", "salt").count().orderBy(F.desc("count")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: skewed groupBy and window functions

# COMMAND ----------

# count(DISTINCT product_id) per customer: what Spark plans on its own
direct = facts.groupBy("customer_id").agg(F.countDistinct("product_id").alias("count"))

# the same written by hand in two steps; count("product_id") skips NULL like count(DISTINCT) does
two_step = (facts.select("customer_id", "product_id").distinct()
                 .groupBy("customer_id").agg(F.count("product_id").alias("count")))

print("rows that differ:", two_step.exceptAll(direct).count() + direct.exceptAll(two_step).count())

# compare the physical plans: look for an aggregate keyed by (customer_id, product_id) in both
direct.explain()
two_step.explain()

# COMMAND ----------

# NULL check: products [7, 7, NULL] -> count(DISTINCT) = 1; .count() after distinct() would give 2
t = spark.createDataFrame([("C1", 7), ("C1", 7), ("C1", None), ("C2", None)], "customer_id string, product_id int")
display(t.groupBy("customer_id").agg(F.countDistinct("product_id").alias("count_distinct"))
         .join(t.distinct().groupBy("customer_id").agg(F.count("product_id").alias("two_step_agg"),
                                                       F.count("*").alias("two_step_rows")), "customer_id")
         .orderBy("customer_id"))

# COMMAND ----------

# salted two-stage aggregation: partial result per (key, salt), then merge per key
# (doubles summed in a different order can differ in the last bits, so compare rounded)
N = 16
# sum only shows the pattern: partial aggregation already handles sum on its own
salted_sum = (facts.withColumn("salt", (F.rand(42) * N).cast("int"))
                   .groupBy("customer_id", "salt").agg(F.sum("amount").alias("part"))
                   .groupBy("customer_id").agg(F.round(F.sum("part"), 2).alias("amount")))
plain_sum = facts.groupBy("customer_id").agg(F.round(F.sum("amount"), 2).alias("amount"))
print("rows that differ:", salted_sum.exceptAll(plain_sum).count())

# COMMAND ----------

# latest order per customer without a window function
latest = (facts.groupBy("customer_id")
               .agg(F.max_by(F.struct(*facts.columns), F.col("order_ts")).alias("r"))
               .select("r.*"))
display(latest.orderBy("customer_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for t in [FACTS, DIM]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("Dropped:", t)
