# Databricks notebook source
# Job task for 03_freshness_check: a run started by the schedule fails when the newest row is older than the SLA.
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("trigger_type", "")
dbutils.widgets.text("sla_hours", "24")
sla = float(dbutils.widgets.get("sla_hours"))
if dbutils.widgets.get("trigger_type") == "periodic":
    age = spark.sql(f"SELECT (unix_timestamp() - unix_timestamp(max(ingested_at))) / 3600 FROM {dbutils.widgets.get('source_table')}").first()[0]
    if age > sla:
        raise Exception(f"Source is stale: newest row is {age:.0f} h old, SLA is {sla:.0f} h")
print("trigger:", dbutils.widgets.get("trigger_type"))
