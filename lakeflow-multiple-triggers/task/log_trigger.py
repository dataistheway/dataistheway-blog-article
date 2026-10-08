# Databricks notebook source
# Job task for the lakeflow_multiple_triggers notebook: logs which trigger type the job passed in.
dbutils.widgets.text("log_table", "")
dbutils.widgets.text("trigger_type", "")
spark.sql(f"INSERT INTO {dbutils.widgets.get('log_table')} SELECT current_timestamp(), :t",
          args={"t": dbutils.widgets.get("trigger_type")})
