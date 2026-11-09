# Databricks notebook source
# Job task for 01_which_trigger: logs the trigger type and the folder the job passed in.
dbutils.widgets.text("log_table", "")
dbutils.widgets.text("trigger_type", "")
dbutils.widgets.text("location", "")
spark.sql(f"INSERT INTO {dbutils.widgets.get('log_table')} SELECT current_timestamp(), :t, :l",
          args={"t": dbutils.widgets.get("trigger_type"), "l": dbutils.widgets.get("location")})
