# Databricks notebook source
dbutils.widgets.text("n_records", "10")

# COMMAND ----------

n_records = dbutils.widgets.get("n_records")

# COMMAND ----------

spark.range(n_records).write.mode("overwrite").saveAsTable("default.n_records_table")

# COMMAND ----------


