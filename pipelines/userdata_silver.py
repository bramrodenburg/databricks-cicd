# Databricks notebook source
from etl import add_metadata

# COMMAND ----------

dbutils.widgets.text("bronze_dir", "/databricks-datasets/iot-stream/data-user/userData.csv", "Source Directory")
dbutils.widgets.text("silver_schema", "iot_bronze", "Destination Schema/Database")

# COMMAND ----------

source_dir = dbutils.widgets.get("bronze_dir")
destination_schema = dbutils.widgets.get("silver_schema")
destination_table = f"{destination_schema}.userdata_t"

# COMMAND ----------

schema = "userid INT, gender STRING, age INT, height INT, weight INT, smoker STRING, familyhistory STRING, cholestlevs STRING, bp STRING, risk INT"

input_df = spark.read.format("csv").schema(schema).option("header", "true").load(source_dir)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_schema}")

# COMMAND ----------

output_df = add_metadata(input_df)

# COMMAND ----------

output_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(destination_table)

# COMMAND ----------


