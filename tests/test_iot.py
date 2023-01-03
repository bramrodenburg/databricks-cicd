# Integration tests for the userdata pipeline
import pytest

from pyspark.sql import SparkSession


@pytest.mark.databricks
def test_userdata_pipeline(spark: SparkSession, dbutils) -> None:
  """ Verify whether the IoT User Data pipeline runs end to end. """
  spark.sql("CREATE SCHEMA IF NOT EXISTS it_bronze_iot")
  
  # Run the userdata_silver pipeline with test parameters.
  dbutils.notebook.run("pipelines/userdata_silver", 
                       timeout_seconds=120,
                       arguments={
    "bronze_dir": "/databricks-datasets/iot-stream/data-user/userData.csv",
    "silver_schema": "it_bronze_iot"
  })
  
  # After running the userdata_silver, we expect that a table called
  # it_bronze_iot.userdata_t is present, containing the processed data.
  df = spark.table("it_bronze_iot.userdata_t")
  
  assert df.count() == 38, "Ingestion of test data should give exactly 38 records."
  
  n_unique_user_ids = df.select("userid").distinct().count()
  assert n_unique_user_ids == 38, "Number of unique user ids should be exactly 38."
