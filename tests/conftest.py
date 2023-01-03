
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request) -> SparkSession:
  """ A Test fixture that returns a SparkSession.
  
  Uses an existing SparkSession when marked with "databricks".
  Otherwise, creates a local SparkSession.
  
  Returns
  -------
    An instance of a SparkSession.
  """
  marker = request.node.get_closest_marker("databricks")
  
  if marker:
    return SparkSession.builder.getOrCreate()
  
  return SparkSession.builder.master("local[*]").getOrCreate()


@pytest.fixture(scope="session")
def dbutils(spark: SparkSession) -> any:
  """ Returns an instance of DBUtils.
  
  TODO: This won't work for local sessions.
  
  Returns
  -------
    An instance of DBUtils
  """
  from pyspark.dbutils import DBUtils
  return DBUtils(spark)
