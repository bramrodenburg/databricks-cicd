from pyspark.sql import SparkSession

from etl import add_metadata


def test_add_metadata(spark: SparkSession) -> None:
  """ Test that add_metadata adds the correct metadata columns. """
  input_df = spark.createDataFrame([
    ["Chuck", "Norris"],
    ["Jean-Claude", "Van Damme"],
    ["Steven", "Seagal"],
    ["Bruce", "Lee"]
  ], ["First name", "Last Name"])
  
  output_df = add_metadata(input_df)
  
  assert "m_timestamp_processed" in output_df.columns, \
    "Column 'm_timestamp_processed' should be present in the output dataframe."
  assert "m_file_processed" in output_df.columns, \
    "Column 'm_file_processed' should be present in the output dataframe."
 

def test_plus_one() -> None:
    assert 1+1 == 2


def test_plus_two() -> None:
    assert 2+2 == 3
