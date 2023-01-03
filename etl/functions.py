# Library of common ETL functions
from pyspark.sql import DataFrame

import pyspark.sql.functions as f


def add_metadata(df: DataFrame) -> DataFrame:
  """ Add metadata columns to the provided dataframe. 
  
  Arguments
  ---------
  df : The input Spark dataframe.

  Returns
  -------
    The original dataframe with two additional metadata columns:
      - m_timestamp_processed : The timestamp when Spark processed the data
      - m_file_processed : The input file (if available) that Spark processed
  """
  return (df
          .withColumn("m_timestamp_processed", f.current_timestamp())
          .withColumn("m_file_processed", f.input_file_name())
        )

