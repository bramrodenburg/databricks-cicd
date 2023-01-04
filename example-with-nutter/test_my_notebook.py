# Databricks notebook source
# MAGIC %pip install nutter

# COMMAND ----------

from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

   def run_test_name(self):
      dbutils.notebook.run('./my_notebook', 600, {"n_records": 100})

   def assertion_test_name(self):
      some_tbl = sqlContext.sql('SELECT COUNT(*) AS total FROM default.n_records_table')
      first_row = some_tbl.first()
      assert (first_row[0] == 100)

result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
#result.exit(dbutils)

# COMMAND ----------


