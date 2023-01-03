# Databricks notebook source
# MAGIC %md ## Run Tests on Databricks
# MAGIC 
# MAGIC This notebook allows users to execute their unit tests and integration tests on Databricks. Users can run this notebook in three modes:
# MAGIC - `Unit Tests` : Only run the unit tests in this project.
# MAGIC - `Integration Tests` : Only run the integration tests in this project. That is, all projects marked with **@databricks**
# MAGIC - `All Tests`: Run both the unit tests and integration tests in this project.

# COMMAND ----------

# MAGIC %md ### Install packages
# MAGIC This notebook assumes that we use `pytest` for running our unit and integration tests. Hence, we need to install it first.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %md
# MAGIC ### Triggering the tests

# COMMAND ----------

# MAGIC %md `autoreload` reloads Python modules automatically before execution all the code. This is helpful for when we are developing code and do not want to detach/attach the notebook each time between tests.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import pytest
import os
import sys

# COMMAND ----------

dbutils.widgets.dropdown(name="type_of_test", choices=["Unit Tests", "Integration Tests", "All Tests"], defaultValue="All Tests", label="Type of test")

# COMMAND ----------

type_of_test = dbutils.widgets.get("type_of_test")

# COMMAND ----------

repo_name = "databricks-cicd"  # Replace this with the name of your repository

# COMMAND ----------

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))
print(repo_root)

# COMMAND ----------

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace/{repo_root}/{repo_name}")
print(os.getcwd())

# COMMAND ----------

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# COMMAND ----------

# MAGIC %md Below we detect which type of tests the user wants to run, and configure the test markers that we want to use accordingly.

# COMMAND ----------

if type_of_test == "All Tests":
  markers = ""
elif type_of_test == "Integration Tests":
  markers = "databricks"
elif type_of_test == "Unit Tests":
  markers = "not databricks"
else:
  raise Exception(f"Encountered an unknown type of test: {type_of_test}")

# COMMAND ----------

# MAGIC %md Below we trigger pytest from Python, and ask it to run all the tests based on the markers that we provide.

# COMMAND ----------

# Run pytest.
retcode = pytest.main(["-v", "-p", "no:cacheprovider", "-m", markers, "tests/"])

# COMMAND ----------

# Fail the cell execution if there are any test failures.
assert retcode == 0, f"The pytest invocation failed. Reason: {retcode.name}. See the log for details."
