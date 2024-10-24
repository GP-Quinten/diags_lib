# Databricks notebook source
"""script to define storage variables"""

# COMMAND ----------

import json
import os

# COMMAND ----------

# databricks specific - to update accordingly
USERNAME = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['tags']['user']
# local path associated to the project code
LOCAL_PROJECT_PATH = f"/Workspace/Repos/{USERNAME}/RareDisease-GitHub-Helios_diagnostic_library"
# local path associated to the data
LOCAL_DATA_PATH = "/mnt/helios/"

# COMMAND ----------

CONFIG_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "config_data")
RAW_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "raw_data")

INTERMEDIATE_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "intermediate_data")

COHORT_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "cohort_data")
FEATURE_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "feature_data")
ALGO_OUTPUT_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "algo_output_data")

STATISTICS_DATA_PATH = os.path.join(LOCAL_DATA_PATH, "statistics")