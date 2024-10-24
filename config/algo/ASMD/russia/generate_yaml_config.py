# Databricks notebook source
# this script must only be run to generate the yaml configuration
# if the yaml configuration files already exist no need to run it

# COMMAND ----------

import os

import yaml

# COMMAND ----------

# from config.algo.ASMD import base
# from config.algo import utils

# COMMAND ----------

# MAGIC %run ../base

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

COUNTRY = "russia"

# COMMAND ----------

FEATURE_CATEGORIES = ["diags", "labs", "procedures"] # must be a subset of the DATABASES_FEATURE_CONFIG keys defined in config/data_provider

for feature_name in FEATURE_CATEGORIES:
  input_filename = f"{feature_name}_conf.csv"
  output_filename = f"{feature_name}_conf.yaml"
  with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", output_filename), 'w') as fp:
    if feature_name in ["diags", "procedures", "treatments"]:
      algo_config = get_code_values_config(os.path.join(CONFIG_DATA_PATH, DISEASE, COUNTRY), input_filename, separator=";")
    elif feature_name in ["labs", "observations", "measurements"]:
      algo_config = get_numerical_values_config(os.path.join(CONFIG_DATA_PATH, DISEASE, COUNTRY), input_filename, separator=";")
    yaml.safe_dump(algo_config, fp)