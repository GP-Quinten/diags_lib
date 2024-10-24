# Databricks notebook source
# from config.variables import storage

# COMMAND ----------

# MAGIC %run ../variables/storage

# COMMAND ----------

## ICD10 nomenclature related variables ##
# location of the folder related to the ICD10 nomenclature
ICD_CONF_PATH = os.path.join(LOCAL_PROJECT_PATH, "config", "ICD10", "yaml_config")
# name of the ICD9 to ICD10 mapping file
ICD9_TO_ICD10_MAPPING_FILE = "ICD9_to_ICD10_no_approximation_2018.yaml"
# name of the ICD10 to description mapping file
ICD10_TO_DESCRIPTION_MAPPING_FILE = "ICD10_to_description_2024.yaml"