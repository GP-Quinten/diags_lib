# Databricks notebook source
# from config.variables import storage
# from config.data_provider.optum_ehr import global

# COMMAND ----------

# MAGIC %run ../variables/storage

# COMMAND ----------

# MAGIC %run ./optum_ehr/global

# COMMAND ----------

## data provider related variables ##
# name of the data provider
DATA_PROVIDER = "optum_ehr"
# location of the folder related to the data provider
DATA_PROVIDER_CONF_PATH = os.path.join(LOCAL_PROJECT_PATH, "config", "data_provider", DATA_PROVIDER, "yaml_config")
# name of the laboratory measure to main unit mapping file 
LABS_TO_MAIN_UNIT_MAPPING_FILE = "labs_to_main_unit.yaml"
# name of the observation measure to main unit mapping file 
OBSERVATIONS_TO_MAIN_UNIT_MAPPING_FILE = "observations_to_main_unit.yaml"
# name of the measurement to main unit mapping file 
MEASUREMENTS_TO_MAIN_UNIT_MAPPING_FILE = "measurements_to_main_unit.yaml"