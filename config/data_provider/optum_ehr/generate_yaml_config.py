# Databricks notebook source
# this script must only be run to generate the yaml configuration
# if the yaml configuration files already exist no need to run it

# COMMAND ----------

import os

import yaml

# COMMAND ----------

# from utils.io_functions import loading
# from config.data_provider import base
# from config.data_provider.optum_ehr import global

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../base

# COMMAND ----------

# MAGIC %run ./global

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_main_unit_config(db_config: dict, db_path: str) -> dict:
    """Gathers the main unit of numerical values from the configuration file

    Args:
        db_config (dict): Configuration file
        db_path (str): Path to configuration file

    Returns:
        dict: Dictionnary with names of numerical values with their matching main unit
    """
    table = read_spark_file(db_path, db_config["TABLE_NAME"])
    table = table.filter(~(F.col(db_config["COL_INFO"]).isNull()))
    table = table.filter(~(F.col(db_config["COL_UNIT"]).isNull()))
    grouped = table.groupBy(db_config["COL_INFO"], db_config["COL_UNIT"]).count()
    window = Window.partitionBy(db_config["COL_INFO"]).orderBy(F.desc("count"))
    df_main_unit = (
        grouped.withColumn("order", F.row_number().over(window))
        .where(F.col("order") == 1)
        .toPandas()
    )
    return dict(
        zip(df_main_unit[db_config["COL_INFO"]], df_main_unit[db_config["COL_UNIT"]])
    )

# COMMAND ----------

# create the labs to main unit mapping
feature_name = "labs"
with open(os.path.join(DATA_PROVIDER_CONF_PATH, LABS_TO_MAIN_UNIT_MAPPING_FILE), 'w') as fp:
  main_unit_config = get_main_unit_config(DATABASES_FEATURE_CONFIG[feature_name], CONFIG_PATH_DATABASE)
  yaml.safe_dump(main_unit_config, fp)

# COMMAND ----------

# create the observations to main unit mapping
feature_name = "observations"
with open(os.path.join(DATA_PROVIDER_CONF_PATH, OBSERVATIONS_TO_MAIN_UNIT_MAPPING_FILE), 'w') as fp:
  main_unit_config = get_main_unit_config(DATABASES_FEATURE_CONFIG[feature_name], CONFIG_PATH_DATABASE)
  yaml.safe_dump(main_unit_config, fp)

# COMMAND ----------

# create the measurements to main unit mapping
feature_name = "measurements"
with open(os.path.join(DATA_PROVIDER_CONF_PATH, MEASUREMENTS_TO_MAIN_UNIT_MAPPING_FILE), 'w') as fp:
  main_unit_config = get_main_unit_config(DATABASES_FEATURE_CONFIG[feature_name], CONFIG_PATH_DATABASE)
  yaml.safe_dump(main_unit_config, fp)