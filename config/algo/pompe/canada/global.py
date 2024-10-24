# Databricks notebook source
import os

import yaml

# COMMAND ----------

# from config.algo.pompe import base
# from config.algo import utils

# COMMAND ----------

# MAGIC %run ../base

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

COUNTRY = "canada"

# COMMAND ----------

POMPE_CANADA_ALGORITHM_BLOCKS = ["ALGORITHM_BLOCK_A", "ALGORITHM_BLOCK_B", "ALGORITHM_BLOCK_C", "ALGORITHM_BLOCK_D"]

POMPE_CANADA_GLOBAL_ALGORITHM_RULE = "(ALGORITHM_BLOCK_A) AND (ALGORITHM_BLOCK_B) AND ((ALGORITHM_BLOCK_C) OR (ALGORITHM_BLOCK_D))"

POMPE_CANADA_ALGORITHM_BLOCK_TO_CATEGORY = {
    "ALGORITHM_BLOCK_A": "(muscular_lab)",
    "ALGORITHM_BLOCK_B": "(hepatic_lab) OR (liver)",
    "ALGORITHM_BLOCK_C": "(myopathy) OR (muscle_weakness) OR (gait_abnormalities)",
    "ALGORITHM_BLOCK_D": "(abnormal_pft) OR (dyspnea) OR (sleep_apnea)"
}

# COMMAND ----------

filename = "diags_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    POMPE_CANADA_DIAGS_FEATURES = yaml.safe_load(fp)

filename = "procedures_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    POMPE_CANADA_PROC_FEATURES = yaml.safe_load(fp)

filename = "labs_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    POMPE_CANADA_LABS_FEATURES = yaml.safe_load(fp)

# COMMAND ----------

POMPE_CANADA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(POMPE_CANADA_DIAGS_FEATURES)
POMPE_CANADA_PROC_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(POMPE_CANADA_PROC_FEATURES)
POMPE_CANADA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(POMPE_CANADA_LABS_FEATURES)

# COMMAND ----------

POMPE_CANADA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_global_algo_category_to_medical_term(
  POMPE_CANADA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  POMPE_CANADA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  )

# COMMAND ----------

POMPE_CANADA_MEDICAL_TERM_TO_ICD_CODES = get_medical_term_to_code(POMPE_CANADA_DIAGS_FEATURES)

# COMMAND ----------

