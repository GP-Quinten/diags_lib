# Databricks notebook source
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

ASMD_RUSSIA_ALGORITHM_BLOCKS = ["ALGORITHM_BLOCK_A", "ALGORITHM_BLOCK_B", "ALGORITHM_BLOCK_C", "ALGORITHM_BLOCK_D"]

ASMD_RUSSIA_GLOBAL_ALGORITHM_RULE = "(ALGORITHM_BLOCK_A) AND ((ALGORITHM_BLOCK_B) OR (ALGORITHM_BLOCK_C) OR NOT (ALGORITHM_BLOCK_D))"

ASMD_RUSSIA_ALGORITHM_BLOCK_TO_CATEGORY = {
    "ALGORITHM_BLOCK_A": "(splenomegaly) OR (hepatomegaly) OR (organomegaly)",
    "ALGORITHM_BLOCK_B": "(family_history)",
    "ALGORITHM_BLOCK_C": "( ((low_hdl) OR (bone_damage) OR (interstitial_lung_disease)) AND (F.col(COL_AGE_AT_INDEX_DATE) >= 18) ) OR ( ((hypotension) OR (development_delay)) AND (F.col(COL_AGE_AT_INDEX_DATE) < 18) )",
    "ALGORITHM_BLOCK_D": "(asthma) OR (autoimmune_liver_damage) OR (blood_diseases_and_tumors) OR (cerebral_palsy) OR (cryptogenic_cirrhosis) OR (cystic_fibrosis) OR (heart_failure) OR (hereditary_ataxia) OR (infections) OR (infectious_liver_disease) OR (infectious_mononucleosis) OR (lysosomal_disorders) OR (pulmonary_obstructive_disease)"
}

# COMMAND ----------

filename = "diags_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    ASMD_RUSSIA_DIAGS_FEATURES = yaml.safe_load(fp)

filename = "procedures_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    ASMD_RUSSIA_PROC_FEATURES = yaml.safe_load(fp)

filename = "labs_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    ASMD_RUSSIA_LABS_FEATURES = yaml.safe_load(fp)

# COMMAND ----------

ASMD_RUSSIA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(ASMD_RUSSIA_DIAGS_FEATURES)
ASMD_RUSSIA_PROC_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(ASMD_RUSSIA_PROC_FEATURES)
ASMD_RUSSIA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(ASMD_RUSSIA_LABS_FEATURES)

# COMMAND ----------

ASMD_RUSSIA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_global_algo_category_to_medical_term(
  ASMD_RUSSIA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  ASMD_RUSSIA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  )

# COMMAND ----------

ASMD_RUSSIA_MEDICAL_TERM_TO_ICD_CODES = get_medical_term_to_code(ASMD_RUSSIA_DIAGS_FEATURES)