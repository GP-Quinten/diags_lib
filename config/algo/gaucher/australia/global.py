# Databricks notebook source
import os

import yaml

# COMMAND ----------

# from config.algo.gaucher import base
# from config.algo import utils

# COMMAND ----------

# MAGIC %run ../base

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

COUNTRY = "australia"

# COMMAND ----------

GAUCHER_AUSTRALIA_ALGORITHM_BLOCKS = ["ALGORITHM_BLOCK_A", "ALGORITHM_BLOCK_B", "ALGORITHM_BLOCK_C", "ALGORITHM_BLOCK_D"]

GAUCHER_AUSTRALIA_GLOBAL_ALGORITHM_RULE = "(ALGORITHM_BLOCK_A) AND ((ALGORITHM_BLOCK_B) OR (ALGORITHM_BLOCK_C) OR (ALGORITHM_BLOCK_D))"

GAUCHER_AUSTRALIA_ALGORITHM_BLOCK_TO_CATEGORY = {
    "ALGORITHM_BLOCK_A": "(thrombocytopenia)",
    "ALGORITHM_BLOCK_B": "(hyperferritinaemia) AND (elevated_transferrin) AND NOT (hemochromatosis)",
    "ALGORITHM_BLOCK_C": "(anemias)",
    "ALGORITHM_BLOCK_D": "(abdominal_distension) OR (abdominal_pain) OR (avascular_necrosis) OR (hepatomegaly) OR (low_hdl) OR (monoclonal_gammopathy) OR (organomegaly) OR (bone_disorders) OR (osteoporosis) OR ((short_stature) AND (F.col(COL_AGE_AT_INDEX_DATE) < 15)) OR (splenomegaly)"
}

# COMMAND ----------

filename = "diags_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    GAUCHER_AUSTRALIA_DIAGS_FEATURES = yaml.safe_load(fp)

filename = "procedures_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    GAUCHER_AUSTRALIA_PROC_FEATURES = yaml.safe_load(fp)

filename = "labs_conf"
with open(os.path.join(ALGO_CONF_PATH, COUNTRY, "yaml_config", f"{filename}.yaml"), 'r') as fp:
    GAUCHER_AUSTRALIA_LABS_FEATURES = yaml.safe_load(fp)

# COMMAND ----------

GAUCHER_AUSTRALIA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(GAUCHER_AUSTRALIA_DIAGS_FEATURES)
GAUCHER_AUSTRALIA_PROC_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(GAUCHER_AUSTRALIA_PROC_FEATURES)
GAUCHER_AUSTRALIA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_algo_category_to_medical_term(GAUCHER_AUSTRALIA_LABS_FEATURES)

# COMMAND ----------

GAUCHER_AUSTRALIA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM = get_global_algo_category_to_medical_term(
  GAUCHER_AUSTRALIA_DIAGS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  GAUCHER_AUSTRALIA_LABS_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
  )

# COMMAND ----------

GAUCHER_AUSTRALIA_MEDICAL_TERM_TO_ICD_CODES = get_medical_term_to_code(GAUCHER_AUSTRALIA_DIAGS_FEATURES)

# COMMAND ----------

