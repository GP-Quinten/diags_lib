# Databricks notebook source
# from config.variables import storage

# COMMAND ----------

# MAGIC %run ../../variables/storage

# COMMAND ----------

DISEASE = "ASMD"

# COMMAND ----------

ASMD_ICD10_CODES = ["E75240", "E75241", "E75244", "E75249"] # types A, B, AB and unspecified
ASMD_NDC_CODES = ["584680050", "584680051"]
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
ASMD_NDC_CODES = [code[:9] for code in ASMD_NDC_CODES]
ASMD_CODES = ASMD_ICD10_CODES + ASMD_NDC_CODES

# COMMAND ----------

ASMD_PREVALENCE = 1/250000

# COMMAND ----------

ALGO_CONF_PATH = os.path.join(LOCAL_PROJECT_PATH, "config", "algo", DISEASE)