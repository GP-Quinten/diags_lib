# Databricks notebook source
# from config.variables import storage

# COMMAND ----------

# MAGIC %run ../../variables/storage

# COMMAND ----------

DISEASE = "pompe"

# COMMAND ----------

POMPE_ICD10_CODES = ["E7402"]
POMPE_NDC_CODES = ["584680150", "584680160", "584680426"]
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
POMPE_NDC_CODES = [code[:9] for code in POMPE_NDC_CODES]
POMPE_CODES = POMPE_ICD10_CODES + POMPE_NDC_CODES

# COMMAND ----------

POMPE_PREVALENCE = 1/40000

# COMMAND ----------

ALGO_CONF_PATH = os.path.join(LOCAL_PROJECT_PATH, "config", "algo", DISEASE)