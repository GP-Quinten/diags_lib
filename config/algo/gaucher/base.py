# Databricks notebook source
# from config.variables import storage

# COMMAND ----------

# MAGIC %run ../../variables/storage

# COMMAND ----------

DISEASE = "gaucher"

# COMMAND ----------

GAUCHER_ICD10_CODES = ["E7522"]
GAUCHER_NDC_CODES = ["540920701", "548681983", "584680220", "584681983", "584684663", "69010601"]
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
GAUCHER_NDC_CODES = [code[:9] for code in GAUCHER_NDC_CODES]
GAUCHER_CODES = GAUCHER_ICD10_CODES + GAUCHER_NDC_CODES

# COMMAND ----------

GAUCHER_PREVALENCE = 1/100000

# COMMAND ----------

ALGO_CONF_PATH = os.path.join(LOCAL_PROJECT_PATH, "config", "algo", DISEASE)