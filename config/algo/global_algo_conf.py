# Databricks notebook source
# from config.algo.ASMD.russia import global
# from config.algo.gaucher.australia import global
# from config.algo.pompe.canada import global

# COMMAND ----------

# MAGIC %run ./ASMD/russia/global

# COMMAND ----------

# MAGIC %run ./gaucher/australia/global

# COMMAND ----------

# MAGIC %run ./pompe/canada/global

# COMMAND ----------

# include the lower and upper limit - age in year
CONFIG_AGE = {
    "ASMD_russia": {"type": "int", "min_value": None, "max_value": None},
    "gaucher_australia": {"type": "int", "min_value": None, "max_value": 65},
    "pompe_canada": {"type": "int", "min_value": 10, "max_value": 65},
}

# include the lower and upper limit - number of days
CONFIG_ENROLLMENT = {
    "ASMD_russia": {"type": "int", "min_value": 30, "max_value": None},
    "gaucher_australia": {"type": "int", "min_value": 30, "max_value": None},
    "pompe_canada": {"type": "int", "min_value": 30, "max_value": None},    
}

# Sampling for the control cohort
CONFIG_K_SAMPLING = {
    "ASMD_russia": 10000,
    "gaucher_australia": 1000,
    "pompe_canada": 1000,
}

# COMMAND ----------

DIAGS_FEATURES = {
    "ASMD_russia": ASMD_RUSSIA_DIAGS_FEATURES,
    "gaucher_australia": GAUCHER_AUSTRALIA_DIAGS_FEATURES,
    "pompe_canada": POMPE_CANADA_DIAGS_FEATURES,
}

LABS_FEATURES = {
    "ASMD_russia": ASMD_RUSSIA_LABS_FEATURES,
    "gaucher_australia": GAUCHER_AUSTRALIA_LABS_FEATURES,
    "pompe_canada": POMPE_CANADA_LABS_FEATURES,
}

PROCEDURES_FEATURES = {
    "ASMD_russia": ASMD_RUSSIA_PROC_FEATURES,
    "gaucher_australia": GAUCHER_AUSTRALIA_PROC_FEATURES,
    "pompe_canada": POMPE_CANADA_PROC_FEATURES,
}

TREATMENTS_FEATURES = {
    "ASMD_russia": {},
    "gaucher_australia": {},
    "pompe_canada": {},
}

OBSERVATIONS_FEATURES = {
    "ASMD_russia": {},
    "gaucher_australia": {},
    "pompe_canada": {},
}

MEASUREMENTS_FEATURES = {
    "ASMD_russia": {},
    "gaucher_australia": {},
    "pompe_canada": {},
}

SDS_FEATURES = {
    "ASMD_russia": {},
    "gaucher_australia": {},
    "pompe_canada": {},
}

# COMMAND ----------

ALGORITHM_BLOCKS = {
    "ASMD_russia": ASMD_RUSSIA_ALGORITHM_BLOCKS,
    "gaucher_australia": GAUCHER_AUSTRALIA_ALGORITHM_BLOCKS,
    "pompe_canada": POMPE_CANADA_ALGORITHM_BLOCKS,
}

GLOBAL_ALGORITHM_RULE = {
    "ASMD_russia": ASMD_RUSSIA_GLOBAL_ALGORITHM_RULE,
    "gaucher_australia": GAUCHER_AUSTRALIA_GLOBAL_ALGORITHM_RULE,
    "pompe_canada": POMPE_CANADA_GLOBAL_ALGORITHM_RULE,
}

ALGORITHM_BLOCK_TO_CATEGORY = {
    "ASMD_russia": ASMD_RUSSIA_ALGORITHM_BLOCK_TO_CATEGORY,
    "gaucher_australia": GAUCHER_AUSTRALIA_ALGORITHM_BLOCK_TO_CATEGORY,
    "pompe_canada": POMPE_CANADA_ALGORITHM_BLOCK_TO_CATEGORY,
}

ALGORITHM_CATEGORY_TO_MEDICAL_TERM = {
    "ASMD_russia": ASMD_RUSSIA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
    "gaucher_australia": GAUCHER_AUSTRALIA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
    "pompe_canada": POMPE_CANADA_ALGORITHM_CATEGORY_TO_MEDICAL_TERM,
}

MEDICAL_TERM_TO_ICD_CODES = {
    "ASMD_russia": ASMD_RUSSIA_MEDICAL_TERM_TO_ICD_CODES,
    "gaucher_australia": GAUCHER_AUSTRALIA_MEDICAL_TERM_TO_ICD_CODES,
    "pompe_canada": POMPE_CANADA_MEDICAL_TERM_TO_ICD_CODES,
}


# COMMAND ----------

FEATURES_CONFIG = {
  "diags": DIAGS_FEATURES,
  "labs": LABS_FEATURES,
  "procedures": PROCEDURES_FEATURES,
  "treatments": TREATMENTS_FEATURES,
  "observations": OBSERVATIONS_FEATURES,
  "measurements": MEASUREMENTS_FEATURES,
  "SDS": SDS_FEATURES,
}