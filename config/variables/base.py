# Databricks notebook source
"""script to define base variables"""

# COMMAND ----------

import numpy as np

# COMMAND ----------

# from config.data_provider import base
# from config.ICD10 import base
# from config.variables import cohort_creation, feature_creation, storage

# COMMAND ----------

# MAGIC %run ../data_provider/base

# COMMAND ----------

# MAGIC %run ../ICD10/base

# COMMAND ----------

# MAGIC %run ./cohort_creation

# COMMAND ----------

# MAGIC %run ./feature_creation

# COMMAND ----------

# MAGIC %run ./storage

# COMMAND ----------

CONFIG_RANDOM_SEED = 123

# COMMAND ----------

# patient information that should be provided in the database
COL_UUID = PATIENTS["COL_UUID"]
COL_BIRTH_YEAR = PATIENTS["COL_BIRTH_YEAR"]
COL_DATE_OF_DEATH = PATIENTS["COL_DATE_OF_DEATH"]
COL_GENDER = PATIENTS["COL_GENDER"]
COL_FIRST_MONTH_ACTIVE = PATIENTS["COL_FIRST_ACTIVITY"]
COL_LAST_MONTH_ACTIVE = PATIENTS["COL_LAST_ACTIVITY"]
COL_REGION = PATIENTS["COL_REGION"]
COL_DECEASED_INDICATOR = PATIENTS["COL_DECEASED_INDICATOR"]

COL_TO_KEEP_PATIENTS = [value for key, value in PATIENTS.items() if key.startswith("COL_")]

# COMMAND ----------

# diagnosis informations that should be provided in the database
COL_DIAG_CODE = DIAGNOSES["COL_INFO"]
COL_DIAG_DATE = DIAGNOSES["COL_DATE"]
DIAG_DATE_FORMAT = DIAGNOSES["DATE_FORMAT"]
COL_DIAGNOSIS_STATUS = DIAGNOSES["COL_STATUS"]
COL_TO_KEEP_DIAGNOSES = [value for key, value in DIAGNOSES.items() if key.startswith("COL_")]

# treatment informations that should be provided in the database
COL_TREATMENT_CODE = TREATMENTS["COL_INFO"]
COL_TREATMENT_DATE = TREATMENTS["COL_DATE"]
TREATMENT_DATE_FORMAT = TREATMENTS["DATE_FORMAT"]
COL_TO_KEEP_TREATMENTS = [value for key, value in TREATMENTS.items() if key.startswith("COL_")]

# COMMAND ----------

# variables for demographic statistics

CONF_STATS_DEMOG = {
    "continuous": [
        COL_ENROLLMENT_DURATION,
        COL_LOOKBACK_PERIOD,
        COL_AGE_AT_INDEX_DATE,
        COL_AGE_AT_LAST_ACTIVE_DATE,
    ],
    "discrete": [COL_DECEASED_INDICATOR, COL_GENDER],
}

CONF_AGE_BINS = [0, 3, 9, 17, 35, 45, 55, 65, 75, 85, np.inf]
CONF_AGE_LABELS = ["0-3", "4-9", "10-17", "18-35", "36-45", "46-55", "56-65", "66-75", "76-85", "+85"]