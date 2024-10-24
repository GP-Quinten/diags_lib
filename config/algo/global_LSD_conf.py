# Databricks notebook source
import os

# COMMAND ----------

# from config.algo.ASMD import base
# from config.algo.gaucher import base
# from config.algo.pompe import base
# from config.algo.other_LSD import base

# COMMAND ----------

# MAGIC %run ./ASMD/base

# COMMAND ----------

# MAGIC %run ./gaucher/base

# COMMAND ----------

# MAGIC %run ./pompe/base

# COMMAND ----------

# MAGIC %run ./other_LSD/base

# COMMAND ----------

# number of certain diagnoses that are needed for a patient to be included in the disease cohort
NB_CERTAIN_DIAGS = 2
# number of treatment prescription that are needed for a patient to be included in the disease cohort
NB_TREATMENTS = 1

# COMMAND ----------

LSD_GOAL_STATUSES = ["Diagnosis of"]

# COMMAND ----------

PREVALENCE_RATES = {
  "ASMD": ASMD_PREVALENCE,
  "gaucher": GAUCHER_PREVALENCE,
  "pompe": POMPE_PREVALENCE,
}

# COMMAND ----------

# Codes related to lysosomal storage disorders
LSD_TO_CODE = {
    "ASMD": ASMD_CODES,
    "fabry": FABRY_CODES, 
    "gangliosidosis": GANGLIOSIDOSIS_CODES,
    "gaucher": GAUCHER_CODES,
    "niemann_pick_without_ASMD": NIEMANN_PICK_WITHOUT_ASMD_CODES,
    "other_LSD": OTHER_LSD_CODES,
    "pompe": POMPE_CODES 
}

# COMMAND ----------

ALL_LSD_DIAG_CODES = {
  "ICD10": ASMD_ICD10_CODES + FABRY_ICD10_CODES + GANGLIOSIDOSIS_ICD10_CODES + GAUCHER_ICD10_CODES + NIEMANN_PICK_WITHOUT_ASMD_ICD10_CODES + OTHER_LSD_ICD10_CODES + POMPE_ICD10_CODES
  }

# COMMAND ----------

ALL_LSD_TREATMENT_CODES = {
  "NDC": ASMD_NDC_CODES + FABRY_NDC_CODES + GANGLIOSIDOSIS_NDC_CODES + GAUCHER_NDC_CODES + NIEMANN_PICK_WITHOUT_ASMD_NDC_CODES + OTHER_LSD_NDC_CODES + POMPE_NDC_CODES
  }

# COMMAND ----------

def reverse_mapping(initial_dict):
    reverse_dict = {}
    for symptom, codes in initial_dict.items():
        for code in codes:
            reverse_dict[code] = symptom
    return reverse_dict

CODE_TO_LSD = reverse_mapping(LSD_TO_CODE)