# Databricks notebook source
"""script to define feature creation variables"""

# COMMAND ----------

# variables related to feature creation part

COL_FEATURE_TYPE = "FEATURE_TYPE"
SPECIFIC_FEATURE_TYPE = "specific"
BROAD_FEATURE_TYPE = "broad"

COL_EXPERT_LOWER_BOUND = "EXPERT_LOWER_BOUND"
COL_EXPERT_UPPER_BOUND = "EXPERT_UPPER_BOUND"

COL_STATS_LOWER_BOUND = "STATS_LOWER_BOUND"
COL_STATS_UPPER_BOUND = "STATS_UPPER_BOUND"

COL_IS_MAIN_UNIT = "IS_MAIN_UNIT"
COL_IS_OUTLIER = "IS_OUTLIER"
COL_IS_GOAL_STATUS = "IS_GOAL_STATUS"

# number of month before index date to look at in "just_before_index_date" mode for diags
MAX_TIMESPAN = 24

# COMMAND ----------

# Names of the different medical description related columns to create
COL_ICD_MEDICAL_TERM = "MEDICAL_TERM_ICD"
COL_ICD_MEDICAL_GROUP = "MEDICAL_GROUP_ICD"
COL_ALGO_MEDICAL_TERM = "MEDICAL_TERM_ALGO"
COL_ALGO_MEDICAL_GROUP = "MEDICAL_GROUP_ALGO"
COL_ALGO_MEDICAL_BLOCK = "MEDICAL_BLOCK_ALGO"