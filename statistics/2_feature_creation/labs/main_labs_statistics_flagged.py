# Databricks notebook source
# from utils.io_functions import loading, saving
# from config.variables import base
# from statistics.2_feature_creation.labs import utils

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../../config/variables/base

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"]
COHORTS_OF_INTEREST = ["cohort", "like_cohort", "control_cohort"]
FEATURE = "labs"
TIMERANGE = "before_index_date" # must be one of "lifetime", "before_index_date" or "just_before_index_date"
EVALUATION_LEVEL = "rule" # must be one of "rule", "algorithm_block"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Flagged cohorts

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    df_all_tests, df_labs = compute_generic_stats_on_labs(algo_name, COHORT, EVALUATION_LEVEL, TIMERANGE, flagged=True)
    write_spark_file(df_labs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_result_materials_per_labs")
    write_spark_file(df_all_tests, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_global_result_materials")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    read_spark_file(
      os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
      f"{algo_name}_flagged_{COHORT}_result_materials_per_labs"
      ).display()
    read_spark_file(
      os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
      f"{algo_name}_flagged_{COHORT}_global_result_materials"
      ).display()

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  df_threshold = get_labs_flagged(algo_name, COHORTS_OF_INTEREST, EVALUATION_LEVEL, [TIMERANGE], flagged=True)
  write_spark_file(df_threshold, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_labs_outside_threshold")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  read_spark_file(
    os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
    f"{algo_name}_flagged_labs_outside_threshold"
    ).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### All flagged patients

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  df_all_tests, df_labs = compute_generic_stats_on_labs(algo_name, COHORTS_OF_INTEREST, EVALUATION_LEVEL, TIMERANGE, all_flagged=True)
  write_spark_file(df_labs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_result_materials_per_labs")
  write_spark_file(df_all_tests, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_global_result_materials")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  read_spark_file(
    os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
    f"{algo_name}_all_flagged_result_materials_per_labs"
    ).display()
  read_spark_file(
    os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
    f"{algo_name}_all_flagged_global_result_materials"
    ).display()

# COMMAND ----------

