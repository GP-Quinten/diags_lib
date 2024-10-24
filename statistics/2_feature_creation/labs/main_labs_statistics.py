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

for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    df_all_tests, df_labs = compute_generic_stats_on_labs(algo_name, COHORT, EVALUATION_LEVEL, TIMERANGE)
    write_spark_file(df_labs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_result_materials_per_labs")
    write_spark_file(df_all_tests, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_global_result_materials")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    read_spark_file(
      os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
      f"{algo_name}_{COHORT}_result_materials_per_labs"
      ).display()
    read_spark_file(
      os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
      f"{algo_name}_{COHORT}_global_result_materials"
      ).display()

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  df_threshold = get_labs_flagged(algo_name, COHORTS_OF_INTEREST, EVALUATION_LEVEL, [TIMERANGE])
  df_availability = get_labs_availability(algo_name, COHORTS_OF_INTEREST, [TIMERANGE])
  write_spark_file(df_availability, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_labs_availability")
  write_spark_file(df_threshold, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_labs_outside_threshold")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  read_spark_file(
    os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
    f"{algo_name}_labs_availability"
    ).display()
  read_spark_file(
    os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE),
    f"{algo_name}_labs_outside_threshold"
    ).display()

# COMMAND ----------

TUPLES = [("cohort","control_cohort"), ("cohort","like_cohort"), ("like_cohort","control_cohort")]
for algo_name in ALGO_OF_INTEREST:
  for TUPLE in TUPLES:
    stats_tests = get_stats_tests_on_labs(algo_name, comparaison_tuple=TUPLE, BALANCED_SAMPLES=True, timerange=TIMERANGE)
    write_spark_file(stats_tests, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"statistical_tests_{algo_name}_{TUPLE[0]}_{TUPLE[1]}")

# COMMAND ----------

# check output
TUPLES = [("cohort","control_cohort"), ("cohort","like_cohort"), ("like_cohort","control_cohort")]
for algo_name in ALGO_OF_INTEREST:
  for TUPLE in TUPLES:
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"statistical_tests_{algo_name}_{TUPLE[0]}_{TUPLE[1]}").display()

# COMMAND ----------

