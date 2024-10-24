# Databricks notebook source
import os

# COMMAND ----------

# from src.4_algorithm_evaluation.medical_driven import algorithm_evaluation

# COMMAND ----------

# MAGIC %run ../../../src/4_algorithm_evaluation/medical_driven/algorithm_evaluation

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORTS_OF_INTEREST = ["cohort", "like_cohort", "control_cohort"] # cohorts names
TIMERANGE = "before_index_date" # must be one of "lifetime", "before_index_date" or "just_before_index_date"
EVALUATION_LEVEL = "rule" # must be a subset of "rule", "algorithm_block

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  get_performance(
    algo_name=algo_name, 
    cohorts_of_interest=COHORTS_OF_INTEREST,
    timerange=TIMERANGE,
    evaluation_level=EVALUATION_LEVEL,
    input_data_path=os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name),
    output_data_path=os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name),
    output_filename=f"{algo_name}_performance_{TIMERANGE}_per_{EVALUATION_LEVEL}"
  )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  df_global_performance = read_spark_file(
    os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name), 
    f"{algo_name}_performance_{TIMERANGE}_per_{EVALUATION_LEVEL}"
    ).display()

# COMMAND ----------

