# Databricks notebook source
# from src.3_algorithm_implementation.medical_driven import algorithm_implementation

# COMMAND ----------

# MAGIC %run ../../../src/3_algorithm_implementation/medical_driven/algorithm_implementation

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORT_NAME = "screening_cohort"
TIMERANGE = "lifetime"
EVALUATION_LEVELS = ["rule", "algorithm_block"] # must be a subset of "rule", "algorithm_block

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  for evaluation_level in EVALUATION_LEVELS:
    get_availability_and_label(
      algo_name=algo_name, 
      level=evaluation_level, 
      input_data_path=os.path.join(FEATURE_DATA_PATH, algo_name), 
      input_filename=f"{algo_name}_{COHORT_NAME}_final_feature_matrix_{TIMERANGE}", 
      output_data_path=os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name), 
      output_filename=f"{algo_name}_{COHORT_NAME}_final_labels_{TIMERANGE}_per_{evaluation_level}"
      )

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  save_flagged_cohorts(algo_name, [COHORT_NAME], TIMERANGE)

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  flagged_cohort = read_spark_file(
    os.path.join(COHORT_DATA_PATH, algo_name), 
    f"{algo_name}_{COHORT_NAME}_flagged_{TIMERANGE}"
    )
  print()
  print(algo_name)
  print('------------')
  print(f"Number of flagged people: {flagged_cohort.count()}")
  print()
  flagged_cohort.display()

# COMMAND ----------

