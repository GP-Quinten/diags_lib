# Databricks notebook source
# from config.algo import global_algo_conf
# from src.1_cohort_creation import 1_4_screening_cohort

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../src/1_cohort_creation/1_4_screening_cohort

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORT_NAME = "screening_cohort"
DB_SAMPLE_RATIO = 0.001 # ratio use to sample the initial database and avoid computational issue

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  get_LSD_screening_cohort(
      sample_ratio=DB_SAMPLE_RATIO,
      config_age=CONFIG_AGE[algo_name], 
      config_enrollment=CONFIG_ENROLLMENT[algo_name], 
      cohort_data_path=os.path.join(COHORT_DATA_PATH, algo_name), 
      output_filename = f"{algo_name}_{COHORT_NAME}"
  )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  screening_cohort_display = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{COHORT_NAME}")
  print()
  print(algo_name)
  print('------------')
  print(f"LSD screening cohort: {screening_cohort_display.count()}")
  print()
  screening_cohort_display.display()

# COMMAND ----------

