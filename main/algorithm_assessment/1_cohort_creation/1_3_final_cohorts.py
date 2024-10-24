# Databricks notebook source
# from config.algo import global_algo_conf
# from src.1_cohort_creation import 1_3_final_cohorts

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../src/1_cohort_creation/1_3_final_cohorts

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  LSD_name = algo_name.split('_')[0]
  get_LSD_and_LSD_like_cohorts(
    LSD_name=LSD_name, 
    config_age=CONFIG_AGE[algo_name], 
    config_enrollment=CONFIG_ENROLLMENT[algo_name], 
    intermediate_data_path=INTERMEDIATE_DATA_PATH,
    cohort_data_path=os.path.join(COHORT_DATA_PATH, algo_name), 
    intermediate_LSD_cohort_filename = "intermediate_LSD_cohort",
    output_LSD_filename = f"{algo_name}_cohort",
    output_LSD_like_filename = f"{algo_name}_like_cohort"
    )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  LSD_name = algo_name.split('_')[0]
  df_LSD_potential = read_spark_file(os.path.join(INTERMEDIATE_DATA_PATH, LSD_name), f"{LSD_name}_potential_patients")
  df_LSD_cohort = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_cohort")
  df_LSD_like_cohort = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_like_cohort")
  print()
  print(algo_name)
  print('------------')
  print(f"Patients: {df_LSD_potential.count()}")
  print(f"LSD cohort: {df_LSD_cohort.count()} (found via diagnoses: {df_LSD_cohort.filter(F.col('N_CERTAIN_DIAGS') >= 2).count()}, found via treatments: {df_LSD_cohort.filter(F.col('N_CERTAIN_DIAGS') < 2).count()})")
  print(f"LSD_like cohort: {df_LSD_like_cohort.count()} (uncertain diagnosis: {df_LSD_like_cohort.filter(F.col('UNCERTAINTY_REASON') == 'uncertain LSD diagnosis').count()}, other certain diagnoses for another LSD: {df_LSD_like_cohort.filter(F.col('UNCERTAINTY_REASON') == 'other certain LSD diagnosis').count()})")
  print()
  df_LSD_potential.display()
  df_LSD_cohort.display()
  df_LSD_like_cohort.display()

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  get_LSD_control_cohort(
    config_age=CONFIG_AGE[algo_name], 
    config_enrollment=CONFIG_ENROLLMENT[algo_name], 
    config_sampling=CONFIG_K_SAMPLING[algo_name],
    intermediate_data_path=INTERMEDIATE_DATA_PATH,
    cohort_data_path=os.path.join(COHORT_DATA_PATH, algo_name), 
    intermediate_control_cohort_filename = "intermediate_control_cohort",
    LSD_cohort_filename = f"{algo_name}_cohort",
    output_filename = f"{algo_name}_control_cohort"
    )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  control_cohort_display = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_control_cohort")
  print()
  print(algo_name)
  print('------------')
  print(f"LSD control cohort: {control_cohort_display.count()}")
  print()
  control_cohort_display.display()

# COMMAND ----------

