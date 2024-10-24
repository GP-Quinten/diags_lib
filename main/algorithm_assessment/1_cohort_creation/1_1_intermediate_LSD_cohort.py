# Databricks notebook source
# from config.algo import global_LSD_conf
# from src.1_cohort_creation import 1_1_intermediate_LSD_cohort

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../../src/1_cohort_creation/1_1_intermediate_LSD_cohort

# COMMAND ----------

get_intermediate_LSD_cohort(
  diagnoses_codes=ALL_LSD_DIAG_CODES, 
  treatment_codes=ALL_LSD_TREATMENT_CODES, 
  storage_data_path=INTERMEDIATE_DATA_PATH, 
  output_filename="intermediate_LSD_cohort"
  )

# COMMAND ----------

# check output
read_spark_file(INTERMEDIATE_DATA_PATH, "intermediate_LSD_cohort").display()

# COMMAND ----------

