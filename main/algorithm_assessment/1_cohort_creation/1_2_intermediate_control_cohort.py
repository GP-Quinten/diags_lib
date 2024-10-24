# Databricks notebook source
# from src.1_cohort_creation import 1_2_intermediate_control_cohort

# COMMAND ----------

# MAGIC %run ../../../src/1_cohort_creation/1_2_intermediate_control_cohort

# COMMAND ----------

DB_SAMPLE_RATIO = 0.05 # ratio use to sample the initial database and avoid computational issue

# COMMAND ----------

get_intermediate_control_cohort(
  sample_ratio=DB_SAMPLE_RATIO, 
  intermediate_LSD_cohort_filename="intermediate_LSD_cohort", 
  storage_data_path=INTERMEDIATE_DATA_PATH, 
  output_filename="intermediate_control_cohort"
)

# COMMAND ----------

# check output
read_spark_file(INTERMEDIATE_DATA_PATH, "intermediate_control_cohort").display()

# COMMAND ----------

