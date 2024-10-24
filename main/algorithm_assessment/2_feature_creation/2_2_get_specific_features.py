# Databricks notebook source
# from src.2_feature_creation import 2_2_get_specific_features

# COMMAND ----------

# MAGIC %run ../../../src/2_feature_creation/2_2_get_specific_features

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORTS_OF_INTEREST = ["cohort", "like_cohort", "control_cohort"] # cohorts names
FEATURE_CATEGORIES = ["diags", "labs", "procedures"] # must be a subset of the DATABASES_FEATURE_CONFIG keys defined in config/data_provider

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  for cohort_name in COHORTS_OF_INTEREST:
    for feature_name in FEATURE_CATEGORIES:
      get_specific_features(
        algo_name,
        feature_name, 
        input_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name),
        input_filename=f"{algo_name}_{cohort_name}_final_{BROAD_FEATURE_TYPE}_{feature_name}",
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
        output_filename=f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}", 
        )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  for cohort_name in COHORTS_OF_INTEREST:
    for feature_name in FEATURE_CATEGORIES:
      read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}").display()

# COMMAND ----------

