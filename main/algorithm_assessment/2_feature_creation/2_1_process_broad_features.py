# Databricks notebook source
# from src.2_feature_creation import 2_1_process_broad_features

# COMMAND ----------

# MAGIC %run ../../../src/2_feature_creation/2_1_process_broad_features

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORTS_OF_INTEREST = ["cohort", "like_cohort", "control_cohort"] # cohorts names
FEATURE_CATEGORIES = ["diags", "labs", "procedures"] # must be a subset of the DATABASES_FEATURE_CONFIG keys defined in config/data_provider
GENERIC_N_DIGITS = 3 # can be 4 or even 5. Level of mapping of an ICD code to its troncated code description (troncated to 3 digits), in order to have more global/comprehensive info on the diagnoses, and to be able to later make a grouping on this level of precision

# COMMAND ----------

# Preprocessing of features
for algo_name in ALGO_OF_INTEREST:
  for cohort_name in COHORTS_OF_INTEREST:
    for feature_name in FEATURE_CATEGORIES:
      get_preprocessed_features(
        algo_name, 
        cohort_name, 
        feature_name, 
        output_data_path=os.path.join(INTERMEDIATE_DATA_PATH, algo_name, feature_name), 
        output_filename=f"{algo_name}_{cohort_name}_preprocessed_{feature_name}", 
        diags_mapping_approximation=False
        )

# COMMAND ----------

# Processing of features
for algo_name in ALGO_OF_INTEREST:
  for cohort_name in COHORTS_OF_INTEREST:
    for feature_name in FEATURE_CATEGORIES:
      get_processed_features(
        algo_name, 
        cohort_name, 
        feature_name,
        generic_n_digits=GENERIC_N_DIGITS,
        input_data_path=os.path.join(INTERMEDIATE_DATA_PATH, algo_name, feature_name),
        input_filename=f"{algo_name}_{cohort_name}_preprocessed_{feature_name}",
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
        output_filename=f"{algo_name}_{cohort_name}_final_{BROAD_FEATURE_TYPE}_{feature_name}", 
        )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  for cohort_name in COHORTS_OF_INTEREST:
    for feature_name in FEATURE_CATEGORIES:
      read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{cohort_name}_final_{BROAD_FEATURE_TYPE}_{feature_name}").display()

# COMMAND ----------

