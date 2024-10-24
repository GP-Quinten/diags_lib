# Databricks notebook source
# from src.2_feature_creation import 2_3_get_features_matrix_medical_driven

# COMMAND ----------

# MAGIC %run ../../../src/2_feature_creation/2_3_get_features_matrix_medical_driven

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"] # Name of the algorithms (LSD type + country)
COHORT_NAME = "screening_cohort"
TIMERANGE = "lifetime"

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  # get cohort
  df_cohort = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{COHORT_NAME}").select(PATIENT_UUID, COL_AGE_AT_INDEX_DATE, COL_GENDER)
  # get labs feature matrix
  feature_name = "labs"
  df_labs = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{COHORT_NAME}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
  labs_feature_matrix = get_labs_feature_matrix(
    algo_name, 
    timerange=TIMERANGE, 
    df_labs=df_labs, 
    df_cohort=df_cohort, 
    output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name),
    first_output_filename=f"{algo_name}_{COHORT_NAME}_combined_{feature_name}_{TIMERANGE}", 
    second_output_filename=f"{algo_name}_{COHORT_NAME}_{feature_name}_feature_matrix_{TIMERANGE}"
  )
  # get diags feature matrix
  feature_name = "diags"
  df_diags = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{COHORT_NAME}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
  diags_feature_matrix = get_diags_feature_matrix(
    algo_name, 
    timerange=TIMERANGE,
    max_timespan=MAX_TIMESPAN,
    df_diags=df_diags, 
    df_cohort=df_cohort, 
    output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
    output_filename=f"{algo_name}_{COHORT_NAME}_{feature_name}_feature_matrix_{TIMERANGE}"
  )
  # get procedures feature matrix
  feature_name = "procedures"
  df_procedures = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{COHORT_NAME}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
  procedures_feature_matrix = get_procedures_feature_matrix(
    algo_name, 
    timerange=TIMERANGE,
    max_timespan=MAX_TIMESPAN,
    df_procedures=df_procedures, 
    df_cohort=df_cohort, 
    output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
    output_filename=f"{algo_name}_{COHORT_NAME}_{feature_name}_feature_matrix_{TIMERANGE}"
  )
  # get final feature matrix
  get_final_feature_matrix(
    df_cohort,
    labs_feature_matrix,
    diags_feature_matrix,
    procedures_feature_matrix,
    output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name),
    output_filename=f"{algo_name}_{COHORT_NAME}_final_feature_matrix_{TIMERANGE}"
    )

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  read_spark_file(
    os.path.join(FEATURE_DATA_PATH, algo_name), 
    f"{algo_name}_{COHORT_NAME}_final_feature_matrix_{TIMERANGE}"
    ).display()

# COMMAND ----------

