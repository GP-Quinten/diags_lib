# Databricks notebook source
# from config.algo import global_algo_conf, global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../config/algo/global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../config/algo/global_algo_conf

# COMMAND ----------

# must be one of "all", "all_but_initialization", "cohort_initialization", "cohort_creation", "feature_creation", "algorithm_implementation" or "algorithm_evaluation"
STEP_MODE = "all"

# COMMAND ----------

# Name of the algorithms (LSD type + country)
ALGO_OF_INTEREST = [
  "pompe_canada", 
  "gaucher_australia", 
  "ASMD_russia"
  ]

# cohort names
COHORTS_OF_INTEREST = [
  "cohort", 
  "like_cohort", 
  "control_cohort"
  ]

 # must be a subset of the DATABASES_FEATURE_CONFIG keys defined in config/data_provider
FEATURE_CATEGORIES = [
  "diags", 
  "labs",
  "procedures"
  ]

# must be one of "lifetime", "before_index_date" or "just_before_index_date"
TIMERANGE = "before_index_date"

# must be one of "rule", "algorithm_block"
EVALUATION_LEVEL = "rule"

# Level of mapping of an ICD code to its troncated code description, 
# in order to have more global/comprehensive info on the diagnoses, 
# and to be able to later make a grouping on this level of precision
GENERIC_N_DIGITS = 3 

# ratio use to sample the initial control database and avoid computational issue
DB_SAMPLE_RATIO = 0.05

# COMMAND ----------

# from src.1_cohort_creation import 1_1_intermediate_LSD_cohort, 1_2_intermediate_control_cohort, 1_3_final_cohorts

# COMMAND ----------

# MAGIC %run ../../src/1_cohort_creation/1_1_intermediate_LSD_cohort

# COMMAND ----------

# MAGIC %run ../../src/1_cohort_creation/1_2_intermediate_control_cohort

# COMMAND ----------

# MAGIC %run ../../src/1_cohort_creation/1_3_final_cohorts

# COMMAND ----------

if STEP_MODE in ["all", "cohort_initialization"]:
  print("starting cohort initialization step")
  get_intermediate_LSD_cohort(
    diagnoses_codes=ALL_LSD_DIAG_CODES, 
    treatment_codes=ALL_LSD_TREATMENT_CODES, 
    storage_data_path=INTERMEDIATE_DATA_PATH, 
    output_filename="intermediate_LSD_cohort"
    )
  print("intermediate LSD cohort has been generated")
  get_intermediate_control_cohort(
    sample_ratio=DB_SAMPLE_RATIO, 
    intermediate_LSD_cohort_filename="intermediate_LSD_cohort", 
    storage_data_path=INTERMEDIATE_DATA_PATH, 
    output_filename="intermediate_control_cohort"
    )
  print("intermediate control cohort has been generated")
  print("cohort initialization step done")

# COMMAND ----------

if STEP_MODE in ["all", "all_but_initialization", "cohort_creation"]:
  print("starting cohort creation step")
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
    print(f"{algo_name} cohort has been generated")
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
    print(f"{algo_name} control cohort has been generated")
  print("cohort creation step done")

# COMMAND ----------

# from src.2_feature_creation import 2_1_process_broad_features, 2_2_get_specific_features, 2_3_get_features_matrix_medical_driven

# COMMAND ----------

# MAGIC %run ../../src/2_feature_creation/2_1_process_broad_features

# COMMAND ----------

# MAGIC %run ../../src/2_feature_creation/2_2_get_specific_features

# COMMAND ----------

# MAGIC %run ../../src/2_feature_creation/2_3_get_features_matrix_medical_driven

# COMMAND ----------

if STEP_MODE in ["all", "all_but_initialization", "feature_creation"]:
  print("starting feature creation step")
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
        get_specific_features(
          algo_name,
          feature_name, 
          input_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name),
          input_filename=f"{algo_name}_{cohort_name}_final_{BROAD_FEATURE_TYPE}_{feature_name}",
          output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
          output_filename=f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}", 
          )
      print(f"features for {algo_name} {cohort_name} have been generated")
      # get cohort
      df_cohort = read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{cohort_name}").select(PATIENT_UUID, COL_AGE_AT_INDEX_DATE, COL_GENDER)
      # get labs feature matrix
      feature_name = "labs"
      df_labs = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
      labs_feature_matrix = get_labs_feature_matrix(
        algo_name, 
        timerange=TIMERANGE, 
        df_labs=df_labs, 
        df_cohort=df_cohort, 
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name),
        first_output_filename=f"{algo_name}_{cohort_name}_combined_{feature_name}_{TIMERANGE}", 
        second_output_filename=f"{algo_name}_{cohort_name}_{feature_name}_feature_matrix_{TIMERANGE}"
      )
      # get diags feature matrix
      feature_name = "diags"
      df_diags = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
      diags_feature_matrix = get_diags_feature_matrix(
        algo_name, 
        timerange=TIMERANGE, 
        max_timespan=MAX_TIMESPAN,
        df_diags=df_diags, 
        df_cohort=df_cohort, 
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
        output_filename=f"{algo_name}_{cohort_name}_{feature_name}_feature_matrix_{TIMERANGE}"
      )
      # get procedures feature matrix
      feature_name = "procedures"
      df_procedures = read_spark_file(os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), f"{algo_name}_{cohort_name}_final_{SPECIFIC_FEATURE_TYPE}_{feature_name}")
      procedures_feature_matrix = get_procedures_feature_matrix(
        algo_name, 
        timerange=TIMERANGE,
        max_timespan=MAX_TIMESPAN,
        df_procedures=df_procedures, 
        df_cohort=df_cohort, 
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name, feature_name), 
        output_filename=f"{algo_name}_{cohort_name}_{feature_name}_feature_matrix_{TIMERANGE}"
      )
      # get final feature matrix
      get_final_feature_matrix(
        df_cohort,
        labs_feature_matrix,
        diags_feature_matrix,
        procedures_feature_matrix,
        output_data_path=os.path.join(FEATURE_DATA_PATH, algo_name),
        output_filename=f"{algo_name}_{cohort_name}_final_feature_matrix_{TIMERANGE}"
        )
      print(f"feature matrix for {algo_name} {cohort_name} has been generated")
  print("feature creation step done")

# COMMAND ----------

# from src.3_algorithm_implementation.medical_driven import algorithm_implementation

# COMMAND ----------

# MAGIC %run ../../src/3_algorithm_implementation/medical_driven/algorithm_implementation

# COMMAND ----------

if STEP_MODE in ["all", "all_but_initialization", "algorithm_implementation"]:
  print("starting algorithm implementation step")
  for algo_name in ALGO_OF_INTEREST:
    for cohort_name in COHORTS_OF_INTEREST:
      get_availability_and_label(
        algo_name=algo_name, 
        level=EVALUATION_LEVEL, 
        input_data_path=os.path.join(FEATURE_DATA_PATH, algo_name), 
        input_filename=f"{algo_name}_{cohort_name}_final_feature_matrix_{TIMERANGE}", 
        output_data_path=os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name), 
        output_filename=f"{algo_name}_{cohort_name}_final_labels_{TIMERANGE}_per_{EVALUATION_LEVEL}"
        )
      print(f"labels for {algo_name} {cohort_name} have been generated")
  
  for algo_name in ALGO_OF_INTEREST:
    save_flagged_cohorts(algo_name, COHORTS_OF_INTEREST, TIMERANGE)
  print("algorithm implementation step done")

# COMMAND ----------

# from src.4_algorithm_evaluation.medical_driven import algorithm_evaluation

# COMMAND ----------

# MAGIC %run ../../src/4_algorithm_evaluation/medical_driven/algorithm_evaluation

# COMMAND ----------

if STEP_MODE in ["all", "all_but_initialization", "algorithm_evaluation"]:
  print("starting algorithm evaluation step")
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
    print(f"performance for {algo_name} have been generated")
  print("algorithm evaluation step done")

# COMMAND ----------

