# Databricks notebook source
# MAGIC %md
# MAGIC ## Import functions and config

# COMMAND ----------

# from statistics.2_feature_creation.diags import utils

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stats on diags specific to the algorithm

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prevalence and lift

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"]
TIMERANGE = "before_index_date" # must be one of "lifetime", "before_index_date" or "just_before_index_date"
FEATURE_TYPE = SPECIFIC_FEATURE_TYPE # must be one of SPECIFIC_FEATURE_TYPE or BROAD_FEATURE_TYPE
DB_CONFIG = DIAGNOSES 
FLAGGED = False

# COMMAND ----------

MAIN_COHORT = "cohort"
OTHER_COHORTS = ["like_cohort", "control_cohort"]
LIST_OF_COLS_TO_GROUP_ON = [[COL_ALGO_MEDICAL_GROUP], [COL_ALGO_MEDICAL_TERM, COL_ALGO_MEDICAL_GROUP]] 
##  LIST_OF_COLS_TO_GROUP_ON is a list of list of columns to group on. Its elements must be lists of elements among: COL_ICD_MEDICAL_TERM, COL_ALGO_MEDICAL_TERM, COL_ALGO_MEDICAL_GROUP, COL_ALGO_MEDICAL_BLOCK (by descending precision). The first element of the list being the highest level of precision

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
    print("")
    print("")
    print("--------------------------")
    print(algo_name)
    print("--------------------------")
    print("")
    for COLS_TO_GROUP_ON in LIST_OF_COLS_TO_GROUP_ON:
      print("")
      print("---------")
      print(algo_name)
      if MAIN_COHORT=="all_cohorts":
          print(MAIN_COHORT)
      COL_STATS_LEVEL = COLS_TO_GROUP_ON[0].lower()
      print(f"Level of grouping : {COL_STATS_LEVEL}")

      prev_df = get_prevalence_and_lift(algo_name, MAIN_COHORT, OTHER_COHORTS, FEATURE_TYPE, COLS_TO_GROUP_ON, DB_CONFIG, TIMERANGE, flagged = FLAGGED)

      if MAIN_COHORT=="all_cohorts":
          prev_df = prev_df[prev_df["COUNT_ALL_COHORTS"]>0]
      else : 
          prev_df = prev_df[prev_df["COUNT_COHORT"]>0]

      # Saving tables
      if FLAGGED:
          write_spark_file(
              prev_df,
              os.path.join(STATISTICS_DATA_PATH, "diags", "results", "flagged"),
              f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL}_diags_results_{TIMERANGE}_flagged",
              file_type="csv",
          )
      else:
          write_spark_file(
              prev_df,
              os.path.join(STATISTICS_DATA_PATH, "diags", "results"),
              f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL}_diags_results_{TIMERANGE}",
              file_type="csv",
          )
        
      # prev_df.display()
      print("---------")
      print("")

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
    print("")
    print("")
    print("--------------------------")
    print(algo_name)
    print("--------------------------")
    print("")
    for COLS_TO_GROUP_ON in LIST_OF_COLS_TO_GROUP_ON:
        print("")
        print("---------")
        print(algo_name)
        if MAIN_COHORT=="all_cohorts":
            print(MAIN_COHORT)
        COL_STATS_LEVEL = COLS_TO_GROUP_ON[0].lower()
        print(f"Level of grouping : {COL_STATS_LEVEL}")

        # Saving tables
        if FLAGGED:
            prev_df = read_spark_file(
                os.path.join(STATISTICS_DATA_PATH, "diags", "results", "flagged"),
                f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL}_diags_results_{TIMERANGE}_flagged",
                file_type="csv", pandas_output=True,
            )
        else:
            prev_df = read_spark_file(
                os.path.join(STATISTICS_DATA_PATH, "diags", "results"),
                f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL}_diags_results_{TIMERANGE}",
                file_type="csv", pandas_output=True,
            )
        prev_df = prev_df.sort_values(by=f"PCT_{MAIN_COHORT.upper()}", ascending=False)
        prev_df.display()
        print("---------")
        print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Counting total number of diags per patient

# COMMAND ----------

COHORTS = ["cohort", "like_cohort", "control_cohort"]
COL_STATS_LEVEL = COL_ALGO_MEDICAL_GROUP # For specific diags, must be an element among: COL_ICD_MEDICAL_TERM, COL_ALGO_MEDICAL_TERM, COL_ALGO_MEDICAL_GROUP, COL_ALGO_MEDICAL_BLOCK (by descending precision)

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  print('')
  print('------------')
  print(algo_name)
  print('------------')
  stats_df = get_diags_stats(algo_name, COHORTS, FEATURE_TYPE, COL_STATS_LEVEL, DB_CONFIG, TIMERANGE, FLAGGED)
  # stats_df.display()
  if FLAGGED:
      write_spark_file(
          stats_df,
          os.path.join(STATISTICS_DATA_PATH, "diags", "counts", "flagged"),
          f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL.lower()}_diags_counts_{TIMERANGE}_flagged",
          file_type="csv",
      )
  else:
      write_spark_file(
          stats_df,
          os.path.join(STATISTICS_DATA_PATH, "diags", "counts"),
          f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL.lower()}_diags_counts_{TIMERANGE}",
          file_type="csv",
      )
  print()

# COMMAND ----------

# check output
for algo_name in ALGO_OF_INTEREST:
  print('')
  print('------------')
  print(algo_name)
  print('------------')
  if FLAGGED:
      stats_df = read_spark_file(
          os.path.join(STATISTICS_DATA_PATH, "diags", "counts", "flagged"),
          f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL.lower()}_diags_counts_{TIMERANGE}_flagged",
          file_type="csv",
      )
  else:
      stats_df = read_spark_file(
          os.path.join(STATISTICS_DATA_PATH, "diags", "counts"),
          f"{algo_name}_stats_{FEATURE_TYPE}_{COL_STATS_LEVEL.lower()}_diags_counts_{TIMERANGE}",
          file_type="csv",
      )
  stats_df.display()
  print()