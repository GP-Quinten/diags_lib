# Databricks notebook source
# from utils.io_functions import loading, saving
# from statistics.1_cohort_creation import utils

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC
# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

ALGO_OF_INTEREST = ["pompe_canada", "gaucher_australia", "ASMD_russia"]
COHORTS_OF_INTEREST = ["cohort", "like_cohort", "control_cohort"]
FEATURE = "cohorts"
TIMERANGE = "before_index_date" # must be one of "lifetime", "before_index_date" or "just_before_index_date"
EVALUATION_LEVEL = "rule" # must be one of "rule", "algorithm_block"

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    attrition_chart = get_attrition_stats(algo_name=algo_name,
                                        cohort=COHORT,
                                        config_age=CONFIG_AGE[algo_name], 
                                        config_enrollment=CONFIG_ENROLLMENT[algo_name], 
                                        config_sampling=CONFIG_K_SAMPLING[algo_name],
                                        intermediate_data_path=INTERMEDIATE_DATA_PATH,
                                        intermediate_LSD_cohort_filename="intermediate_LSD_cohort",
                                        intermediate_cohort_filename="intermediate_control_cohort")
    stats_demogs, age_groups = get_stats_demogs(algo_name=algo_name, 
                                    cohort=COHORT)
    
    write_spark_file(stats_demogs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_stats_demogs")
    write_spark_file(age_groups, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_age_repartition")
    write_spark_file(attrition_chart, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_attrition_chart")

# COMMAND ----------

# check outputs

for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_attrition_chart").display()
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_stats_demogs").display()
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_{COHORT}_age_repartition").display()

# COMMAND ----------

