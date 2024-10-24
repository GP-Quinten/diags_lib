# Databricks notebook source
# from utils.io_functions import loading, saving
# from statistics.1_cohort_creation import utils

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

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
    stats_demogs, age_groups = get_stats_demogs(algo_name=algo_name, 
                                    cohort=COHORT,
                                    timerange=TIMERANGE,
                                    level=EVALUATION_LEVEL,
                                    flagged=True)
    write_spark_file(stats_demogs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_stats_demogs")
    write_spark_file(age_groups, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_age_repartition")


# COMMAND ----------

# check outputs

for algo_name in ALGO_OF_INTEREST:
  for COHORT in COHORTS_OF_INTEREST:
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_stats_demogs").display()
    read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_flagged_{COHORT}_age_repartition").display()

# COMMAND ----------

for algo_name in ALGO_OF_INTEREST:
  stats_demogs, age_groups = get_stats_demogs(algo_name=algo_name, 
                                  cohort=None,
                                  timerange=TIMERANGE,
                                  level=EVALUATION_LEVEL,
                                  all_flagged=True)
  write_spark_file(stats_demogs, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_cohorts_stats_demogs")
  write_spark_file(age_groups, os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_cohorts_age_repartition")

# COMMAND ----------

# check outputs

for algo_name in ALGO_OF_INTEREST:
  read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_cohorts_stats_demogs").display()
  read_spark_file(os.path.join(STATISTICS_DATA_PATH, algo_name, FEATURE), f"{algo_name}_all_flagged_cohorts_age_repartition").display()

# COMMAND ----------

