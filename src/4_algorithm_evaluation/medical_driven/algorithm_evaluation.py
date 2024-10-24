# Databricks notebook source
from typing import List

import pandas as pd

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_algo_conf, global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../../config/variables/base

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_algo_conf

# COMMAND ----------

def get_performance(
    algo_name: str,
    cohorts_of_interest: List,
    timerange: str,
    evaluation_level: str,
    input_data_path: str,
    output_data_path: str,
    output_filename: str,
):
    """Get performance of the expert driven algorithm. Returns a dataframe with the following information:
        - cohort_type: type of the cohort the figures are related to
        - n_people: number of people in the initial cohort
        - rule_X_availability: number of people that have enough data to go through the given rule X
        - minimal_global_availability per rule: number of people that have enough data to go through at least one rule
        - strict_global_availability per rule: number of people that have enough data to go through all the rules
        - rule_X_label: number of people that have been flagged by this rule
        - global_label: number of people that have been flagged by at least one rule

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        cohorts_of_interest (List): Name of the cohorts to compute the perfomance on
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)
        evaluation_level (str): Level at which the performance are computed (rule, algorithm_block)
        input_data_path (str): Path to load input data
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    LSD_name = algo_name.split('_')[0]
    df_global_performance = pd.DataFrame(data=None)
    for cohort_name in cohorts_of_interest:
        df_labels = read_spark_file(
            input_data_path, f"{algo_name}_{cohort_name}_final_labels_{timerange}_per_{evaluation_level}"
        ).toPandas()
        df_labels = df_labels[[col for col in df_labels.columns if col != COL_UUID]]
        df_stats = df_labels.apply(lambda x: pd.value_counts(x, dropna=True, normalize=False))
        df_stats_pct = df_labels.apply(lambda x: pd.value_counts(x, dropna=True, normalize=True))
        df_stats_pct = df_stats_pct.rename(columns={col: f"{col}_pct" for col in df_stats_pct.columns})
        df_performance = pd.concat([df_stats, df_stats_pct], axis=1).reset_index()
        df_performance.insert(0, "n_people", len(df_labels))
        df_performance.insert(0, "cohort_type", f"{algo_name}_{cohort_name}")
        df_global_performance = pd.concat([df_global_performance, df_performance])
    df_global_performance = df_global_performance[df_global_performance.index == True].drop(columns=["index"])
    write_spark_file(df_global_performance, output_data_path, output_filename)

    print()
    if "cohort" in cohorts_of_interest:
        minimal_info_LSD = df_global_performance[
            df_global_performance.cohort_type == f"{algo_name}_cohort"
        ].minimal_global_availability_pct.values[0]
        recall_LSD = df_global_performance[df_global_performance.cohort_type == f"{algo_name}_cohort"].global_label_pct.values[0]
        print(
            f"Ratio of {algo_name} patients with enough data to go through the algorithm: {round(minimal_info_LSD, 3)}"
        )
        print(f"Ratio of {algo_name} patients flagged by the algorithm (when enough data): {round(recall_LSD, 3)}")
    if "control_cohort" in cohorts_of_interest:
        minimal_info_control = df_global_performance[
            df_global_performance.cohort_type == f"{algo_name}_control_cohort"
        ].minimal_global_availability_pct.values[0]
        recall_control = df_global_performance[
            df_global_performance.cohort_type == f"{algo_name}_control_cohort"
        ].global_label_pct.values[0]
        print(
            f"Ratio of control patients with enough data to go through the algorithm: {round(minimal_info_control, 3)}"
        )
        print(f"Ratio of control people flagged by the algorithm (when enough data): {round(recall_control, 3)}")
    if "cohort" in cohorts_of_interest and "control_cohort" in cohorts_of_interest:
        fp_rate = (1 / PREVALENCE_RATES[LSD_name]) * recall_control / recall_LSD
        print(
            f"Estimation of false positives for each true positive in real-world screening use-case: {round(fp_rate)}"
        )
    print()

# COMMAND ----------

