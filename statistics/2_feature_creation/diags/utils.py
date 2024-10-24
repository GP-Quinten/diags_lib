# Databricks notebook source
import os

import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../config/variables/base

# COMMAND ----------

def retrieve_diag_table(
    algo_name: str,
    cohort: str,
    feature_type: str,
    patient_uuid: str,
    timerange: str = "before_index_date",
    flagged: bool = False,
    cohort_data_path: str = COHORT_DATA_PATH,
    feature_data_path: str = FEATURE_DATA_PATH,
) -> tuple:
    """
    Retrieves appropriate table of diagnosis of specific cohort

    Args:
        algo_name (str): Name of algorithm studied
        cohort (str): Type of cohort studied
        timerange (str): Default = "before_index_date". Can be "lifetime" or "just_before_index_date" as well
        feature_type (str): Features studied : can be SPECIFIC_FEATURE_TYPE (specific to the algorithm) or BROAD_FEATURE_TYPE (not in algo)
        patient_uuid (str): Unique descriptor of patients
        flagged (bool, optional): Default = False. If True, computes statistics on each flagged cohort
        all_flagged (bool, optional): Default = False. If flagged, if all_flagged, computes stats on all flagged patients
        cohort_data_path (str, optional): path to initial cohorts. Defaults to config value: COHORT_DATA_PATH.
        feature_data_path (str, optional): path to feature tables. Defaults to config value: FEATURE_DATA_PATH.

    Returns:
        pyspark.dataframe : df_features, table with the features of interest of the cohort of interest
        list : ptid_list, list of ptids of the cohort studied
    """

    if cohort=="all_cohorts":
        tmp_cohort = read_spark_file(
            os.path.join(cohort_data_path, algo_name),
            f"{algo_name}_all_flagged_cohorts_{timerange}",
        )
        ptid_list = tmp_cohort.select(patient_uuid).distinct().rdd.flatMap(lambda x: x).collect()
        cohort_list = tmp_cohort.select('COHORT_TYPE').distinct().rdd.flatMap(lambda x: x).collect()
        df_features = read_spark_file(
            os.path.join(feature_data_path, algo_name, "diags"),
            f"{algo_name}_{cohort_list[0]}_final_{feature_type}_diags",
        )
        for cohort_name in cohort_list[1:]:
            tmp_features = read_spark_file(
                os.path.join(feature_data_path, algo_name, "diags"),
                f"{algo_name}_{cohort_name}_final_{feature_type}_diags",
            )
            df_features = df_features.union(tmp_features)

        df_features = df_features.where(F.col(patient_uuid).isin(ptid_list))

    else: 
        df_features = read_spark_file(
            os.path.join(feature_data_path, algo_name, "diags"),
            f"{algo_name}_{cohort}_final_{feature_type}_diags",
        )
        if flagged:
            ptid_list = (
                read_spark_file(os.path.join(cohort_data_path, algo_name), f"{algo_name}_{cohort}_flagged_{timerange}")
                .select(patient_uuid)
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            df_features = df_features.where(F.col(patient_uuid).isin(ptid_list))
        else:
            ptid_list = (
                read_spark_file(os.path.join(cohort_data_path, algo_name), f"{algo_name}_{cohort}")
                .select(patient_uuid)
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect()
            )

    return df_features, ptid_list


def get_feature_importance(prevalence: float, lift: float) -> float:
    """
    Returns an indice of importance value ([0:1]). It representes how 'important' or 'useful' is a diagnosis. If lift is <1, importance is null. Else, if lift and prevalence are high, prevalence will be high and closer to 1. If a diagnosis has a high lift but a low prevalence, its importance value will be low.

    Args:
        prevalence (float): prevalence of diag
        lift (float): lift of diag

    Returns:
        float: importance indice between 0 and 1
    """
    # If lift and prevalences are high, importance is high and closer to 1.
    if lift >= 1:
        normalized_lift = 1 - 1 / lift
        return 2 * normalized_lift * prevalence / (normalized_lift + prevalence)
    # If lift is <1, importance is null
    return 0


def get_prevalence_and_lift(
    algo_name: str,
    main_cohort: str,
    other_cohorts: list,
    feature_type: str,
    cols_to_group_on: list,
    db_config: dict = DIAGNOSES,
    timerange: str = "before_index_date",
    flagged: bool = False,
    cohort_data_path: str = COHORT_DATA_PATH,
    feature_data_path: str = FEATURE_DATA_PATH,
) -> pd.DataFrame:
    """
    Returns a table with the prevalences, lifts and importances of the diagnosis of the cohorts of interest. There are different levels of precision for the diagnosis. Saves the final table. If a patient has 2 occurences of a diag, we only count one.

    Args:
        algo_name (str): Name of algorithm studied
        main_cohort (str): Type of cohort studied. Lifts will be computed as main_cohort/other_cohort. Can be 'cohort', 'like_cohort', 'control_cohort', 'all_cohorts'
        other_cohorts (list): Cohorts to compare to main_cohort. List of elements among 'like_cohort', 'control_cohort' or empty and no lif will be computed
        feature_type (str): Features studied : can be SPECIFIC_FEATURE_TYPE (specific to the algorithm) or BROAD_FEATURE_TYPE (not in algo)
        cols_to_group_on (str): list of columns to group on for the statistics. Can be COL_ICD_MEDICAL_TERM, COL_ICD_MEDICAL_GROUP, COL_ALGO_MEDICAL_TERM, COL_ALGO_MEDICAL_GROUP, COL_ALGO_MEDICAL_BLOCK (by descending precision). The first element of the list being the highest level of precision.
        db_config (dict, optional): Default = DIAGNOSES. Config dict containing the info related to our features
        timerange (str, optional): Default = "before_index_date". Can be "lifetime" or "just_before_index_date" as well
        flagged (bool, optional): Default = False. If True, computes statistics on each flagged cohort
        cohort_data_path (str, optional): path to initial cohorts. Defaults to config value: COHORT_DATA_PATH.
        feature_data_path (str, optional): path to feature tables. Defaults to config value: FEATURE_DATA_PATH.

    Returns:
        pd.Dataframe: returns a table with a row per diagnosis, with the prevalences, lifts and importances for studied cohorts.
    """
    max_lift = 1000  # Some lifts can be very high namely because we consider rare disease in RWD thus have very few patients or very low prevalences
    epsilon = 1e-10  # To avoid a division by 0
    patient_uuid = db_config["COL_UUID"]
    cols_of_grouping = cols_to_group_on.copy()
    level = cols_to_group_on[0].lower()
    cohorts = [main_cohort] + other_cohorts

    # Add feature_type in cols to group on to be able to distinguish them in the final table
    if feature_type == BROAD_FEATURE_TYPE and (len(cols_of_grouping) > 1):
        cols_of_grouping.append(COL_FEATURE_TYPE)
    df_stats = pd.DataFrame(data=None, columns=cols_of_grouping)

    for cohort in cohorts:
        df_features, ptid_list = retrieve_diag_table(
            algo_name,
            cohort,
            feature_type,
            patient_uuid,
            timerange,
            flagged,
            cohort_data_path,
            feature_data_path,

        )
        
        nb_ptids = len(ptid_list)

        # Keep only the occurences of diags before index date
        if timerange in ["before_index_date", "just_before_index_date"]:
            df_features = df_features.filter(F.col(db_config["COL_DATE"]) < F.col(COL_INDEX_DATE))
            if timerange == "just_before_index_date":
                max_diag_timespan = 24
                df_features = df_features.filter(
                    F.abs(F.months_between(F.col(db_config["COL_DATE"]), F.col(COL_INDEX_DATE))) <= max_diag_timespan
                )
        # Keep only 1 occurence of a diag
        df_features = df_features.dropDuplicates([patient_uuid] + cols_of_grouping)
        # Removing rows of diag that do not have the info on columns to group on
        for col_to_group_on in cols_of_grouping:
            df_features = df_features.filter(F.col(col_to_group_on).isNotNull())

        # Grouping on columns to group on and couting number of patients with each diag
        df_cohort_stats = df_features.groupby(cols_of_grouping).agg(
            F.count(patient_uuid).cast("int").alias(f"COUNT_{cohort.upper()}")
        )
        # Computing prevalence
        df_cohort_stats = df_cohort_stats.toPandas()
        df_cohort_stats[f"PCT_{cohort.upper()}"] = df_cohort_stats[f"COUNT_{cohort.upper()}"] / nb_ptids
        # df_cohort_stats.display()
        df_stats = df_stats.merge(df_cohort_stats, on=cols_of_grouping, how="outer")
    df_stats = df_stats.fillna(0)

    # Computing lifts and importances
    for other_cohort in other_cohorts:
        df_stats[f"LIFT_{main_cohort.upper()}_ON_{other_cohort.upper()}"] = (
            df_stats[f"PCT_{main_cohort.upper()}"]
            / df_stats[f"PCT_{other_cohort.upper()}"].apply(lambda x: max(x, epsilon))
        ).apply(lambda x: min(x, max_lift))

        df_stats[f"IMPORTANCE_{main_cohort.upper()}_ON_{other_cohort.upper()}"] = df_stats.apply(
            lambda x: get_feature_importance(
                x[f"PCT_{main_cohort.upper()}"], x[f"LIFT_{main_cohort.upper()}_ON_{other_cohort.upper()}"]
            ),
            axis=1,
        )

    df_stats = df_stats.sort_values(by=f"PCT_{main_cohort.upper()}", ascending=False)
    # df_stats.display()

    return df_stats


def get_diags_stats(
    algo_name: str,
    cohorts: list,
    feature_type: str,
    col_stats_level: str,
    db_config: dict = DIAGNOSES,
    timerange: str = "before_index_date",
    flagged: bool = False,
    cohort_data_path: str = COHORT_DATA_PATH,
    feature_data_path: str = FEATURE_DATA_PATH,
):
    """
    Returns a table of statistics on the total number of diagnosis per patient in the studied table of diagnosis : total count, percentiles, median. If a patient has 2 occruences of a diag, we only count as one.

    Args:
        algo_name (str): Name of algorithm studied
        cohorts (list): list of cohorts to compute stats on
        feature_type (str): Features studied : can be SPECIFIC_FEATURE_TYPE (specific to the algorithm) or BROAD_FEATURE_TYPE (not in algo)
        col_stats_level (str): level of precision/detail of diagnosis we want to aggregate stats on
        db_config (dict, optional): Default = DIAGNOSES. Config dict containing the info related to our features
        timerange (str, optional): Default = "before_index_date". Can be "lifetime" or "just_before_index_date" as well
        flagged (bool, optional): _description_. Defaults to False.
        cohort_data_path (str, optional): path to initial cohorts. Defaults to config value: COHORT_DATA_PATH.
        feature_data_path (str, optional): path to feature tables. Defaults to config value: FEATURE_DATA_PATH.

    Returns:
        pd.Dataframe: Returns a table with one row per cohort : and columns of number of patients having the diag among the cohort, and percentiles
    """
    patient_uuid = db_config["COL_UUID"]
    df_stats = pd.DataFrame()

    for cohort in cohorts:
        df_features, ptid_list = retrieve_diag_table(
            algo_name,
            cohort,
            feature_type,
            patient_uuid,
            timerange,
            flagged,
            cohort_data_path,
            feature_data_path,
        )
        nb_ptids = len(ptid_list)
        # Keep only the occurences of diags before index date
        if timerange in ["before_index_date", "just_before_index_date"]:
            df_features = df_features.filter(F.col(db_config["COL_DATE"]) < F.col(COL_INDEX_DATE))
            if timerange == "just_before_index_date":
                max_diag_timespan = 24  # to be added in the configuration
                df_features = df_features.filter(
                    F.abs(F.months_between(F.col(db_config["COL_DATE"]), F.col(COL_INDEX_DATE))) <= max_diag_timespan
                )
        # Keep only one occurence per patient per diag
        df_features = df_features.dropDuplicates([patient_uuid, col_stats_level])
        df_features = df_features.filter(F.col(col_stats_level).isNotNull())
        # Grouping by patient
        df_diags_stats = df_features.groupby(patient_uuid).agg(F.count(patient_uuid).cast("int").alias("COUNT_DIAGS"))
        # ptid_table = pd.DataFrame(ptid_list, columns=[patient_uuid])
        ptid_table = spark.createDataFrame(pd.DataFrame(ptid_list, columns=[patient_uuid]))
        df_diags_stats = ptid_table.join(df_diags_stats, on=patient_uuid, how="left")
        df_diags_stats = df_diags_stats.fillna(0)
        df_diags_stats = df_diags_stats.withColumn("COHORT_TYPE", F.lit(cohort))
        # Final grouping : 1 row per cohort
        df_diags_stats = df_diags_stats.groupby("COHORT_TYPE").agg(
            F.expr(f"percentile({'COUNT_DIAGS'}, 0.05)").alias("p05"),
            F.expr(f"percentile({'COUNT_DIAGS'}, 0.25)").alias("Q1"),
            F.expr(f"percentile({'COUNT_DIAGS'}, 0.5)").alias("MEDIAN"),
            F.expr(f"percentile({'COUNT_DIAGS'}, 0.75)").alias("Q3"),
            F.expr(f"percentile({'COUNT_DIAGS'}, 0.95)").alias("p95"),
        )
        df_diags_stats = df_diags_stats.toPandas()
        df_stats = pd.concat([df_stats, df_diags_stats], ignore_index=True)

    return df_stats
