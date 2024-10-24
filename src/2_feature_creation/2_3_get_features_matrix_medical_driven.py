# Databricks notebook source
import os
from typing import Dict

import numpy as np
import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_algo_conf

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../config/variables/base

# COMMAND ----------

def get_numerical_features_in_time_window(
    df: pyspark.sql.DataFrame, db_config: Dict, min_value: int, max_months: int
) -> pyspark.sql.DataFrame:
    """Check if there are N feature values in a timespan of M months for each patient in the dataframe
    and returns the associated infos.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file
        min_value (int): Number of minimal feature values to have in the time window
        max_months (int): Maximum length of the time window

    Returns:
        pyspark.sql.DataFrame: A dataframe with patients having at least min_value feature values in a timespan of maximum max_months months, only for the period closest to the INDEX_DATE.
    """

    df = df.withColumn(
        db_config["COL_DATE"],
        F.to_date(F.col(db_config["COL_DATE"]), db_config["DATE_FORMAT"]),
    )
    df = df.withColumn(
        COL_INDEX_DATE, F.to_date(F.col(COL_INDEX_DATE), db_config["DATE_FORMAT"])
    )

    # Cross join operation
    df_cross = df.alias("df1").join(
        df.alias("df2"),
        F.col(f"df1.{db_config['COL_UUID']}") == F.col(f"df2.{db_config['COL_UUID']}"),
    )

    # Calculate months difference and count the results in M month window
    df_cross = df_cross.withColumn(
        "MONTHS_DIFF",
        F.abs(
            F.months_between(
                F.col(f"df1.{db_config['COL_DATE']}"),
                F.col(f"df2.{db_config['COL_DATE']}"),
            )
        ),
    )
    df_cross = df_cross.filter(F.col("MONTHS_DIFF") <= max_months)

    # Aggregate results
    df_cross = df_cross.groupBy(
        f"df1.{db_config['COL_UUID']}",
        f"df1.{PATIENTS['COL_BIRTH_YEAR']}",
        f"df1.{PATIENTS['COL_GENDER']}",
        f"df1.{db_config['COL_INFO']}",
        f"df1.{COL_ALGO_MEDICAL_TERM}",
        f"df1.{COL_ALGO_MEDICAL_GROUP}",
        f"df1.{COL_ALGO_MEDICAL_BLOCK}",
        f"df1.{db_config['COL_UNIT']}",
        f"df1.{db_config['COL_DATE']}",
        f"df1.{db_config['COL_RESULT']}",
        f"df1.{COL_INDEX_DATE}",
    ).agg(
        F.min(f"df2.{db_config['COL_DATE']}").alias("WINDOW_START"),
        F.max(f"df2.{db_config['COL_DATE']}").alias("WINDOW_END"),
        F.count("*").alias("N_RESULTS_IN_WINDOW"),
        F.sort_array(
            F.collect_list(
                F.struct(
                    f"df2.{db_config['COL_DATE']}", f"df2.{db_config['COL_RESULT']}"
                )
            )
        ).alias("TESTS"),
    )

    # Retrieve values and dates of the features that are in the window of interest
    df_cross = df_cross.withColumn(
        f"{db_config['COL_RESULT']}S_IN_WINDOW",
        F.col(f"TESTS.{db_config['COL_RESULT']}"),
    )
    df_cross = df_cross.withColumn(
        f"{db_config['COL_DATE']}S_IN_WINDOW", F.col(f"TESTS.{db_config['COL_DATE']}")
    )
    df_cross = df_cross.drop("TESTS")

    # Filter patients with less than N results in M month window
    df_final = df_cross.filter(F.col("N_RESULTS_IN_WINDOW") >= min_value)

    # Find the difference between index date and feature date
    df_final = df_final.withColumn(
        "INDEX_DIFF",
        F.abs(F.months_between(F.col(db_config["COL_DATE"]), F.col(COL_INDEX_DATE))),
    )

    # Keep only the period closest to the INDEX_DATE for each patient
    df_final = df_final.withColumn(
        "MIN_DIFF", F.min("INDEX_DIFF").over(Window.partitionBy(db_config["COL_UUID"]))
    )
    df_final = df_final.filter(F.col("INDEX_DIFF") == F.col("MIN_DIFF")).drop(
        "MIN_DIFF", "INDEX_DIFF"
    )

    # Rename closest feature to index index date as reference
    df_final = df_final.withColumnRenamed(
        db_config["COL_DATE"], f"{db_config['COL_DATE']}_REF"
    )
    df_final = df_final.withColumnRenamed(
        db_config["COL_RESULT"], f"{db_config['COL_RESULT']}_REF"
    )

    return df_final


def combine_numerical_features(
    df: pyspark.sql.DataFrame,
    db_config: Dict,
    feature_conf: Dict,
    timerange: str,
) -> pyspark.sql.DataFrame:
    """Combines all numerical features in a specific time range.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file
        feature_conf (Dict): Configuration information sepcific to feature
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)

    Returns:
        pyspark.sql.DataFrame: Dataframe with all numerical features in a specific time window combined
    """
    if timerange in ["before_index_date", "just_before_index_date"]:
        df = df.filter(F.col(db_config["COL_DATE"]) < F.col(COL_INDEX_DATE))

    features = (
        df.select(F.col(db_config["COL_INFO"]))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    df_final_list = []

    for feature in features:
        # Filter dataframe by feature
        df_filtered = df.filter(F.col(db_config["COL_INFO"]) == feature)
        min_values = feature_conf[feature]["min_values"]
        max_timespan = feature_conf[feature]["max_timespan"]
        if timerange == "just_before_index_date":
            df_filtered = df_filtered.filter(
                F.abs(
                    F.months_between(
                        F.col(db_config["COL_DATE"]), F.col(COL_INDEX_DATE)
                    )
                )
                <= max_timespan
            )
        df_final = get_numerical_features_in_time_window(
            df_filtered, db_config, min_values, max_timespan
        )
        df_final_list.append(df_final)

    # Concatenate all dataframes
    df_result = df_final_list[0]
    for df_final in df_final_list[1:]:
        df_result = df_result.union(df_final)

    return df_result


def compute_labs_to_binary(
    df_features: pyspark.sql.DataFrame,
    df_cohort: pyspark.sql.DataFrame,
    db_config: Dict,
    feature_conf: Dict,
) -> pyspark.sql.DataFrame:
    """Computes laboratory measures to a binary matrix depending on the availability of values and the number of values requested by the algorithm.

    Args:
        df_features (pyspark.sql.DataFrame): Pyspark DataFrame with features
        df_cohort (pyspark.sql.DataFrame): Pyspark DataFrame with information about the cohort
        db_config (Dict): Configuration file
        feature_conf (Dict): Configuration information relative to the laboratory measures

    Returns:
        pyspark.sql.DataFrame: DataFrame with binary values for each laboratory measure.
    """
    features = (
        df_features.select(db_config["COL_INFO"])
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    for feature in features:
        # To avoid issues when calling columns
        medical_term_col = feature_conf[feature]["medical_term"]

        # Adding useful columns
        df_features = df_features.withColumn(
            "LAB_DATE_REF", F.to_date(F.col("LAB_DATE_REF"), db_config["DATE_FORMAT"])
        )
        df_features = df_features.withColumn(
            "TEST_YEAR", F.substring(F.col("LAB_DATE_REF"), 1, 4).cast(IntegerType())
        )
        df_features = df_features.withColumn(
            "AGE_AT_TEST", (F.col("TEST_YEAR") - F.col(PATIENTS["COL_BIRTH_YEAR"]))
        )
        df_features = df_features.withColumn(f"{medical_term_col}_binary", F.lit(None))
        df_features = df_features.withColumn(
            f"{medical_term_col}_outside_threshold", F.lit(None)
        )

        thresholds = feature_conf[feature]["thresholds"]
        for conditions in thresholds:
            # Retrieve parameters from config
            lower_bound = conditions["age_range"][0]
            if lower_bound == "null":
                lower_bound = 0
            upper_bound = conditions["age_range"][1]
            if upper_bound == "null":
                upper_bound = np.inf
            gender = conditions["gender"].lower()
            comparator = conditions["comparator"]
            value = conditions["threshold_value"]
            min_values = feature_conf[feature]["min_values"]
            # Define udf to count values outside threshold
            count_outside_threshold = udf(
                lambda values: sum(
                    1 for x in values if eval(str(x) + comparator + str(value))
                ),
                IntegerType(),
            )

            df_features = df_features.withColumn(
                f"{medical_term_col}_outside_threshold",
                F.when(
                    (
                        (F.col("AGE_AT_TEST").between(lower_bound, upper_bound))
                        & (F.lower(F.col(PATIENTS["COL_GENDER"])) == gender)
                        & (F.col(db_config["COL_INFO"]) == feature)
                    ),
                    count_outside_threshold("TEST_RESULTS_IN_WINDOW"),
                ).otherwise(F.col(f"{medical_term_col}_outside_threshold")),
            )

            # Turn values to binary: if not enough values available, value is False.
            # If enough values available, value is True.
            df_features = df_features.withColumn(
                f"{medical_term_col}_binary",
                F.when(
                    F.col(f"{medical_term_col}_outside_threshold").isNotNull(),
                    (F.col(f"{medical_term_col}_outside_threshold") >= min_values).cast(
                        IntegerType()
                    ),
                ).otherwise(F.col(f"{medical_term_col}_binary")),
            )

    binary_columns = [col for col in df_features.columns if col.endswith("binary")]
    selected_columns = [db_config["COL_UUID"]] + binary_columns
    df_features = df_features.select(*selected_columns)

    # Add cohort information
    df = df_cohort.select(db_config["COL_UUID"]).join(
        df_features, on=db_config["COL_UUID"], how="left"
    )
    custom_aggregation_udf = udf(lambda values: sum(values) if values else None)
    exprs = [
        custom_aggregation_udf(F.collect_list(col))
        .cast(BooleanType())
        .alias(col.rstrip("binary").rstrip("_"))
        for col in binary_columns
    ]
    df = df.groupBy(db_config["COL_UUID"]).agg(*exprs)
    return df


def combine_discrete_specific_features(
    df: pyspark.sql.DataFrame,
    df_cohort: pyspark.sql.DataFrame,
    db_config: Dict,
    feature_conf: Dict,
    max_timespan: str,
    timerange: str,
) -> pyspark.sql.DataFrame:
    """Combines discrete features for each patient in a given time span in a binary matrix.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame with discrete features. 1 row per UUID / FEATURE.
        db_config (Dict): Configuration file related to database
        feature_conf (Dict): Configuration information related to discrete feature
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)

    Returns:
        pyspark.sql.DataFrame: DataFrame with 1 column / FEATURE and 1 row per UUID.
    """
    if timerange in ["before_index_date", "just_before_index_date"]:
        df = df.filter(F.col(db_config["COL_DATE"]) <= F.col(COL_INDEX_DATE))
        if timerange == "just_before_index_date":
            max_diag_timespan = max_timespan
            df = df.filter(
                F.abs(
                    F.months_between(
                        F.col(db_config["COL_DATE"]), F.col(COL_INDEX_DATE)
                    )
                )
                <= max_diag_timespan
            )

    # Pivot on column of interest (eg for diags pivot on diag codes)
    df_pivot = df.groupBy(db_config["COL_UUID"]).pivot(db_config["COL_INFO"]).count()
    df_pivot = df_pivot.drop(F.col("null"))
    df_pivot = df_pivot.na.fill(value=0)

    # Add missing information
    features = [key for key, value in feature_conf.items()]
    missing_features = [diag for diag in features if diag not in df_pivot.columns]

    column_exprs = [F.lit(0).alias(col_name) for col_name in missing_features]
    df_pivot = df_pivot.select("*", *column_exprs)
    df = df_cohort.select(db_config["COL_UUID"]).join(
        df_pivot, on=db_config["COL_UUID"], how="left"
    )
    df = df.na.fill(value=0)
    df = df.withColumns(
        {
            col_name: F.col(col_name).cast(BooleanType())
            for col_name in df.columns
            if col_name != db_config["COL_UUID"]
        }
    )
    return df

def get_labs_feature_matrix(
    algo_name: str,
    timerange: str,
    df_labs: pyspark.sql.DataFrame,
    df_cohort: pyspark.sql.DataFrame,
    output_data_path: str,
    first_output_filename: str,
    second_output_filename: str,
) -> pyspark.sql.DataFrame:
    """Retrieves two DataFrames:
        - Combined laboratory values (used for statistics on availability)
        - Binary feature matrix (used to go through the algorithm)

    Args:
        algo_name (str): Name of LSD studied
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)
        df_labs (pyspark.sql.DataFrame): DataFrame with laboratory measures information
        df_cohort (pyspark.sql.DataFrame): DataFrame with information about the cohort studied
        output_data_path (str): Path to store output data
        first_output_filename (str): Name of first output (combined laboratory values)
        second_output_filename (str): Name of second output (binary feature matrix)

    Returns:
        pyspark.sql.DataFrame: Binary laboratory measures matrix
    """
    db_config = DATABASES_FEATURE_CONFIG[feature_name]
    features_config = FEATURES_CONFIG[feature_name][algo_name]
    labs_combined = combine_numerical_features(
        df_labs, db_config, features_config, timerange=timerange
    )
    write_spark_file(labs_combined, output_data_path, first_output_filename)

    labs_feature_matrix = compute_labs_to_binary(
        labs_combined, df_cohort, db_config, features_config
    )
    write_spark_file(labs_feature_matrix, output_data_path, second_output_filename)
    return labs_feature_matrix


def get_diags_feature_matrix(
    algo_name: str,
    timerange: str,
    max_timespan: str,
    df_diags: pyspark.sql.DataFrame,
    df_cohort: pyspark.sql.DataFrame,
    output_data_path: str,
    output_filename: str,
) -> pyspark.sql.DataFrame:
    """Retrieves diagnoses binary matrix.

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)
        max_timespan (str): Maximum time before index date to look at
        df_diags (pyspark.sql.DataFrame): DataFrame with information about diagnoses
        df_cohort (pyspark.sql.DataFrame): DataFrame with information about the cohort studied
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file

    Returns:
        pyspark.sql.DataFrame: Diagnoses binary matrix.
    """
    db_config = DATABASES_FEATURE_CONFIG[feature_name]
    features_config = FEATURES_CONFIG[feature_name][algo_name]
    diags_feature_matrix = combine_discrete_specific_features(
        df_diags, df_cohort, db_config, features_config, max_timespan, timerange=timerange
    )
    write_spark_file(diags_feature_matrix, output_data_path, output_filename)
    return diags_feature_matrix


def get_procedures_feature_matrix(
    algo_name: str,
    timerange: str,
    max_timespan: str,
    df_procedures: pyspark.sql.DataFrame,
    df_cohort: pyspark.sql.DataFrame,
    output_data_path: str,
    output_filename: str,
) -> pyspark.sql.DataFrame:
    """Retrieves procedures binary matrix.

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        timerange (str): Time range studied (lifetime, before_index_date, just_before_index_date)
        max_timespan (str): Maximum time before index date to look at
        df_procedures (pyspark.sql.DataFrame): DataFrame with information about procedures
        df_cohort (pyspark.sql.DataFrame): DataFrame with information about the cohort studied
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file

    Returns:
        pyspark.sql.DataFrame: Diagnoses binary matrix.
    """
    db_config = DATABASES_FEATURE_CONFIG[feature_name]
    features_config = FEATURES_CONFIG[feature_name][algo_name]
    procedures_feature_matrix = combine_discrete_specific_features(
        df_procedures, df_cohort, db_config, features_config, max_timespan, timerange=timerange
    )
    write_spark_file(procedures_feature_matrix, output_data_path, output_filename)
    return procedures_feature_matrix


def get_final_feature_matrix(
    df_cohort: pyspark.sql.DataFrame,
    labs_feature_matrix: pyspark.sql.DataFrame,
    diags_feature_matrix: pyspark.sql.DataFrame,
    procedures_feature_matrix: pyspark.sql.DataFrame,
    output_data_path: str,
    output_filename: str,
):
    """Combines all features matrix to create the final matrix to go through the algorithm.

    Args:
        df_cohort (pyspark.sql.DataFrame): DataFrame with information about the cohort studied
        labs_feature_matrix (pyspark.sql.DataFrame): Binary laboratory measures matrix
        diags_feature_matrix (pyspark.sql.DataFrame): Diagnoses binary matrix
        procedures_feature_matrix (pyspark.sql.DataFrame): Procedures binary matrix
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    final_feature_matrix = df_cohort.join(
        labs_feature_matrix.join(
            diags_feature_matrix.join(procedures_feature_matrix, on=PATIENT_UUID, how="left"), 
            on=PATIENT_UUID, 
            how="left"),
        on=PATIENT_UUID,
        how="left",
    )
    write_spark_file(final_feature_matrix, output_data_path, output_filename)
