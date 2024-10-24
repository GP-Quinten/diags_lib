# Databricks notebook source
import os
from typing import Dict, List

import pyspark
from pyspark.sql import functions as F

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

def udf_algo_group_mapping(specific_features_config: Dict, mapping_col: str):
    """ Replaces '_' with ' ' in configuration file.

    Args:
        specific_features_config (Dict): Configuration file
        mapping_col (str): Column to map

    Returns:
        function: Returns a user defined function.
    """
    return F.udf(lambda x: specific_features_config[x][mapping_col].replace("_", " "))


def get_algorithm_group_info(
    df: pyspark.sql.DataFrame, db_config: Dict, specific_features_config: Dict
) -> pyspark.sql.DataFrame:
    """ Retrieves information about feature.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration information about feature
        specific_features_config (Dict): Configuration file

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with additionnal columns.
    """
    # retrieve feature name, feature group and tree group
    df = df.withColumn(
        COL_ALGO_MEDICAL_TERM,
        udf_algo_group_mapping(specific_features_config, "medical_term")(
            F.col(db_config["COL_INFO"])
        ),
    )
    df = df.withColumn(
        COL_ALGO_MEDICAL_GROUP,
        udf_algo_group_mapping(specific_features_config, "algorithm_category")(
            F.col(db_config["COL_INFO"])
        ),
    )
    df = df.withColumn(
        COL_ALGO_MEDICAL_BLOCK,
        udf_algo_group_mapping(specific_features_config, "algorithm_block")(
            F.col(db_config["COL_INFO"])
        ),
    )
    return df


def get_specific_features(
    algo_name: str,
    feature_name: str,
    input_data_path: str,
    input_filename: str,
    output_data_path: str,
    output_filename: str,
):
    """ Writes specific features matrix.

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        feature_name (str): Name of feature studied
        input_data_path (str): Path to input data
        input_filename (str): Name of input file
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    db_config = DATABASES_FEATURE_CONFIG[feature_name]
    features_config = FEATURES_CONFIG[feature_name][algo_name]
    df_broad_feature = read_spark_file(input_data_path, input_filename)
    df_specific_feature = df_broad_feature.filter(
        F.col(COL_FEATURE_TYPE) == SPECIFIC_FEATURE_TYPE
    )
    df_specific_feature = get_algorithm_group_info(
        df_specific_feature, db_config, features_config
    )
    write_spark_file(df_specific_feature, output_data_path, output_filename)
