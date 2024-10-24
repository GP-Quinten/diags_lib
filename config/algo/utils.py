# Databricks notebook source
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# from utils.io_functions import loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

def get_code_values_config(data_path: str, name_file: str, separator: str) -> dict:
    """Gets the ICD10 code information in the configuration file.

    Args:
        data_path (str): Path to data with information
        name_file (str): Name of data
        separator (str): Separator of the csv file

    Returns:
        dict: Dictionnary with the ICD10 codes and the matching information
    """
    df = read_spark_file(
        data_path, name_file, file_type="csv", pandas_output=True, sep=separator
    )
    df = df.set_index("code")
    df["medical_term"] = df["medical_term"].apply(lambda x: x.lower())
    df["algorithm_category"] = df["algorithm_category"].apply(lambda x: x.lower())
    df["algorithm_block"] = df["algorithm_block"].apply(
        lambda x: f"ALGORITHM_BLOCK_{x}"
    )
    if "diagnosis_status" in df.columns:
        df["goal_status"] = df["diagnosis_status"].apply(lambda x: [x])
        conf = df[
            [
                "code_type",
                "medical_term",
                "algorithm_category",
                "algorithm_block",
                "goal_status",
            ]
        ].to_dict(orient="index")
    else:
        conf = df[
            ["code_type", "medical_term", "algorithm_category", "algorithm_block"]
        ].to_dict(orient="index")
    return conf


def get_numerical_values_config(data_path: str, name_file: str, separator: str) -> dict:
    """Gets the information on the numerical values requested via the configuration file.

    Args:
        data_path (str): Path to data with information
        name_file (str): Name of data
        separator (str): Separator of the csv file

    Returns:
        dict: Dictionnary with the numerical values (i.e. laboratory measures) and their matching information
    """
    df = read_spark_file(
        data_path, name_file, file_type="csv", pandas_output=True, sep=separator
    )
    df = df.replace("null", np.nan)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    df = df.fillna("null")
    df["medical_term"] = df["medical_term"].apply(lambda x: x.lower())
    df["algorithm_category"] = df["algorithm_category"].apply(lambda x: x.lower())
    df["algorithm_block"] = df["algorithm_block"].apply(
        lambda x: f"ALGORITHM_BLOCK_{x}"
    )
    df_grouped = df[
        [
            "test_name",
            "medical_term",
            "algorithm_category",
            "algorithm_block",
            "main_unit",
            "lower_limit",
            "upper_limit",
            "max_timespan",
            "min_values",
        ]
    ].drop_duplicates()
    conf = df_grouped.set_index("test_name").to_dict(orient="index")
    for lab in conf.keys():
        df_lab = df[df["test_name"] == lab][
            [
                "gender",
                "age_range_inf",
                "age_range_sup",
                "normal_range_inf",
                "normal_range_sup",
                "comparator",
                "threshold_value",
            ]
        ]
        df_lab["age_range"] = df_lab.apply(
            lambda x: [x["age_range_inf"], x["age_range_sup"]], axis=1
        )
        df_lab["normal_range"] = df_lab.apply(
            lambda x: [x["normal_range_inf"], x["normal_range_sup"]], axis=1
        )
        df_lab = df_lab.drop(
            columns=[
                "age_range_inf",
                "age_range_sup",
                "normal_range_inf",
                "normal_range_sup",
            ]
        )
        conf[lab]["thresholds"] = df_lab.to_dict("records")
    return conf


def get_algo_category_to_medical_term(input_config: dict) -> dict:
    """Matches the algorithm category to medical terms.

    Args:
        input_config (dict): Configuration file with the matching terms

    Returns:
        dict: Medical terms
    """
    output_config = {v["algorithm_category"]: set() for k, v in input_config.items()}
    for v in input_config.values():
        output_config[v["algorithm_category"]].add(f"({v['medical_term']})")
    return {k: " | ".join(v) for k, v in output_config.items()}


def get_global_config(*configs: dict) -> dict:
    """Creates a configuration file from several other configurations

    Returns:
        dict: Global configuration file
    """
    global_config = dict()
    for config in configs:
        global_config.update(config)
    return global_config


def get_global_algo_category_to_medical_term(*configs: dict) -> dict:
    """Matches all medical terms with their algorithm category

    Returns:
        dict: Medical terms
    """
    global_config = get_global_config(*configs)
    output_config = {k: set() for k in global_config.keys()}
    for k in output_config.keys():
        for config in configs:
            if config.get(k):
                output_config[k].add(config[k])
    return {k: " | ".join(v) for k, v in output_config.items()}


def get_medical_term_to_code(input_config: dict) -> dict:
    """Matches codes with their corresponding medical term

    Args:
        input_config (dict): Configuration containing the matching

    Returns:
        dict: Codes
    """
    output_config = {v["medical_term"]: set() for k, v in input_config.items()}
    for k, v in input_config.items():
        output_config[v["medical_term"]].add(f"({k})")
    return {k: " | ".join(v) for k, v in output_config.items()}
