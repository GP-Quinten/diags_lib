# Databricks notebook source
# this script must only be run to generate the yaml configuration
# if the yaml configuration files already exist no need to run it

# COMMAND ----------

import os

import yaml

# COMMAND ----------

# from utils.io_functions import loading
# from config.ICD10 import base

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ./base

# COMMAND ----------

def get_ICD9_to_ICD10_mapping(
    input_data_path: str,
    input_filename: str,
    separator: str,
    diags_mapping_approximation: bool,
) -> dict:
    """Maps ICD10 codes to ICD9

    Args:
        input_data_path (str): Path to input data
        input_filename (str): Name of input data
        separator (str): Separator of the input data (stored as a csv)
        diags_mapping_approximation (bool): Wether or not to approximate during matching

    Returns:
        dict: Dictionnary with the matched codes
    """
    ICD10_to_ICD9 = read_spark_file(
        input_data_path,
        input_filename,
        file_type="csv",
        pandas_output=True,
        sep=separator,
    )
    ICD10_to_ICD9["ICD10"] = ICD10_to_ICD9["ICD10"].apply(lambda x: str(x).strip())
    ICD10_to_ICD9["ICD9"] = ICD10_to_ICD9["ICD9"].apply(lambda x: str(x).strip())
    if not diags_mapping_approximation:
        ICD10_to_ICD9 = ICD10_to_ICD9[ICD10_to_ICD9["approximate"] == "0"]
    return dict(zip(ICD10_to_ICD9["ICD9"], ICD10_to_ICD9["ICD10"]))


def get_ICD10_to_name_mapping(
    input_data_path: str, input_filename: str, separator: str
) -> dict:
    """Gather names of mapped ICD10 codes

    Args:
        input_data_path (str): Path to input data
        input_filename (str): Name of input data
        separator (str): Separator of the input data (stored as a csv)

    Returns:
        dict: Dictionnary with names matching the mapped codes
    """
    ICD10_to_name = read_spark_file(
        input_data_path,
        input_filename,
        file_type="csv",
        pandas_output=True,
        sep=separator,
    )
    ICD10_to_name["ICD10"] = ICD10_to_name["ICD10"].apply(lambda x: str(x).strip())
    ICD10_to_name["description"] = ICD10_to_name["description"].apply(
        lambda x: x.strip()
    )
    return dict(zip(ICD10_to_name["ICD10"], ICD10_to_name["description"]))

# COMMAND ----------

input_filename = "2018_ICD_general_equivalence_mapping.csv"
output_filename = ICD9_TO_ICD10_MAPPING_FILE
with open(os.path.join(ICD_CONF_PATH, output_filename), 'w') as fp:
  ICD9_to_ICD10_mapping = get_ICD9_to_ICD10_mapping(
    input_data_path=os.path.join(CONFIG_DATA_PATH, "ICD10"), 
    input_filename=input_filename, 
    separator=";",
    diags_mapping_approximation=False
    )
  yaml.safe_dump(ICD9_to_ICD10_mapping, fp)

# COMMAND ----------

input_filename = "icd10cm_order_2024.csv"
output_filename = ICD10_TO_DESCRIPTION_MAPPING_FILE
with open(os.path.join(ICD_CONF_PATH, output_filename), 'w') as fp:
  ICD10_to_name_mapping = get_ICD10_to_name_mapping(
    input_data_path=os.path.join(CONFIG_DATA_PATH, "ICD10"), 
    input_filename=input_filename, 
    separator=";",
    )
  yaml.safe_dump(ICD10_to_name_mapping, fp)