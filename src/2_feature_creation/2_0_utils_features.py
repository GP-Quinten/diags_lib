# Databricks notebook source
import os

import yaml

# COMMAND ----------

def load_yaml_mapping_file(input_data_path, input_filename):
  """Load a yaml mapping file

  Args:
      input_data_path (str): Path to input data
      input_filename (str): Name of input data
      separator (str): Separator of the input data (stored as a csv)

  Returns:
      dict: Mapping dictionnary
  """
  with open(os.path.join(input_data_path, input_filename), 'r') as fp:
    mapping_file = yaml.safe_load(fp)
  return mapping_file