# Databricks notebook source
import os

import pandas as pd

# COMMAND ----------

def read_spark_file(path: str, name_file: str, file_type: str ="parquet", pandas_output: bool =False, sep : str =",", **kwargs):
    """ Read spark dataframe 
    
    Args:
        path (str): Path to data to read
        name_file (str): Name of file to read
        filte_type (str): could be "parquet" or "csv"
        pandas_output (bool): whether to let the table in spark (False) or to convert into pandas DataFrame (True)
        
    Returns:
        df (spark dataframe or pandas DataFrame): Dataframe loaded
    """
    complete_path = os.path.join(path, name_file)
    print(f"Loading file from: {complete_path}")
    if file_type == "parquet":
        df = spark.read.parquet(complete_path, **kwargs)

    elif file_type =="csv":
        df = spark.read.option("delimiter", sep).option("header", True).csv(complete_path, **kwargs)

    if pandas_output:
        df = df.toPandas()

    return df

def file_existence(path:str) -> bool:
    """Check if a file or a directory exists

    Args:
        path (str): Path to check

    Returns:
        bool: Wether the file is there or not
    """
    try:
        dbutils.fs.ls(path)
        return True

    except:
        return False