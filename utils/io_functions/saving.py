# Databricks notebook source
import os

import pandas as pd

# COMMAND ----------

def write_spark_file(df: pd.DataFrame, path: str, name_file: str, file_type: str ="parquet", partitions : int =1, overwrite: bool =True, input_df_pandas: bool =False):
    """ Write spark dataframe 
    
    Args:
        df (pd.DataFrame): Dataframe to save
        path (str): Path to save dataframe to
        name_file (str): Name to save file under
        file_type (str): can be "parquet" or "csv"
        partitions (int): number of partition for the file to save
        overwrite (bool): whether to overwrite a potential table with same name
        input_df_pandas (bool): if input df is a pandas dataframe to convert
    """
    complete_path = os.path.join(path, name_file)
    print('DataFrame will be saved in: {}'.format(complete_path))
    
    if input_df_pandas:
      #spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
      df = spark.createDataFrame(df)
    
    if isinstance(df, pd.DataFrame):
        print('transforming pandas dataframe to spark one')
        df = spark.createDataFrame(df)

    if file_type == "parquet":
        df.repartition(partitions).write.mode("overwrite").parquet(complete_path)

    elif file_type == "csv":
        df.write.option("header", True).mode("overwrite").csv(complete_path)
        
    elif file_type == 'json':
        df.write.mode("overwrite").json(complete_path)

# COMMAND ----------

