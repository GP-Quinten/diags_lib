# Databricks notebook source
from typing import Dict

# COMMAND ----------

import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# from config.variables import base

# COMMAND ----------

# MAGIC %run ../../config/variables/base

# COMMAND ----------

def process_patient_info(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Processes various patient information in the patient table from Optum

    Args:
        df (pyspark.sql.DataFrame): Patient table

    Returns:
        pyspark.sql.DataFrame: Processed patient table
    """
    ## Modify needed columns
    df = df.withColumn(
        COL_BIRTH_YEAR, F.substring(df.BIRTH_YR, 1, 4).cast(IntegerType())
    )
    df = df.withColumn(
        COL_FIRST_YEAR_ACTIVE,
        F.substring(F.col(COL_FIRST_MONTH_ACTIVE), 1, 4).cast(IntegerType()),
    )
    df = df.withColumn(
        COL_LAST_YEAR_ACTIVE,
        F.substring(F.col(COL_LAST_MONTH_ACTIVE), 1, 4).cast(IntegerType()),
    )

    ## Creates new columns to compute enrollment duration
    df = df.withColumn(
        COL_FIRST_DATE_ACTIVE,
        F.to_date(F.concat(F.col(COL_FIRST_MONTH_ACTIVE), F.lit("01")), "yyyyMMdd"),
    )
    df = df.withColumn(
        COL_LAST_DATE_ACTIVE,
        F.to_date(F.concat(F.col(COL_LAST_MONTH_ACTIVE), F.lit("01")), "yyyyMMdd"),
    )
    df = df.withColumn(
        COL_DATE_OF_DEATH,
        F.to_date(F.concat(F.col(COL_DATE_OF_DEATH), F.lit("01")), "yyyyMMdd"),
    )
    df = df.withColumn(
        COL_ENROLLMENT_DURATION,
        F.datediff(F.col(COL_LAST_DATE_ACTIVE), F.col(COL_FIRST_DATE_ACTIVE)),
    )

    ## Computes age at first activity
    df = df.withColumn(
        COL_AGE_AT_FIRST_ACTIVITY,
        (F.col(COL_FIRST_YEAR_ACTIVE) - F.col(COL_BIRTH_YEAR)),
    )
    ## Computes age at last activity
    df = df.withColumn(
        COL_AGE_AT_LAST_ACTIVE_DATE,
        (F.col(COL_LAST_YEAR_ACTIVE) - F.col(COL_BIRTH_YEAR)),
    )

    ## Drop useless columns
    cols_to_drop = (COL_FIRST_MONTH_ACTIVE, COL_LAST_MONTH_ACTIVE)
    df = df.drop(*cols_to_drop)
    return df


def get_idx_date_related_info(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Computes information related to the index date

    Args:
        df (pyspark.sql.DataFrame): DataFrame with cohort information

    Returns:
        pyspark.sql.DataFrame: DataFrame with added information about the index date
    """
    ## Compute columns relative to index date
    df = df.withColumn(
        COL_YEAR_INDEX_DATE,
        F.substring(F.col(COL_INDEX_DATE), 1, 4).cast(IntegerType()),
    )
    df = df.withColumn(
        COL_AGE_AT_INDEX_DATE, (F.col(COL_YEAR_INDEX_DATE) - F.col(COL_BIRTH_YEAR))
    )
    df = df.withColumn(
        COL_LOOKBACK_PERIOD,
        F.datediff(F.col(COL_INDEX_DATE), F.col(COL_FIRST_DATE_ACTIVE)),
    )
    return df


def get_patient_info(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Retrieves a Pyspark dataframe with several columns added:
          - FIRST_YEAR_ACTIVE
          - LAST_YEAR_ACTIVE
          - ENROLLMENT_DURATION
          - AGE_AT_FIRST_ACTIVITY
          - YEAR_INDEX_DATE
          - AGE_AT_INDEX_DATE
          - LOOKBACK PERIOD

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame with patient information
    """
    df = process_patient_info(df)
    df = get_idx_date_related_info(df)
    return df


def exclude_patients(
    df: pyspark.sql.DataFrame, df_to_exclude: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """Exclude patients being in the right dataset from the left dataset

    Args:
        df (pyspark.sql.DataFrame): DataFrame we need to cross check
        df_to_exclude (pyspark.sql.DataFrame): DataFrame with potential information to exclude from the other

    Returns:
        _type_: Filtered DataFrame
    """
    return df.join(df_to_exclude, on=COL_UUID, how="leftanti")


def get_patients_in_criteria(
    df: pyspark.sql.DataFrame,
    col_to_filter: str,
    config: Dict,
) -> pyspark.sql.DataFrame:
    """Filter the dataframe based on the exclusion criteria

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame informations with the patients
        col_to_filter (str): feature on which we want to filter
        config (Dict): filter configuration
    """
    col_type = config.get("type", None)
    min_value = config.get("min_value", None)
    max_value = config.get("max_value", None)
    bool_value = config.get("bool_value", None)
    if col_type == "bool":
        if bool_value is not None:
            return df.where(df[col_to_filter] == bool_value)
        return df
    if col_type == "float" or col_type == "int":
        if min_value is None and max_value is None:
            return df
        if min_value is None and max_value is not None:
            return df.where(df[col_to_filter] <= max_value)
        if min_value is not None and max_value is None:
            return df.where(df[col_to_filter] >= min_value)
        return df.where(
            (df[col_to_filter] >= min_value) & (df[col_to_filter] <= max_value)
        )
    raise ValueError(f"Column type unknown: {col_type}")


def filter_missing_info(
    df: pyspark.sql.DataFrame, colum_to_filter: str
) -> pyspark.sql.DataFrame:
    """Filters data with missing information

    Args:
        df (pyspark.sql.DataFrame): DataFrame to clean
        colum_to_filter (str): Columns to clean

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame
    """
    df = df.filter(F.col(colum_to_filter).isNotNull())
    df = df.filter(~F.col(colum_to_filter).isin(["Unknown", "unknown"]))
    return df


def filter_incorrect_dates(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Filters out incoherent dates

    Args:
        df (pyspark.sql.DataFrame): DataFrame to clean

    Returns:
        pyspark.sql.DataFrame: DataFrame with incoherent dates removed
    """
    return df.filter(
        (F.col(COL_BIRTH_YEAR) <= F.col(COL_FIRST_YEAR_ACTIVE))
        & (F.col(COL_FIRST_DATE_ACTIVE) <= F.col(COL_INDEX_DATE))
        & (F.col(COL_INDEX_DATE) <= F.col(COL_LAST_DATE_ACTIVE))
        & (
            (F.col(COL_DATE_OF_DEATH).isNull())
            | (F.col(COL_INDEX_DATE) <= F.col(COL_DATE_OF_DEATH))
        )
    )


def get_patients_in_quality_checks(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Computes quality checks

    Args:
        df (pyspark.sql.DataFrame): DataFrame to clean

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame
    """
    df = filter_missing_info(df, COL_AGE_AT_INDEX_DATE)
    df = filter_missing_info(df, COL_GENDER)
    df = filter_incorrect_dates(df)
    return df


# COMMAND ----------

def sample_table_approx(
    table: pyspark.sql.DataFrame, sample_ratio: float, seed: int = 123
) -> pyspark.sql.DataFrame:
    return table.sample(withReplacement=False, fraction=sample_ratio, seed=seed)


def sample_table_exact(
    table: pyspark.sql.DataFrame, k_sample: int, N_LSD: int = None, seed: int = 123
) -> pyspark.sql.DataFrame:
    """Samples part of the cohort

    Args:
        table (pyspark.sql.DataFrame): DataFrame to sample from
        N_LSD (int): Number of LSD types we want to sample for
        k_sample (int): Number of rows to sample
        seed (int, optional): Random seed. Defaults to 123.

    Returns:
        pyspark.sql.DataFrame : Sampled dataframe
    """
    if N_LSD is not None:
        k_sample = k_sample * N_LSD
    df = table.withColumn("outcome", F.lit(0))
    window = Window.partitionBy("outcome").orderBy(F.rand(seed=seed))
    df = (
        df.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") <= k_sample)
        .drop("row_number")
    )
    df = df.drop("outcome")
    return df