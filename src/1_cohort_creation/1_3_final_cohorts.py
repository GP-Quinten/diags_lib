# Databricks notebook source
from typing import Dict, List

# COMMAND ----------

import pyspark
from pyspark.sql import functions as F

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from src.1_cohort_creation import 1_0_utils_cohort

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../1_cohort_creation/1_0_utils_cohort

# COMMAND ----------

def get_specific_LSD(
    df_generic_LSD: pyspark.sql.DataFrame, specific_LSD: str
) -> pyspark.sql.DataFrame:
    """Filter a dataframe on a specific type of LSD

    Args:
        df_generic_LSD (pyspark.sql.DataFrame): Dataframe with all LSD
        specific_LSD (str): LSD to filter on

    Returns:
        pyspark.sql.DataFrame: Dataframe filtered on a specific LSD
    """
    return df_generic_LSD.filter(F.col(COL_LSD_GROUP) == specific_LSD)


def get_certain_other_LSD(
    df_generic_LSD: pyspark.sql.DataFrame, specific_LSD: str
) -> pyspark.sql.DataFrame:
    """Retrieves all other LSD than the one of interest

    Args:
        df_generic_LSD (pyspark.sql.DataFrame): Dataframe with all LSD
        specific_LSD (str): LSD to filter out

    Returns:
        pyspark.sql.DataFrame: Dataframe with the specific LSD filtered out
    """
    return df_generic_LSD.filter(
        (F.col(COL_LSD_GROUP) != specific_LSD)
        & (F.col(COL_CERTAIN_LSD) == CERTAIN_FLAG)
    )


def apply_exclusion_filter(
    df: pyspark.sql.DataFrame, exclusion_config: Dict, get_len: bool = False,
) -> pyspark.sql.DataFrame:
    """Filters out patient not fitting criterias

    Args:
        df (pyspark.sql.DataFrame): DataFrame with patient information
        exclusion_config (Dict): Exclusion criterias
        get_len (bool): Retrieve lengths of dataframes. Defaults to False.

    Returns:
        pyspark.sql.DataFrame: DataFrame with patient not fitting criterias excluded
        if get_len: Retrieves a dict with the count of patients after each removal
    """
    # remove patients not meeting quality checks
    df_qc = get_patients_in_quality_checks(df)
    if get_len:
      length = {"Initial":df.select(COL_UUID).distinct().count(), 
                "Quality checks":df_qc.select(COL_UUID).distinct().count()}
    # remove patients meeting exclusion criteria
    for col_to_filter, config in exclusion_config.items():
        df_qc = get_patients_in_criteria(df_qc, col_to_filter, config)
        if get_len:
          length[col_to_filter] = df_qc.select(COL_UUID).distinct().count()
    if get_len:
      return length
    return df_qc


def get_LSD_cohort(
    df_specific_LSD: pyspark.sql.DataFrame,
    df_certain_other_LSD: pyspark.sql.DataFrame,
    patient_table: pyspark.sql.DataFrame,
    exclusion_config: Dict,
    get_len: bool = False,
) -> pyspark.sql.DataFrame:
    """Builds a cohort of certified patients for a given LSD

    Args:
        df_specific_LSD (pyspark.sql.DataFrame): DataFrame with patients having only a specific LSD
        df_certain_other_LSD (pyspark.sql.DataFrame): DataFrame with patients having any LSD but the specific one
        patient_table (pyspark.sql.DataFrame): DataFrame with original patients information
        exclusion_config (Dict): Exclusion criterias
        get_len (bool): Retrieve lengths of dataframes. Defaults to False.

    Returns:
        pyspark.sql.DataFrame: DataFrame with cohort of certified LSD patients and all their information
    """
    # retrieve LSD patients
    df_certain_specific_LSD = df_specific_LSD.filter(
        F.col(COL_CERTAIN_LSD) == CERTAIN_FLAG
    )
    df_LSD = exclude_patients(df_certain_specific_LSD, df_certain_other_LSD)
    # enrich with patient info
    df_LSD = df_LSD.join(patient_table[COL_TO_KEEP_PATIENTS], on=COL_UUID, how="left")
    df_LSD = get_patient_info(df_LSD)
    # apply exclusion filter
    df_LSD = apply_exclusion_filter(df_LSD, exclusion_config, get_len)
    return df_LSD


def get_LSD_like_cohort(
    df_specific_LSD: pyspark.sql.DataFrame,
    df_certain_other_LSD: pyspark.sql.DataFrame,
    patient_table: pyspark.sql.DataFrame,
    exclusion_config: Dict,
    get_len: bool = False,
) -> pyspark.sql.DataFrame:
    """Builds a cohort of uncertified patients for a given LSD

    Args:
        df_specific_LSD (pyspark.sql.DataFrame): DataFrame with patients having only a specific LSD
        df_certain_other_LSD (pyspark.sql.DataFrame): DataFrame with patients having any LSD but the specific one
        patient_table (pyspark.sql.DataFrame): DataFrame with original patients information
        exclusion_config (Dict): Exclusion criterias
        get_len (bool): Retrieve lengths of dataframes. Defaults to False.

    Returns:
        pyspark.sql.DataFrame: DataFrame with cohort of uncertified LSD patients and all their information
    """
    # retrieve LSD patients
    df_certain_specific_LSD = df_specific_LSD.filter(
        F.col(COL_CERTAIN_LSD) == CERTAIN_FLAG
    )
    df_LSD = exclude_patients(df_certain_specific_LSD, df_certain_other_LSD)
    # retrieve LSD-like patients
    df_LSD_like = exclude_patients(df_specific_LSD, df_LSD)
    # add uncertainty reason
    certain_other_lsd_patients = (
        df_certain_other_LSD.select(F.col(COL_UUID)).rdd.flatMap(lambda x: x).collect()
    )
    df_LSD_like = df_LSD_like.withColumn(
        COL_UNCERTAINTY_REASON,
        F.when(
            (F.col(COL_UUID).isin(certain_other_lsd_patients))
            & (F.col(COL_CERTAIN_LSD) == CERTAIN_FLAG),
            "other certain LSD diagnosis",
        ).otherwise("uncertain LSD diagnosis"),
    )
    # enrich with patient info
    df_LSD_like = df_LSD_like.join(
        patient_table[COL_TO_KEEP_PATIENTS], on=COL_UUID, how="left"
    )
    df_LSD_like = get_patient_info(df_LSD_like)
    # apply exclusion filter
    df_LSD_like = apply_exclusion_filter(df_LSD_like, exclusion_config, get_len)
    return df_LSD_like


def get_LSD_and_LSD_like_cohorts(
    LSD_name: str,
    config_age: dict,
    config_enrollment: dict,
    intermediate_data_path: str,
    cohort_data_path: str,
    intermediate_LSD_cohort_filename: str,
    output_LSD_filename: str,
    output_LSD_like_filename: str,
):
    """Builds and saves LSD and LSD-like cohorts.

    Args:
        LSD_name (str): Name of LSD studied
        config_age (dict): Configuration information about age
        config_enrollment (dict): Configuration information about enrollment duration
        intermediate_data_path (str): Path to intermediate data
        cohort_data_path (str): Path to cohort data
        intermediate_LSD_cohort_filename (str): Name of intermediate LSD cohort
        output_LSD_filename (str): Name of output file for LSD cohort
        output_LSD_like_filename (str): name of output file for LSD-like cohort
    """
    patients_table = read_spark_file(CONFIG_PATH_DATABASE, PATIENTS["TABLE_NAME"])
    intermediate_LSD_cohort = read_spark_file(
        intermediate_data_path, intermediate_LSD_cohort_filename
    )
    # inclusion
    df_specific_LSD = get_specific_LSD(intermediate_LSD_cohort, LSD_name)
    write_spark_file(
        df_specific_LSD,
        os.path.join(intermediate_data_path, LSD_name),
        f"{LSD_name}_potential_patients",
    )
    df_certain_other_LSD = get_certain_other_LSD(intermediate_LSD_cohort, LSD_name)
    # exclusion
    exclusion_config = {
        COL_AGE_AT_INDEX_DATE: config_age,
        COL_ENROLLMENT_DURATION: config_enrollment,
    }
    df_LSD_cohort = get_LSD_cohort(
        df_specific_LSD, df_certain_other_LSD, patients_table, exclusion_config
    )
    df_LSD_like_cohort = get_LSD_like_cohort(
        df_specific_LSD, df_certain_other_LSD, patients_table, exclusion_config
    )
    # write files
    write_spark_file(df_LSD_cohort, cohort_data_path, output_LSD_filename)
    write_spark_file(df_LSD_like_cohort, cohort_data_path, output_LSD_like_filename)


def get_LSD_control_cohort(
    config_age: dict,
    config_enrollment: dict,
    config_sampling: dict,
    intermediate_data_path: str,
    cohort_data_path: str,
    intermediate_control_cohort_filename: str,
    LSD_cohort_filename: str,
    output_filename: str,
):
    """Builds and saves a control cohort of which size depends on the number of patients in the LSD cohort and a given sampling ratio.

    Args:
        config_age (dict): Configuration information about age
        config_enrollment (dict): Configuration information about enrollment duration
        config_sampling (dict): Configuration information about sampling
        intermediate_data_path (str): Path to intermediate data
        cohort_data_path (str): Path to cohort data
        intermediate_control_cohort_filename (str): Name of intermediate control cohort
        LSD_cohort_filename (str): Name of LSD cohort 
        output_filename (str): Name of output file
    """
    control_cohort = read_spark_file(
        intermediate_data_path, intermediate_control_cohort_filename
    )
    LSD_cohort = read_spark_file(cohort_data_path, LSD_cohort_filename).select(COL_UUID)
    # remove patients not meeting quality checks
    control_cohort = get_patients_in_quality_checks(control_cohort)
    # remove patients meeting exclusion criteria
    exclusion_config = {
        COL_AGE_AT_INDEX_DATE: config_age,
        COL_ENROLLMENT_DURATION: config_enrollment,
    }
    for col_to_filter, config in exclusion_config.items():
        control_cohort = get_patients_in_criteria(control_cohort, col_to_filter, config)
    # final sampling
    control_cohort = sample_table_exact(
        control_cohort,
        k_sample=config_sampling,
        N_LSD=LSD_cohort.count(),
        seed=CONFIG_RANDOM_SEED,
    )
    # write file
    write_spark_file(control_cohort, cohort_data_path, output_filename)


# COMMAND ----------

