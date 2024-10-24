# Databricks notebook source
from typing import Tuple

# COMMAND ----------

import pyspark
from pyspark.sql import Window
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

def get_idx_date_distribution(cohort: pyspark.sql.DataFrame) -> Tuple[float, float]:
    """Returns mean and stddev of the distribution of the ratio LOOKBACK_PERIOD / ENROLLMENT_DURATION of the
    dataframe.
    Args:
        cohort (pyspark.sql.DataFrame): Dataframe containing cohort information for a specific LSD

    Returns:
        Tuple: Mean and standard deviation of the ratio in the cohort
    """
    # compute ratio LP/ED
    COL_RATIO = "RATIO"
    cohort = cohort.withColumn(
        COL_RATIO, (F.col(COL_LOOKBACK_PERIOD) / F.col(COL_ENROLLMENT_DURATION))
    )
    # get mean and stddev of ratio column
    cohort_mean = cohort.select(F.mean(COL_RATIO)).collect()[0][0]
    cohort_stddev = cohort.select(F.stddev(COL_RATIO)).collect()[0][0]
    return cohort_mean, cohort_stddev


def add_index_date(
    cohort: pyspark.sql.DataFrame,
    ratio_mean: float,
    ratio_stddev: float,
    seed: int = 42,
) -> pyspark.sql.DataFrame:
    """
    Returns a pyspark dataframe with a new column added : INDEX_DATE. It is a fake INDEX_DATE for the control cohort, computed from real INDEX_DATE of LSD-cohort.
    It is computed as: index_date = first_active_date + enrollment_duration * ratio (--> N(ratio_mean, ratio_stddev))
    ratio follows a normal distribution  of mean = ratio_mean and stddev = ratio_stddev.
    It also clips the index date beetween FIRST_DATE_ACTIVE and LAST_DATE_ACTIVE

    Args :
        cohort (pyspark.sql.DataFrame): Pyspark DataFrame with enriched_control_cohort patient information
        ratio_mean (float): mean of ratio LOOKBACK_PERIOD / ENROLLMENT_DURATION of specific LSD-cohort
        ratio_stddev (float): stddev of ratio LOOKBACK_PERIOD / ENROLLMENT_DURATION of specific LSD-cohort
        seed (int): seed to apply on randomisation.

    Returns :
        pyspark.sql.DataFrame : Control cohort with a fake index date computed
    """
    # Add a new column called 'INDEX_DATE' to the DataFrame
    # index_date = first_active_date + enrollment_duration * ratio (--> N(ratio_mean, ratio_stddev))
    cohort_with_index_date = cohort.withColumn(
        COL_INDEX_DATE,
        F.date_add(
            F.col(COL_FIRST_DATE_ACTIVE),
            F.greatest(
                F.lit(0),
                F.least(
                    F.col(COL_ENROLLMENT_DURATION),
                    F.round(
                        F.col(COL_ENROLLMENT_DURATION)
                        * (ratio_mean + ratio_stddev * F.randn(seed)),
                        0,
                    ).cast("int"),
                ),
            ),
        ),
    )

    return cohort_with_index_date


def get_intermediate_control_cohort(
    sample_ratio: int,
    intermediate_LSD_cohort_filename: str,
    storage_data_path: str,
    output_filename: str,
):
    """Retrieves control patients excluding those having the LSD studied. Also does quality checks on the patients (removing null values, removing incoherent dates).

    Args:
        sample_ratio (int): Ratio determining the number of control patients per sick patient
        intermediate_LSD_cohort_filename (str): File name
        storage_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    patients_table = read_spark_file(CONFIG_PATH_DATABASE, PATIENTS["TABLE_NAME"])
    intermediate_LSD_cohort = read_spark_file(
        storage_data_path, intermediate_LSD_cohort_filename
    )
    intermediate_LSD_cohort = intermediate_LSD_cohort.join(
        patients_table[COL_TO_KEEP_PATIENTS], on=PATIENT_UUID, how="left"
    )
    # filter on certain LSD to later compute index date distribution
    intermediate_LSD_cohort = intermediate_LSD_cohort.filter(
        F.col(COL_CERTAIN_LSD) == True
    )
    # add patients info to later compute index date distribution
    intermediate_LSD_cohort = get_patient_info(intermediate_LSD_cohort)
    # Exclude all LSD and LSD-like patients from Patients table
    intermediate_control_cohort = exclude_patients(
        patients_table[COL_TO_KEEP_PATIENTS], intermediate_LSD_cohort
    )
    # The below line is optional and database dependent - allows to reduce computation time
    if sample_ratio is not None:
        intermediate_control_cohort = sample_table_approx(
            intermediate_control_cohort,
            sample_ratio=sample_ratio,
            seed=CONFIG_RANDOM_SEED,
        )
    # retrieve patient info for control cohort
    intermediate_control_cohort = process_patient_info(intermediate_control_cohort)
    # retrieve mean and standard deviation of look-back period over observation period of main_LSD_cohort
    LSD_ratio_mean, LSD_ratio_stddev = get_idx_date_distribution(
        intermediate_LSD_cohort
    )
    # use mean and stddev to assign index date in control cohort
    intermediate_control_cohort = add_index_date(
        intermediate_control_cohort, LSD_ratio_mean, LSD_ratio_stddev, seed=42
    )
    # retrieve patient info related to index date for control cohort
    intermediate_control_cohort = get_idx_date_related_info(intermediate_control_cohort)
    write_spark_file(intermediate_control_cohort, storage_data_path, output_filename)


# COMMAND ----------

