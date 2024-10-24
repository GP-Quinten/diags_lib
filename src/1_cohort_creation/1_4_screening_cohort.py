# Databricks notebook source
# from utils.io_functions import loading, saving
# from config.variables import base
# from src.1_cohort_creation import 1_0_utils_cohort, 1_2_intermediate_control_cohort

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../1_cohort_creation/1_0_utils_cohort

# COMMAND ----------

# MAGIC %run ../1_cohort_creation/1_2_intermediate_control_cohort

# COMMAND ----------

def get_LSD_screening_cohort(
    sample_ratio: int,
    config_age: dict,
    config_enrollment: dict,
    cohort_data_path: str,
    output_filename: str,
):
    """Builds and saves a screening cohort of which size depends on the sampling ratio using to randomly sample the database.

    Args:
        sample_ratio (float): ratio used for random sampling of the initial patients table
        config_age (dict): Configuration information about age
        config_enrollment (dict): Configuration information about enrollment duration
        cohort_data_path (str): Path to cohort data
        output_filename (str): Name of output file
    """
    patients_table = read_spark_file(CONFIG_PATH_DATABASE, PATIENTS["TABLE_NAME"])
    # The below line is optional and database dependent - allows to reduce computation time
    if sample_ratio is not None:
        screening_cohort = sample_table_approx(
            patients_table,
            sample_ratio=sample_ratio,
            seed=CONFIG_RANDOM_SEED,
        )
    # retrieve patient info for screening cohort
    screening_cohort = process_patient_info(screening_cohort)
    # assign index date as the last active date to include all events for the screening
    screening_cohort = add_index_date(screening_cohort, 1.0, 0.0, seed=42)
    # retrieve patient info related to index date for control cohort
    screening_cohort = get_idx_date_related_info(screening_cohort)
    # remove patients not meeting quality checks
    screening_cohort = get_patients_in_quality_checks(screening_cohort)
    # remove patients meeting exclusion criteria
    exclusion_config = {
        COL_AGE_AT_INDEX_DATE: config_age,
        COL_ENROLLMENT_DURATION: config_enrollment,
    }
    for col_to_filter, config in exclusion_config.items():
        screening_cohort = get_patients_in_criteria(screening_cohort, col_to_filter, config)
    # write file
    write_spark_file(screening_cohort, cohort_data_path, output_filename)

# COMMAND ----------

