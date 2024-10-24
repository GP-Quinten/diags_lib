# Databricks notebook source
from typing import Dict, List

# COMMAND ----------

import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_LSD_conf

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../config/variables/base

# COMMAND ----------

# MAGIC %run ../../config/algo/global_LSD_conf

# COMMAND ----------

reverse_mapping_udf = F.udf(lambda x: CODE_TO_LSD.get(x, CODE_TO_LSD[x[:9]]))

# COMMAND ----------

def filter_table_from_codes(
    table: pyspark.sql.DataFrame,
    code_dict: Dict,
    filter_column: str,
    columns_to_keep: List,
    method: str,
) -> pyspark.sql.DataFrame:
    """Filter a pyspark DataFrame on codes

    Args:
        table (pyspark.sql.DataFrame): Table to filter
        code_dict (Dict): Codes to filter on
        filter_column (str): Column to filter on
        columns_to_keep (List): Columns to keep in table to filter
        method (str): Can be "equals" or "startswith" depending on how the filter is supposed to work

    Returns:
        pyspark.sql.DataFrame: Filtered pyspark DataFrame
    """
    df = table[columns_to_keep]
    # Create the filter condition
    # Initialize to a False condition since we have OR filter
    filter_condition = F.col(filter_column) != F.col(filter_column)
    for _, codes in code_dict.items():
        if method == "equals":
            filter_condition = filter_condition | F.col(filter_column).isin(codes)
        if method == "startswith":
            for code in codes:
                filter_condition = filter_condition | F.col(filter_column).startswith(
                    code
                )
    # Filter the DataFrame based on the codes
    return df.filter(filter_condition)


def get_LSD_diagnosis(diag_table: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Retrieves certain and uncertain LSD diagnoses

    Args:
        diag_table (pyspark.sql.DataFrame): Pyspark DataFrame containing the diagnoses and their caracteristics

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with diagnoses flagged as certain or uncertain. The most frequent diagnosis is retrieved each time
    """
    diag_table = diag_table.withColumn(
        COL_DIAG_DATE, F.to_date(F.col(COL_DIAG_DATE), DIAG_DATE_FORMAT)
    )

    # retrieve certain diagnoses
    # only keep diagnoses with goal status
    certain_diag_table = diag_table.filter(F.col(COL_DIAGNOSIS_STATUS).isin(LSD_GOAL_STATUSES))
    # only keep diagnoses codes from different dates for a patient
    certain_diag_table = certain_diag_table.dropDuplicates(
        subset=[COL_UUID, COL_DIAG_CODE, COL_DIAG_DATE]
    )
    # retrieve number of certain diagnoses by patient and code
    df_certain_diag_count = certain_diag_table.groupBy(COL_UUID, COL_DIAG_CODE).agg(
        F.count(COL_UUID).alias(COL_N_CERTAIN_DIAGS),
        F.min(COL_DIAG_DATE).alias(
            f"{PREFIX_FIRST_DATE}_{PREFIX_CERTAIN_DIAG}_{COL_DIAG_DATE}"
        ),
    )
    # retrieve LSD name based on diagnosis code
    df_certain_diag_count = df_certain_diag_count.withColumn(
        COL_LSD_GROUP, reverse_mapping_udf(F.col(COL_DIAG_CODE))
    )
    # only keep LSD with the most certain diagnoses by LSD group
    window = Window.partitionBy(COL_UUID, COL_LSD_GROUP).orderBy(
        F.desc(COL_N_CERTAIN_DIAGS)
    )
    df_certain = (
        df_certain_diag_count.withColumn("row", F.row_number().over(window))
        .filter(F.col("row") == 1)
        .drop("row")
    )

    # retrieve uncertain diagnoses
    # only keep diagnoses without goal status
    uncertain_diag_table = diag_table.filter(~F.col(COL_DIAGNOSIS_STATUS).isin(LSD_GOAL_STATUSES))
    # only keep diagnoses codes from different dates for a patient
    uncertain_diag_table = uncertain_diag_table.dropDuplicates(
        subset=[COL_UUID, COL_DIAG_CODE, COL_DIAG_DATE]
    )
    # retrieve number of uncertain diagnoses by patient and code
    df_uncertain_diag_count = uncertain_diag_table.groupBy(COL_UUID, COL_DIAG_CODE).agg(
        F.count(COL_UUID).alias(COL_N_UNCERTAIN_DIAGS),
        F.min(COL_DIAG_DATE).alias(
            f"{PREFIX_FIRST_DATE}_{PREFIX_UNCERTAIN_DIAG}_{COL_DIAG_DATE}"
        ),
    )
    # retrieve LSD name based on diagnosis code
    df_uncertain_diag_count = df_uncertain_diag_count.withColumn(
        COL_LSD_GROUP, reverse_mapping_udf(F.col(COL_DIAG_CODE))
    )
    # only keep LSD with the most uncertain diagnoses by LSD group
    window = Window.partitionBy(COL_UUID, COL_LSD_GROUP).orderBy(
        F.desc(COL_N_UNCERTAIN_DIAGS)
    )
    df_uncertain = (
        df_uncertain_diag_count.withColumn("row", F.row_number().over(window))
        .filter(F.col("row") == 1)
        .drop("row")
    )

    # join the two dataframes
    df = df_certain.join(df_uncertain, on=[COL_UUID, COL_LSD_GROUP], how="outer")
    # fill null values with zero
    df = df.fillna(0, subset=[COL_N_CERTAIN_DIAGS, COL_N_UNCERTAIN_DIAGS])
    return df


def get_LSD_treatment(treatment_table: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Retrieves patients treated for a LSD

    Args:
        treatment_table (pyspark.sql.DataFrame): Pyspark DataFrame containing the treatments and their caracteristics

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with the patients treated for an LSD. The most frequent treatment is retrieved
    """
    treatment_table = treatment_table.withColumn(
        COL_TREATMENT_DATE, F.to_date(F.col(COL_TREATMENT_DATE), TREATMENT_DATE_FORMAT)
    )
    # only keep treatment codes from different dates for a patient
    treatment_table = treatment_table.dropDuplicates(
        subset=[COL_UUID, COL_TREATMENT_CODE, COL_TREATMENT_DATE]
    )
    # retrieve number of treatment by patient and code
    df_treatment_count = treatment_table.groupBy(COL_UUID, COL_TREATMENT_CODE).agg(
        F.count(COL_UUID).alias(COL_N_TREATMENTS),
        F.min(COL_TREATMENT_DATE).alias(f"{PREFIX_FIRST_DATE}_{COL_TREATMENT_DATE}"),
    )
    # retrieve LSD name based on treatment code
    df_treatment_count = df_treatment_count.withColumn(
        COL_LSD_GROUP, reverse_mapping_udf(F.col(COL_TREATMENT_CODE))
    )
    # only keep LSD with the most prescriptions by LSD group
    window = Window.partitionBy(COL_UUID, COL_LSD_GROUP).orderBy(
        F.desc(COL_N_TREATMENTS)
    )
    df = (
        df_treatment_count.withColumn("row", F.row_number().over(window))
        .filter(F.col("row") == 1)
        .drop("row")
    )
    return df


def get_LSD_index_date(df_LSD: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Retrieves the index date for each patient

    Args:
        df_LSD (pyspark.sql.DataFrame): Pyspark DataFrame containing the diagnosis and treatment information about each patient. Diagnosis information is prioritized

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with the index date for each patient
    """
    df_LSD = df_LSD.withColumn(
        f"{PREFIX_CERTAIN_DIAG}_{COL_INDEX_DATE}",
        F.when(
            F.col(
                f"{PREFIX_FIRST_DATE}_{PREFIX_CERTAIN_DIAG}_{COL_DIAG_DATE}"
            ).isNull(),
            F.col(f"{PREFIX_FIRST_DATE}_{COL_TREATMENT_DATE}"),
        ).otherwise(
            F.col(f"{PREFIX_FIRST_DATE}_{PREFIX_CERTAIN_DIAG}_{COL_DIAG_DATE}")
        ),
    )
    df_LSD = df_LSD.withColumn(
        f"{PREFIX_UNCERTAIN_DIAG}_{COL_INDEX_DATE}",
        F.col(f"{PREFIX_FIRST_DATE}_{PREFIX_UNCERTAIN_DIAG}_{COL_DIAG_DATE}"),
    )
    df_LSD = df_LSD.withColumn(
        COL_INDEX_DATE,
        F.when(
            F.col(f"{PREFIX_CERTAIN_DIAG}_{COL_INDEX_DATE}").isNull(),
            F.col(f"{PREFIX_UNCERTAIN_DIAG}_{COL_INDEX_DATE}"),
        ).otherwise(F.col(f"{PREFIX_CERTAIN_DIAG}_{COL_INDEX_DATE}")),
    )
    return df_LSD


def get_LSD_patients(
    df_diag: pyspark.sql.DataFrame,
    df_treatment: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """Retrieves a dataframe with all information about patients having any LSD

    Args:
        df_diag (pyspark.sql.DataFrame): Dataframe with information about diagnoses
        df_treatment (pyspark.sql.DataFrame): Dataframe with information about treatments

    Returns:
        pyspark.sql.DataFrame: Dataframe with information about each patient having an LSD. One patient can have several rows
    """
    # join diagnosis and treatment info
    df = df_diag.join(df_treatment, on=[COL_UUID, COL_LSD_GROUP], how="outer")
    # fill null values with zeros
    df = df.fillna(
        0, subset=[COL_N_CERTAIN_DIAGS, COL_N_UNCERTAIN_DIAGS, COL_N_TREATMENTS]
    )
    # assign index date based on first diagnosis and treatement date
    df = get_LSD_index_date(df)
    return df.select(
        F.col(COL_UUID),
        F.col(COL_LSD_GROUP),
        F.col(COL_INDEX_DATE),
        F.col(COL_N_CERTAIN_DIAGS),
        F.col(COL_N_UNCERTAIN_DIAGS),
        F.col(COL_N_TREATMENTS),
    )


def flag_certain_LSD_patients(
    df_LSD: pyspark.sql.DataFrame, nb_certain_diag: int, nb_treatment: int
) -> pyspark.sql.DataFrame:
    """Flag patients with certain LSD based on number of certain diagnoses or treatments

    Args:
        df_LSD (pyspark.sql.DataFrame): Dataframe with information about each patient having an LSD
        nb_certain_diag (int): Minimal number of diagnoses required to have a certain diagnosis
        nb_treatment (int): Minimal number of treatments required to have a certain diagnosis

    Returns:
        pyspark.sql.DataFrame: Dataframe with certain LSD patients flagged.
    """
    df_LSD = df_LSD.withColumn(
        COL_CERTAIN_LSD,
        F.when(
            (F.col(COL_N_CERTAIN_DIAGS) >= nb_certain_diag)
            | (F.col(COL_N_TREATMENTS) >= nb_treatment),
            CERTAIN_FLAG,
        ).otherwise(not CERTAIN_FLAG),
    )
    return df_LSD


def get_intermediate_LSD_cohort(
    diagnoses_codes: dict, treatment_codes: dict, storage_data_path: str, output_filename: str
):
    """ Retrieves patients related to an LSD through diagnosis or treatment. 

    Args:
        diagnoses_codes (dict): Diagnoses code list
        treatment_codes (dict): Treatments code list
        storage_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    # Load tables of interest
    diagnoses_table = read_spark_file(CONFIG_PATH_DATABASE, DIAGNOSES["TABLE_NAME"])
    treatments_table = read_spark_file(CONFIG_PATH_DATABASE, TREATMENTS["TABLE_NAME"])

    # Get LSD diagnoses
    LSD_diags = filter_table_from_codes(
        diagnoses_table,
        diagnoses_codes,
        DIAGNOSES["COL_INFO"],
        COL_TO_KEEP_DIAGNOSES,
        "equals",
    )
    # Get LSD treatments
    LSD_treatments = filter_table_from_codes(
        treatments_table,
        treatment_codes,
        TREATMENTS["COL_INFO"],
        COL_TO_KEEP_TREATMENTS,
        "startswith",
    )

    # Get patients with LSD diagnosis
    LSD_patient_diag = get_LSD_diagnosis(LSD_diags)
    # Get patients with LSD treatment
    LSD_patients_treament = get_LSD_treatment(LSD_treatments)

    # Get patients with LSD diag and/or LSD treatment
    LSD_patients = get_LSD_patients(LSD_patient_diag, LSD_patients_treament)

    # flag patients with certain or uncertain LSD
    flagged_LSD_patients = flag_certain_LSD_patients(
        LSD_patients, NB_CERTAIN_DIAGS, NB_TREATMENTS
    )

    # write data
    write_spark_file(flagged_LSD_patients, storage_data_path, output_filename)


# COMMAND ----------

