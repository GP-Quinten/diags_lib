# Databricks notebook source
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

import os
from typing import Dict, List

import numpy as np
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_algo_conf
# from src.2_feature_creation import 2_0_utils_features

# COMMAND ----------

# MAGIC %run ./2_0_utils_features

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../config/variables/base

# COMMAND ----------

def remove_null_values(
    df: pyspark.sql.DataFrame, col_info: str
) -> pyspark.sql.DataFrame:
    """Removes null numerical values and unknown categorical values.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame to clean
        col_info (str): Column to clean

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with no null values in col_info.
    """
    df = df.filter(~(F.col(col_info).isNull()))
    df = df.filter(~F.col(col_info).isin(["Unknown", "unknown"]))
    return df


def set_values_type(
    df: pyspark.sql.DataFrame, col_info: str, col_type: pyspark.sql.types
) -> pyspark.sql.DataFrame:
    """Changes type of a column.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_info (str): Column to change type of
        col_type (pyspark.sql.types): New column type

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with a changed column type.
    """
    df = df.withColumn(col_info, F.col(col_info).cast(col_type))
    return df


def set_col_date(
    df: pyspark.sql.DataFrame, col_final_date: str, col_dates: List
) -> pyspark.sql.DataFrame:
    """If several types of dates available for one event, retrieves the most prioritized one when available.
    
    Example of use: in Optum DB in labs table there are 3 dates columns for 1 lab measure (["COLLECTED_DATE", "RESULT_DATE", "ORDER_DATE"]) that are not always filled. We want to retrieve only 1 date with the following order: ["COLLECTED_DATE", "RESULT_DATE", "ORDER_DATE"].

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_final_date (str): New column to put the date in
        col_dates (List): Columns names of possible dates, ordered from the most prioritized to the least

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame with an added column of dates.
    """
    if len(col_dates) > 1:
        if col_final_date not in df.columns:
            df = df.withColumn(col_final_date, F.lit(None).cast(StringType()))
        for col_date in col_dates:
            df = df.withColumn(
                col_final_date,
                F.when(
                    F.col(col_final_date).isNotNull(), F.col(col_final_date)
                ).otherwise(F.col(col_date)),
            )
        return df
    return df


def set_date_format(
    df: pyspark.sql.DataFrame, col_date: str, date_format: str
) -> pyspark.sql.DataFrame:
    """Changes dates to a specific format.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_date (str): Column with date to change format of
        date_format (str): Target format for the date

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame
    """
    df = df.withColumn(col_date, F.to_date(F.col(col_date), date_format))
    return df


def preprocess_features(
    df: pyspark.sql.DataFrame, db_config: Dict
) -> pyspark.sql.DataFrame:
    """Preprocesses a given table feature. Removes null values, sets columns with dates on one specific format, selects columns to keep in order to have a clean and ready to use dataframe.

    Principle of use: give a feature table (diagnoses, or labs, or other type of feature table) and a previously defined configuration of this table (look into optum config file for an example)-> preprocess the table.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file for the given feature

    Returns:
        pyspark.sql.df: Preprocessed features dataframe
    """
    df = set_col_date(df, db_config["COL_DATE"], db_config["DATE_PRIORITY"])
    df = set_date_format(df, db_config["COL_DATE"], db_config["DATE_FORMAT"])
    df = remove_null_values(df, db_config["COL_INFO"])
    df = remove_null_values(df, db_config["COL_DATE"])
    if db_config.get("COL_RESULT", None):
        df = set_values_type(df, db_config["COL_RESULT"], DoubleType())
        # if no date for the feature, not kept
        df = remove_null_values(df, db_config["COL_RESULT"])
    if db_config.get("COL_UNIT", None):
        # if no unit for feature but needed, then not kept
        df = remove_null_values(df, db_config["COL_UNIT"])
    # Keep only columns of interest defined in config
    col_to_keep = [value for key, value in db_config.items() if key.startswith("COL_")]
    col_to_keep = [col for col in col_to_keep if col in df.columns]
    df = df.select(col_to_keep)
    return df


def get_main_unit(
    df: pyspark.sql.DataFrame,
    db_config: Dict,
    specific_features_config: Dict,
    generic_feature_to_unit_config: Dict,
) -> pyspark.sql.DataFrame:
    """ Retrieves numerical values that are in a given unit and annotates if the feature is used in the algorithm or not.

    Principle of use: compare values that are of the same unit so only the values that are in a given unit are retrieved. In the optum databse, it is related to the labs table and the observations table.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file 
        specific_features_config (Dict): Configuration file specific to feature
        generic_feature_to_unit_config (Dict): Map with generic features units

    Returns:
        pyspark.sql.DataFrame: DataFrame with cleaned numerical values and units for a feature
    """
    # Retrieve all the labs available in the cohort/table
    labs_of_interest = (
        df.select(F.col(db_config["COL_INFO"]))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Creat a dict with a 'lab' and the 'main_unit' associated
    specific_feature_to_unit_config = {
        key: value["main_unit"]
        for key, value in specific_features_config.items()
        if key in labs_of_interest
    }

    # check if feature is integrated in the configuration
    def determine_feature_type(info, info_unit):
        for feature, unit in specific_feature_to_unit_config.items():
            if info == feature and info_unit == unit:
                return SPECIFIC_FEATURE_TYPE
        return BROAD_FEATURE_TYPE
    # Convert the Python function to a PySpark UDF
    feature_type_udf = F.udf(determine_feature_type, StringType())
    # Apply the UDF to the DataFrame
    df = df.withColumn(COL_FEATURE_TYPE, feature_type_udf(db_config["COL_INFO"], db_config["COL_UNIT"]))

    # check if feature has main unit
    def check_feature_conditions(feature_type, info, unit):
        if feature_type == SPECIFIC_FEATURE_TYPE:
            return 1
        if feature_type == BROAD_FEATURE_TYPE and (info, unit) in generic_feature_to_unit_config.items():
            return 1
        return 0

    # Convert the Python function to a PySpark UDF
    check_feature_udf = F.udf(check_feature_conditions, IntegerType())

    # Apply the UDF to the DataFrame
    df = df.withColumn(COL_IS_MAIN_UNIT, check_feature_udf(COL_FEATURE_TYPE, db_config["COL_INFO"], db_config["COL_UNIT"]))
    return df


def outliers_udf(conf):
    """ Flags outliers as 1 and non outliers as 0.

    Args:
        conf (Dict): Configuration file with numerical bounds

    Returns:
        function: Returns a user defined function
    """
    return F.udf(
        lambda x, y, z: 0
        if (
            x is not None
            and x >= conf[(y, z)].get(COL_EXPERT_LOWER_BOUND, -np.inf)
            and x <= conf[(y, z)].get(COL_EXPERT_UPPER_BOUND, np.inf)
        )
        else 1
    )


def compute_outliers(
    df: pyspark.sql.DataFrame, db_config: Dict, specific_features_config: Dict
):
    """ Computes statistical and expert advised outliers in two different dataframes. Allows to flag expert advised outliers to remove them from the cohort later on.

    Args:
        df (pyspark.sql.DataFrame): DataFrame with numerical feature information
        db_config (Dict): Database related configuration file
        features_config (Dict): Feature related configuration file

    Returns:
        pyspark.sql.DataFrame: Two dataframes, one with expert advised outliers flagged, another with statistical outliers.
    """
    df_outliers = (
        df.filter(~F.col(db_config["COL_RESULT"]).isNull())
        .groupBy(db_config["COL_INFO"], db_config["COL_UNIT"], COL_FEATURE_TYPE)
        .agg(
            F.count(db_config["COL_INFO"]).alias("COUNT"),
            F.expr(f"percentile({db_config['COL_RESULT']}, 0.25)").alias("Q1"),
            F.expr(f"percentile({db_config['COL_RESULT']}, 0.75)").alias("Q3"),
        )
    )
    df_outliers = df_outliers.withColumn("IQR", F.col("Q3") - F.col("Q1"))
   
    outliers_conf = (
        df_outliers.toPandas()
        .set_index([db_config["COL_INFO"], db_config["COL_UNIT"]])
        .to_dict(orient="index")
    )
    for feature, conf in specific_features_config.items():
        if (feature, conf["main_unit"]) in outliers_conf.keys():
            outliers_conf[(feature, conf["main_unit"])][COL_EXPERT_LOWER_BOUND] = conf[
                "lower_limit"
            ]
            outliers_conf[(feature, conf["main_unit"])][COL_EXPERT_UPPER_BOUND] = conf[
                "upper_limit"
            ]
    df = df.withColumn(
        COL_IS_OUTLIER,
        outliers_udf(outliers_conf)(
            F.col(db_config["COL_RESULT"]),
            F.col(db_config["COL_INFO"]),
            F.col(db_config["COL_UNIT"]),
        ),
    )
    return df, df_outliers


def remove_relative_indicator(
    df: pyspark.sql.DataFrame, col_relative_indicator: str
) -> pyspark.sql.DataFrame:
    """ Removes columns with a relative indicator.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_relative_indicator (str): Column with the relative indicator

    Returns:
        pyspark.sql.DataFrame: Filtered DataFrame
    """
    df = df.filter(F.col(col_relative_indicator).isNull())
    return df


def deduplicate_info_by_date(
    df: pyspark.sql.DataFrame, db_config: Dict
) -> pyspark.sql.DataFrame:
    """ Removes duplicated rows with information from the same date.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file

    Returns:
        pyspark.sql.DataFrame: Pyspark DataFrame
    """
    df = df.dropDuplicates(
        subset=[db_config["COL_UUID"], db_config["COL_INFO"], db_config["COL_DATE"]]
    )
    return df


def keep_main_unit(df: pyspark.sql.DataFrame, col_main_unit: str) -> pyspark.sql.DataFrame:
    """ Keeps numerical values that are in the main unit.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_main_unit (str): Column containing information about the unit

    Returns:
        pyspark.sql.DataFrame: Filtered Pyspark DataFrame
    """
    df = df.filter(F.col(col_main_unit) == 1)
    df = df.drop(col_main_unit)
    return df


def remove_outliers(df: pyspark.sql.DataFrame, col_outlier: str) -> pyspark.sql.DataFrame:
    """ Removes outliers based on their numerical value for a feature.

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        col_outlier (str): Column containing information about wether or not the value is considered an outlier or not.

    Returns:
        pyspark.sql.DataFrame: Filtered Pyspark DataFrame
    """
    df = df.filter(F.col(col_outlier) == 0)
    df = df.drop(col_outlier)
    return df


# COMMAND ----------

def apply_mapping(code, code_type, mapping_dict):
    """Detects if yes or no the given code is ICD9 and is mapped in the mapping dictionary.

    Args:
        code (str): code
        code_type (str): Type of the code (ICD10, ICD9, SDS, ...)
        mapping_dict (Dict): Dictionary in which ICD9 codes are mapped (to ICD10 codes)

    Returns:
        int: 1 if code is a mapped ICD9 code. 0 if not.
    """
    if code_type != "ICD9":
        return 0
    else:
        map_value = mapping_dict.get(code, None)
    if map_value:
        return 1
    return 0


def mapping_udf(mapping_dict):
    """User defined function returning 1 if a given code is mapped in the ICD9->ICD10 mapping dictionary, 0 otherwise.

    Args:
        mapping_dict (Dict): Dictionary in which ICD9 codes are mapped (to ICD10 codes)

    Returns:
        function: Returns a user defined function
    """
    return F.udf(lambda code, code_type: apply_mapping(code, code_type, mapping_dict))


def apply_col_info_mapping(map_value, code, mapping_dict):
    """Applies the mappings of ICD9 codes towards ICD10 codes with the mapping dictionary.

    Args:
        map_value (int): 0 if code is not mapped, 1 otherwise
        code (str): diagnosis code to be possibly mapped
        mapping_dict (Dict): Dictionary in which ICD9 codes are mapped (to ICD10 codes)

    Returns:
        str: Returns a code : ICD10 translation from ICD9 if mapped. If not mapped returns original code.
    """
    if int(map_value) == 1:
        return mapping_dict[code]
    else:
        return code


def col_info_map_udf(mapping_dict):
    """Applies the ICD9->ICD10 mapping with a user defined function

    Args:
        mapping_dict (Dict): Dictionary in which ICD9 codes are mapped (to ICD10 codes)

    Returns:
        function: Returns a user defined function
    """
    return F.udf(
        lambda map_value, code: apply_col_info_mapping(map_value, code, mapping_dict)
    )


def apply_col_info_type_mapping(map_value, code_type):
    """Changes the code_type to ICD10 if map_value is 1

    Args:
        map_value (int): 0 if code is not mapped, 1 otherwise
        code_type (str): Type of the code (ICD10, ICD9, SDS, ...)

    Returns:
        str: final code type
    """
    if int(map_value) == 1:
        return "ICD10"
    else:
        return code_type


def col_info_type_map_udf():
    """User defined function that changes the code_type to ICD10 if map_value is 1 (code was mapped)

    Returns:
        function: Returns a user defined function
    """
    return F.udf(
        lambda map_value, code_type: apply_col_info_type_mapping(map_value, code_type)
    )


def mapping_to_ICD10(df, db_config, mapping_dict):
    """Applies the mapping of ICD9 codes towards ICD10 codes.
    Mappings are all exact translations from ICD9 to ICD10 in the mapping dictionary in the configuration

    Args:
        df (pyspark.sql.DataFrame): Dataframe with columns with ICD9 codes to be mapped
        db_config (Dict): Database related configuration file
        mapping_dict (Dict): Dictionary in which ICD9 codes are mapped (to ICD10 codes)

    Returns:
        pyspark.sql.DataFrame: Dataframe only with some ICD9 codes translated in ICD10 codes, and their types too. Also a new column 'MAPPED' indicated if the code was translated or not.
    """
    df = df.withColumn(
        "MAPPED",
        mapping_udf(mapping_dict)(
            F.col(db_config["COL_INFO"]), F.col(db_config["COL_INFO_TYPE"])
        ).cast(IntegerType()),
    )
    df = df.withColumn(
        db_config["COL_INFO"],
        col_info_map_udf(mapping_dict)(F.col("MAPPED"), F.col(db_config["COL_INFO"])),
    )
    df = df.withColumn(
        db_config["COL_INFO_TYPE"],
        col_info_type_map_udf()(F.col("MAPPED"), F.col(db_config["COL_INFO_TYPE"])),
    )
    return df


def get_goal_status(df: pyspark.sql.DataFrame, db_config: Dict, features_config: Dict):
    """Adds 2 new columns to the dataframe : one to indicate if the diag is related to the algorithm, and one to flag the interesting features : the certain ones or the ones that are in the configuration file and have the right status.

    Example of use : in an AMSD algorithm, patients are flagged if they have a family history of an ASMD diagnosis. Thus in optum to do that we retrieve the patients that have a (DIAGNOSIS_STATUS = Family History) for an ASMD code input in the diag Table. Then we want to retrieve other diagnosis (certain only) that are not related to the algorithm in order to highlight other prevalent diagnoses in the cohorts.

    Args:
        df (pyspark.sql.DataFrame): Dataframe of feature that was preprocessed
        db_config (Dict): Database related configuration file
        features_config (Dict): Configuration of the feature

    Returns:
        pyspark.sql.DataFrame: Dataframe with 2 columns indicating if each diagnosis is useful or not
    """
    # Retrieve all the distincts diags in the cohort/dataframe
    diags_of_interest = (
        df.select(F.col(db_config["COL_INFO"]))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    # create a dict with the diags of the algo in the config associated to their goal status
    feature_to_status_config = {
        key: value["goal_status"]
        for key, value in features_config.items()
        if key in diags_of_interest
    }

    def determine_feature_type(info, status):
        for feature, statuses in feature_to_status_config.items():
            if info == feature and status in statuses:
                return SPECIFIC_FEATURE_TYPE
        return BROAD_FEATURE_TYPE
    # Convert the Python function to a PySpark UDF
    feature_type_udf = F.udf(determine_feature_type, StringType())
    # Apply the UDF to the DataFrame
    df = df.withColumn(COL_FEATURE_TYPE, feature_type_udf(db_config["COL_INFO"], db_config["COL_STATUS"]))

    # check if feature has the status of interest
    # Filter : keeping specific diags (in the algo) as just defined, or broad features that are certain
    filter_condition = (F.col(COL_FEATURE_TYPE) == SPECIFIC_FEATURE_TYPE) | (
        (F.col(COL_FEATURE_TYPE) == BROAD_FEATURE_TYPE)
        & (F.col(db_config["COL_STATUS"]).isin(db_config["DEFAULT_GOAL_STATUSES"]))
    )
    # New column with all algo related diags + certain diags
    df = df.withColumn(COL_IS_GOAL_STATUS, filter_condition.cast("int"))
    return df


def get_name_mapping(
    code: str, generic_features_config: Dict, mapping_col: str, generic_n_digits: int
):
    """Get name of MEDICAL TERM or MEDICAL GROUP of 'code'.
    MEDICAL_TERM is obtained using official translation of ICD10 codes from ICD10 website. MEDICAL_GROUP is the traduction of the code with the first generic_n_digits digits.

    Args:
        code (str): code to translate
        generic_features_config (Dict): Dictionnary of names mapping to ICD10 codes
        mapping_col (str): Column to map
        generic_n_digits (int): Number of digits to keep from an ICD code to get its translation in MEDICAL GROUP  

    Returns:
        str or None: name retrieved from mapped ICD code
    """
    if mapping_col == "medical_term":
        name = generic_features_config.get(code, None)
        return name.replace("_", " ") if name else name
    if mapping_col == "medical_group":
        name = generic_features_config.get(code[:generic_n_digits], None)
        return name.replace("_", " ") if name else name
    return None


def udf_name_mapping(
    generic_features_config: Dict, mapping_col: str, generic_n_digits: int
):
    """User defined function to Get name of MEDICAL TERM or MEDICAL GROUP of ICD codes in a specific column.

    Args:
        generic_features_config (Dict): Dictionnary of names mapping to ICD10 codes
        mapping_col (str): Column to map
        generic_n_digits (int): Number of digits to keep from an ICD code to get its translation in MEDICAL GROUP

    Returns:
        function: Returns a user defined function
    """
    return F.udf(
        lambda x: get_name_mapping(
            x, generic_features_config, mapping_col, generic_n_digits
        )
    )


def get_icd_feature_info(
    df: pyspark.sql.DataFrame,
    db_config: Dict,
    generic_features_config: Dict,
    generic_n_digits: int,
):
    """Adding 2 new columns to the diagnosis table: MEDICAL_TERM and MEDICAL_GROUP that are associated to each diagnosis.

    Args:
        df (pyspark.sql.DataFrame): Preprocessed Dataframe of Diagnosis table
        db_config (Dict): Database related configuration file
        generic_features_config (Dict): Dictionnary of names mapping to ICD10 codes
        generic_n_digits (int): Number of digits to keep from an ICD code to get its translation in MEDICAL GROUP  

    Returns:
        pyspark.sql.DataFrame: Dataframe with 2 new columns
    """
    df = df.withColumn(
        COL_ICD_MEDICAL_TERM,
        udf_name_mapping(generic_features_config, "medical_term", generic_n_digits)(
            F.col(db_config["COL_INFO"])
        ),
    )
    df = df.withColumn(
        COL_ICD_MEDICAL_GROUP,
        udf_name_mapping(generic_features_config, "medical_group", generic_n_digits)(
            F.col(db_config["COL_INFO"])
        ),
    )
    return df


def keep_goal_status(df: pyspark.sql.DataFrame, col_goal_status: int):
    """Keeping only the interesting diagnoses: the ones that have a col_gol_status of 1 
    Those diagnoses are : the ones that are certain and the ones that are related to the configuration file. 

    Args:
        df (pyspark.sql.DataFrame): Dataframe to filter
        col_goal_status (str): column of goal status to filter on

    Returns:
        pyspark.sql.DataFrame: Clean Pyspark DataFrame
    """
    df = df.filter(F.col(col_goal_status) == 1)
    df = df.drop(col_goal_status)
    return df


# COMMAND ----------

def clean_features(
    df: pyspark.sql.DataFrame, db_config: Dict, drop_duplicates: str = True
) -> pyspark.sql.DataFrame:
    """ Cleans categorical and numerical features

    Args:
        df (pyspark.sql.DataFrame): Pyspark DataFrame
        db_config (Dict): Configuration file
        drop_duplicates (str): Default = True. Allows to deduplicate information by date.

    Returns:
        pyspark.sql.DataFrame: Clean Pyspark DataFrame
    """
    if db_config.get("TABLE_TYPE", None) == "CODE":
        if db_config.get("COL_INFO_TYPE", None) in df.columns:
            df = df.filter(F.col(db_config["COL_INFO_TYPE"]).isin(db_config["DEFAULT_CODE_TYPES"]))
        if COL_IS_GOAL_STATUS in df.columns:
            df = keep_goal_status(df, COL_IS_GOAL_STATUS)
    if db_config.get("TABLE_TYPE", None) == "NUMERICAL":
        if db_config.get("COL_RELATIVE_INDICATOR", None):
            df = remove_relative_indicator(df, db_config["COL_RELATIVE_INDICATOR"])
        if COL_IS_MAIN_UNIT in df.columns:
            df = keep_main_unit(df, COL_IS_MAIN_UNIT)
            if COL_IS_OUTLIER in df.columns:
                df = remove_outliers(df, COL_IS_OUTLIER)
    if drop_duplicates:
        df = deduplicate_info_by_date(df, db_config)
    return df


def process_features(
    df: pyspark.sql.DataFrame,
    feature_name: str,
    db_config: Dict,
    specific_features_config: Dict,
    mapping_dict: Dict,
    generic_n_digits: int,
) -> pyspark.sql.DataFrame:
    """ Processes and cleans all features into a single dataframe.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to fill with clean information
        feature_name (str): Name of feature studied
        db_config (Dict): Database related configuration
        specific_features_config (Dict): Feature configuration file
        mapping_dict (Dict): Dictionnary of mapping ICD 3 digits to MEDICAL TERM
        generic_n_digits (int): Number of digits to keep from an ICD code to get its translation in MEDICAL GROUP 

    Returns:
        pyspark.sql.df: Clean dataframe with all features.
    """
    if db_config.get("COL_STATUS", None) and feature_name in ["diags"]:
        df = get_goal_status(df, db_config, specific_features_config)
        df = get_icd_feature_info(df, db_config, mapping_dict, generic_n_digits)
        df_outliers = None
    elif db_config.get("COL_UNIT", None) and feature_name in [
        "labs",
        "observations",
        "measurements",
    ]:
        df = get_main_unit(df, db_config, specific_features_config, mapping_dict)
        if db_config.get("COL_RESULT", None):
            df, df_outliers = compute_outliers(df, db_config, specific_features_config)
        else:
            df_outliers = None
    elif feature_name in ["procedures"]:
        features_of_interest = (
            df.select(F.col(db_config["COL_INFO"]))
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        feature_to_code_type_config = {
            key: value["code_type"]
            for key, value in specific_features_config.items()
            if key in features_of_interest
        }
        # check if feature is integrated in the configuration
        def determine_feature_type(info, info_type):
            for feature, code_type in feature_to_code_type_config.items():
                if info == feature and info_type == code_type:
                    return SPECIFIC_FEATURE_TYPE
            return BROAD_FEATURE_TYPE
        # Convert the Python function to a PySpark UDF
        feature_type_udf = F.udf(determine_feature_type, StringType())
        # Apply the UDF to the DataFrame
        df = df.withColumn(COL_FEATURE_TYPE, feature_type_udf(db_config["COL_INFO"], db_config["COL_INFO_TYPE"]))
        df_outliers = None
    elif feature_name in ["treatments"]:
        features_of_interest = (
            df.select(F.col(db_config["COL_INFO"]))
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        feature_to_code_type_config = {
            key: value["code_type"]
            for key, value in specific_features_config.items()
            if key in features_of_interest
        }
        # check if feature is integrated in the configuration
        # NB: there is not a COL_INFO_TYPE for treatments in Optum EHR, all treatments are under NDC format
        def determine_feature_type(info):
            for feature, code_type in feature_to_code_type_config.items():
                if info == feature:
                    return SPECIFIC_FEATURE_TYPE
            return BROAD_FEATURE_TYPE
        # Convert the Python function to a PySpark UDF
        feature_type_udf = F.udf(determine_feature_type, StringType())
        # Apply the UDF to the DataFrame
        df = df.withColumn(COL_FEATURE_TYPE, feature_type_udf(db_config["COL_INFO"]))
        df_outliers = None
    elif feature_name in ["SDS"]:
        features_of_interest = (
            df.select(F.col(db_config["COL_INFO"]))
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        feature_to_attribute_config = {
            key: value["attribute"]
            for key, value in specific_features_config.items()
            if key in features_of_interest
        }
        # check if feature is integrated in the configuration
        def determine_feature_type(info, info_attribute):
            for feature, attribute in feature_to_attribute_config.items():
                # if info == feature and info_attribute == attribute:
                if info == feature:
                    return SPECIFIC_FEATURE_TYPE
            return BROAD_FEATURE_TYPE
        # Convert the Python function to a PySpark UDF
        feature_type_udf = F.udf(determine_feature_type, StringType())
        # Apply the UDF to the DataFrame
        df = df.withColumn(COL_FEATURE_TYPE, feature_type_udf(db_config["COL_INFO"], db_config["COL_ATTRIBUTE"]))
        df_outliers = None
    else:
        df = df.withColumn(COL_FEATURE_TYPE, F.lit(BROAD_FEATURE_TYPE))
        df_outliers = None
    return df, df_outliers


# COMMAND ----------

def get_preprocessed_features(
    algo_name: str,
    cohort_name: str,
    feature_name: str,
    output_data_path: str,
    output_filename: str,
    diags_mapping_approximation=False,
) -> pyspark.sql.DataFrame:
    """ Preprocesses a given feature table and adds patients information.
    
    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        cohort_name (str): Name of cohort studied
        feature_name (str): Name of feature studied
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file
        diags_mapping_approximation (Bool): Default = False.
    
    Returns:
        pyspark.sql.DataFrame: Preprocessed and enriched dataframe.
    """
    df_cohort = read_spark_file(
        os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{cohort_name}"
    )
    feature_table = read_spark_file(
        CONFIG_PATH_DATABASE, DATABASES_FEATURE_CONFIG[feature_name]["TABLE_NAME"]
    )
    PTIDs = (
        df_cohort.select(F.col(PATIENT_UUID))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    df_feature = feature_table.filter(F.col(PATIENT_UUID).isin(PTIDs))
    # pre-processing : Removes null values, sets columns with dates on one specific format, selects columns to keep in order to have a clean and ready to use dataframe
    df_feature_preprocessed = preprocess_features(
        df_feature, DATABASES_FEATURE_CONFIG[feature_name]
    )
    # add index date info, birth year and gender
    df_feature_preprocessed = df_feature_preprocessed.join(
        df_cohort[
            [
                PATIENT_UUID,
                COL_INDEX_DATE,
                PATIENTS["COL_BIRTH_YEAR"],
                PATIENTS["COL_GENDER"],
            ]
        ],
        on=PATIENT_UUID,
        how="left",
    )
    if feature_name == "diags":
        # enrich feature config with ICD9 codes
        mapping_dict = load_yaml_mapping_file(ICD_CONF_PATH, ICD9_TO_ICD10_MAPPING_FILE)
        df_feature_preprocessed = mapping_to_ICD10(
            df_feature_preprocessed,
            DATABASES_FEATURE_CONFIG[feature_name],
            mapping_dict,
        )
    write_spark_file(df_feature_preprocessed, output_data_path, output_filename)


# COMMAND ----------

def get_processed_features(
    algo_name: str,
    cohort_name: str,
    feature_name: str,
    generic_n_digits: int,
    input_data_path: str,
    input_filename: str,
    output_data_path: str,
    output_filename: str,
):
    """ Writes processed broad feature matrix.

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        cohort_name (str): Name of cohort studied
        feature_name (str): Name of feature studied
        input_data_path (str): Path to input file
        input_filename (str): Name of input file
        output_data_path (str): Path to store output file
        output_filename (str): Name of output file
    """
    df_feature_preprocessed = read_spark_file(input_data_path, input_filename)
    db_config = DATABASES_FEATURE_CONFIG[feature_name]
    # Loading of the config with the info for each specific feature
    # Example : FEATURES_CONFIG['diags']['ASMD_russia'] -> config yaml file in config/algo/ASMD/russia/yaml_config/diags_conf.yaml
    features_config = FEATURES_CONFIG.get(feature_name, {}).get(algo_name, {})

    # Retrieve a single mapping dict specific to the feature being processed
    if feature_name == "diags":
        mapping_dict = load_yaml_mapping_file(ICD_CONF_PATH, ICD10_TO_DESCRIPTION_MAPPING_FILE)
    elif feature_name == "labs":
        mapping_dict = load_yaml_mapping_file(
            DATA_PROVIDER_CONF_PATH, LABS_TO_MAIN_UNIT_MAPPING_FILE
        )
    elif feature_name == "observations":
        mapping_dict = load_yaml_mapping_file(
            DATA_PROVIDER_CONF_PATH, OBSERVATIONS_TO_MAIN_UNIT_MAPPING_FILE
        )
    elif feature_name == "measurements":
        mapping_dict = load_yaml_mapping_file(
            DATA_PROVIDER_CONF_PATH, MEASUREMENTS_TO_MAIN_UNIT_MAPPING_FILE
        )
    else:
        mapping_dict = {}
    
    # Process the feature
    df_feature_processed, df_outliers = process_features(
        df_feature_preprocessed, feature_name, db_config, features_config, mapping_dict, generic_n_digits,
    )

    # Saving new dataframe
    if df_feature_processed.count() > 0:
        write_spark_file(
            df_feature_processed,
            os.path.join(INTERMEDIATE_DATA_PATH, algo_name, feature_name),
            f"{algo_name}_{cohort_name}_processed_{BROAD_FEATURE_TYPE}_{feature_name}",
        )
        if df_outliers is not None:
            write_spark_file(
                df_outliers,
                os.path.join(STATISTICS_DATA_PATH, algo_name, feature_name),
                f"{algo_name}_{cohort_name}_{BROAD_FEATURE_TYPE}_{feature_name}_outliers_conf",
            )
        df_cleaned_feature = clean_features(
            df_feature_processed, db_config, drop_duplicates=True
        )
        if df_cleaned_feature.count() > 0:
            write_spark_file(df_cleaned_feature, output_data_path, output_filename)