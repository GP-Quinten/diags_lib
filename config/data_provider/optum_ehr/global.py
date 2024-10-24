# Databricks notebook source
sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

OPTUM_DATACUT = "202301"
CONFIG_PATH_DATABASE = f"/mnt/optumhum/{OPTUM_DATACUT}/ontology/base/"

# COMMAND ----------

# Name of the universally unique identifier of a patient
PATIENT_UUID = "PTID"

# COMMAND ----------

# Configuration for the OPTUM Patient table
PATIENTS = {
    "TABLE_NAME": "Patient",
    "TABLE_TYPE": "PATIENT_INFO",
    "COL_UUID": PATIENT_UUID,
    "COL_BIRTH_YEAR": "BIRTH_YR",
    "COL_GENDER": "GENDER",
    "COL_RACE": "RACE",
    "COL_ETHNICITY": "ETHNICITY",
    "COL_REGION": "REGION",
    "COL_DECEASED_INDICATOR": "DECEASED_INDICATOR",
    "COL_DATE_OF_DEATH": "DATE_OF_DEATH",
    "COL_FIRST_ACTIVITY": "FIRST_MONTH_ACTIVE",
    "COL_LAST_ACTIVITY": "LAST_MONTH_ACTIVE", 
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
    "FEMALE_GENDER": "Female",
    "MALE_GENDER": "Male",
}

# Configuration for the OPTUM Diagnosis table
DIAGNOSES = {
    "TABLE_NAME": "Diagnosis",
    "TABLE_TYPE": "CODE",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "DIAGNOSIS_CD",
    "COL_INFO_TYPE": "DIAGNOSIS_CD_TYPE",
    "COL_STATUS": "DIAGNOSIS_STATUS",
    "COL_DATE": "DIAG_DATE",
    # ordered by preference date
    "DATE_PRIORITY": ["DIAG_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
    # subset of diagnosis statuses to consider in the table
    "DEFAULT_GOAL_STATUSES": ["Diagnosis of"],
    # subset of code types to consider in the table - note that the code already integrates exact mapping with ICD9 codes when ICD10 in subset
    "DEFAULT_CODE_TYPES": ["ICD10"]

}

# Configuration for the OPTUM RX Prescribed table
TREATMENTS = {
    "TABLE_NAME": "RX Prescribed",
    "TABLE_TYPE": "CODE",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "NDC",
    "COL_DATE": "RXDATE",
    # ordered by preference date
    "DATE_PRIORITY": ["RXDATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
}

# Configuration for the OPTUM Lab table
LABS = {
    "TABLE_NAME": "Lab",
    "TABLE_TYPE": "NUMERICAL",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "TEST_NAME",
    "COL_INFO_TYPE": "TEST_TYPE",
    "COL_RESULT": "TEST_RESULT",
    "COL_UNIT": "RESULT_UNIT",
    "COL_RELATIVE_INDICATOR": "RELATIVE_INDICATOR",
    "COL_NORMAL_RANGE": "NORMAL_RANGE",
    "COL_DATE": "LAB_DATE",
    # ordered by preference date
    "DATE_PRIORITY": ["COLLECTED_DATE", "RESULT_DATE", "ORDER_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
}

# Configuration for the OPTUM Observation table
OBSERVATIONS = {
    "TABLE_NAME": "Observation",
    "TABLE_TYPE": "NUMERICAL",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "OBS_TYPE",
    "COL_RESULT": "OBS_RESULT",
    "COL_UNIT": "OBS_UNIT",
    "COL_DATE": "OBS_DATE",
    # ordered by preference date
    "DATE_PRIORITY": ["RESULT_DATE", "OBS_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
}

# Configuration for the OPTUM NLP Measurement table
MEASUREMENTS = {
    "TABLE_NAME": "NLP Measurement",
    "TABLE_TYPE": "NUMERICAL",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "MEASUREMENT_TYPE",
    "COL_RESULT": "MEASUREMENT_VALUE",
    "COL_UNIT": "MEASUREMENT_DETAIL",
    "COL_DATE": "MEASUREMENT_DATE",
    # ordered by preference date
    "DATE_PRIORITY": ["MEASUREMENT_DATE", "NOTE_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
}

# Configuration for the OPTUM Procedure table
PROCEDURES = {
    "TABLE_NAME": "Procedure",
    "TABLE_TYPE": "CODE",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "PROC_CODE",
    "COL_INFO_TYPE": "PROC_CODE_TYPE",
    "COL_DESCRIPTION": "PROC_DESC",
    "COL_DATE": "PROC_DATE",
    # ordered by preference date
    "DATE_PRIORITY": ["PROC_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
    # subset of code types to consider in the table
    "DEFAULT_CODE_TYPES": ["ICD10", "CPT4", "CPT5"]
}

# Configuration for the OPTUM NLP SDS table
SDS = {
    "TABLE_NAME": "NLP SDS",
    "TABLE_TYPE": "NOTES",
    "COL_UUID": PATIENT_UUID,
    "COL_INFO": "SDS_TERM",
    "COL_DATE": "SDS_DATE",
    "COL_ATTRIBUTE": "SDS_ATTRIBUTE",
    "COL_LOCATION": "SDS_LOCATION",
    "COL_SENTIMENT": "SDS_SENTIMENT",
    # ordered by preference date
    "DATE_PRIORITY": ["OCCURRENCE_DATE", "NOTE_DATE"],
    # format of the date-related columns
    "DATE_FORMAT": "yyyy-MM-dd",
}

# COMMAND ----------

DATABASES_FEATURE_CONFIG = {
  "diags": DIAGNOSES,
  "treatments": TREATMENTS,
  "labs": LABS,
  "procedures": PROCEDURES,
  "observations": OBSERVATIONS,
  "measurements": MEASUREMENTS,
  "SDS": SDS
}

# COMMAND ----------

