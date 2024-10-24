# Databricks notebook source
import re
from typing import Dict, List

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType

# COMMAND ----------

# from utils.io_functions import loading, saving
# from config.variables import base
# from config.algo import global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/loading

# COMMAND ----------

# MAGIC %run ../../../utils/io_functions/saving

# COMMAND ----------

# MAGIC %run ../../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../../config/variables/base

# COMMAND ----------

def parse_logical_rule(rule_str: str) -> str:
    """Replaces logical operators written as strings with their respective logical operators : AND -> &, OR -> |, NOT -> ~.

    Args:
        rule_str (str): Logical rule of the algorithm

    Returns:
        str: Parsed rule
    """
    rule_str = rule_str.replace(" AND ", " & ")
    rule_str = rule_str.replace(" OR ", " | ")
    rule_str = rule_str.replace("NOT ", "~")
    return rule_str


def get_feature_names(
    rule: str,
    algorithm_block_to_category: dict,
    algorithm_category_to_medical_term: dict,
) -> str:
    """Goes through the algorithm rules blocks and retrieves the corresponding categories, then the corresponding medical terms.

    Args:
        rule (str): Algorithm rule
        algorithm_block_to_category (dict): Dictionnary with the corresponding block to each algorithm category
        algorithm_category_to_medical_term (dict): Dictionnary with the corresponding category to each algorithm medical term

    Returns:
        str: Algorithm rule at the medical term level
    """
    for key, value in algorithm_block_to_category.items():
        rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
    for key, value in algorithm_category_to_medical_term.items():
        rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
    return rule


def generate_possibilities(rule: str) -> list:
    """Generates the various possibilities in a rule if an OR condition is present.

    Args:
        rule (str): Parsed algorithm rule

    Returns:
        list: Contains the various possbilities for the rule
    """
    # Find first occurrence of OR clause
    or_match = re.search("\(((?:(?!& \().)* \| (?:(?!\) &).)*)\)", rule)

    # If no OR clause is found, then return the rule as the only possibility
    if or_match is None:
        return [rule]

    # Get the OR clause and its possibilities
    or_clause = or_match.group(1)
    possibilities = or_clause.split(" | ", 1)
    # Generate a rule for each possibility
    new_rules = [
        rule.replace(f"({or_clause})", f"({poss})", 1) for poss in possibilities
    ]

    # Recursively generate possibilities for each new rule
    return [
        possibility
        for new_rule in new_rules
        for possibility in generate_possibilities(new_rule)
    ]


def get_global_rule(
    df: pd.DataFrame,
    algorithm_rule: str,
    algorithm_block_to_category: dict,
    algorithm_category_to_medical_term: dict,
    medical_term_to_ICD_codes: dict,
) -> str:
    """Parses all the rules from blocks to medical terms and generates the various possibilities in case there are any OR conditions.

    Args:
        df (pd.DataFrame): Feature matrix
        algorithm_rule (str): Parsed algorithm rule
        algorithm_block_to_category (dict): Dictionnary with the corresponding block to each algorithm category
        algorithm_category_to_medical_term (dict): Dictionnary with the corresponding category to each algorithm medical term
        medical_term_to_ICD_codes (dict): Dictionnary with the corresponding medical term to each algorithm ICD/NDC code

    Returns:
        list: All rules of the algorithm parsed
    """
    # Parse rule and generate possibilities
    parsed_algorithm_rule = parse_logical_rule(algorithm_rule)
    all_rules = generate_possibilities(parsed_algorithm_rule)

    # Build global rule by going through each rule individually and assembling them
    global_rule = []
    for i, rule in enumerate(all_rules, start=1):
        print(f"Rule {i}: {rule}")
        for key, value in algorithm_block_to_category.items():
            rule = rule.replace(key, parse_logical_rule(value))
        for key, value in algorithm_category_to_medical_term.items():
            rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
        for key, value in medical_term_to_ICD_codes.items():
            rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
        for col_name in df.drop("PTID").columns:
            rule = rule.replace(f"({col_name})", f'(F.col("{col_name}"))')
        global_rule.append(f"({rule})")
    return global_rule


def get_algo_rule(
    df: pd.DataFrame,
    rule: str,
    algorithm_block_to_category: dict,
    algorithm_category_to_medical_term: dict,
    medical_term_to_ICD_codes: dict,
) -> str:
    """Unveils one rule from the block level to the medical term level.

    Args:
        df (pd.DataFrame): Feature matrix
        rule (str): Rule of interest (each algorithm can contain several)
        algorithm_block_to_category (dict): Dictionnary with the corresponding block to each algorithm category
        algorithm_category_to_medical_term (dict): Dictionnary with the corresponding category to each algorithm medical term
        medical_term_to_ICD_codes (dict): Dictionnary with the corresponding medical term to each algorithm ICD/NDC code

    Returns:
        str: Selected rule at the medical term level
    """
    for key, value in algorithm_block_to_category.items():
        rule = rule.replace(key, parse_logical_rule(value))
    for key, value in algorithm_category_to_medical_term.items():
        rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
    for key, value in medical_term_to_ICD_codes.items():
        rule = rule.replace(f"({key})", f"({parse_logical_rule(value)})")
    for col_name in df.drop("PTID").columns:
        rule = rule.replace(f"({col_name})", f'(F.col("{col_name}"))')
    return rule


def get_availability(
    df: pd.DataFrame,
    level: str,
    global_rule: str,
    algorithm_blocks: list,
    algorithm_block_to_category: dict,
    algorithm_category_to_medical_term: dict,
    medical_term_to_ICD_codes: dict,
) -> pd.DataFrame:
    """Retrieves the availability (number of patients with enough information to go through) per rule or per algorithm block.

    Args:
        df (pd.DataFrame): Feature matrix
        level (str): Rule or block (one rule is made of several blocks)
        global_rule (str): All rules of the algorithm parsed
        algorithm_blocks (list): List of the algorithm blocks (i.e. ["ALGORITHM_BLOCK_A","ALGORITHM_BLOCK_B])
        algorithm_block_to_category (dict): Dictionnary with the corresponding block to each algorithm category
        algorithm_category_to_medical_term (dict): Dictionnary with the corresponding category to each algorithm medical term
        medical_term_to_ICD_codes (dict): Dictionnary with the corresponding medical term to each algorithm ICD/NDC code

    Returns:
        pd.DataFrame: Dataframe with the availability for each cohort at a given level
    """
    if level == "rule":
        global_availability_rule = []
        for i, rule in enumerate(global_rule.copy(), start=1):
            rule_for_availability = rule.replace("~", "")
            df = df.withColumn(f"rule_{i}_availability", eval(rule_for_availability))
            df = df.withColumn(
                f"rule_{i}_availability",
                F.when(F.col(f"rule_{i}_availability").isNull(), False).otherwise(True),
            )
            global_availability_rule.append(f"F.col('rule_{i}_availability')")
        minimal_availability_rule = " | ".join(global_availability_rule)
        df = df.withColumn(
            "minimal_global_availability", eval(minimal_availability_rule)
        )
        strict_availability_rule = " & ".join(global_availability_rule)
        df = df.withColumn("strict_global_availability", eval(strict_availability_rule))

    if level == "algorithm_block":
        for block in algorithm_blocks:
            algo_rule = get_algo_rule(
                df,
                block,
                algorithm_block_to_category,
                algorithm_category_to_medical_term,
                medical_term_to_ICD_codes,
            )
            rule_for_availability = algo_rule.replace("~", "")
            df = df.withColumn(f"{block}_availability", eval(rule_for_availability))
            df = df.withColumn(
                f"{block}_availability",
                F.when(F.col(f"{block}_availability").isNull(), False).otherwise(True),
            )
    return df


def get_global_label(rule_labels: list):
    """Retrieves any label that is not null.

    Args:
        rule_labels (list): List of labels
    """
    not_null_labels = [label for label in rule_labels if label is not None]
    if not_null_labels:
        return any(not_null_labels)
    return None


global_label_udf = F.udf(lambda rule_labels: get_global_label(rule_labels))


def get_label(
    df: pd.DataFrame,
    level: str,
    global_rule: str,
    algorithm_blocks: list,
    algorithm_block_to_category: dict,
    algorithm_category_to_medical_term: dict,
    medical_term_to_ICD_codes: dict,
) -> pd.DataFrame:
    """Computes for each patient if they have been flagged by the algorithm.

    Args:
        df (pd.DataFrame): Feature matrix
        level (str): Rule or block (one rule is made of several blocks)
        global_rule (str): All rules of the algorithm parsed
        algorithm_blocks (list): List of the algorithm blocks (i.e. ["ALGORITHM_BLOCK_A","ALGORITHM_BLOCK_B])
        algorithm_block_to_category (dict): Dictionnary with the corresponding block to each algorithm category
        algorithm_category_to_medical_term (dict): Dictionnary with the corresponding category to each algorithm medical term
        medical_term_to_ICD_codes (dict): Dictionnary with the corresponding medical term to each algorithm ICD/NDC code

    Returns:
        pd.DataFrame: Dataframe with the final labels per level and globally
    """
    # Retrieve availability
    df_availability = df.alias("df_availability")
    df_availability = df_availability.replace(False, True)
    df_availability = get_availability(
        df_availability,
        level,
        global_rule,
        algorithm_blocks,
        algorithm_block_to_category,
        algorithm_category_to_medical_term,
        medical_term_to_ICD_codes,
    )
    df = df.join(
        df_availability.select(
            [PATIENTS["COL_UUID"]]
            + [col for col in df_availability.columns if col.endswith("_availability")]
        ),
        on=PATIENTS["COL_UUID"],
        how="left",
    )

    if level == "rule":
        # Evaluate each rule
        for i, rule in enumerate(global_rule, start=1):
            df_available = df.filter(F.col(f"rule_{i}_availability") == True).na.fill(
                value=False
            )
            df_available = df_available.withColumn(f"rule_{i}_label", eval(rule))
            df = df.join(
                df_available.select([PATIENTS["COL_UUID"], f"rule_{i}_label"]),
                on=PATIENTS["COL_UUID"],
                how="left",
            )
        final_df = df.select(
            [PATIENTS["COL_UUID"]]
            + [
                col
                for col in df.columns
                if col.endswith("_availability") or col.endswith("_label")
            ]
        )
        final_df = final_df.withColumn(
            "global_label",
            global_label_udf(
                F.array(
                    [F.col(col) for col in final_df.columns if col.endswith("_label")]
                )
            ).cast(BooleanType()),
        )

    if level == "algorithm_block":
        # Evaluate each block
        for block in algorithm_blocks:
            algo_rule = get_algo_rule(
                df,
                block,
                algorithm_block_to_category,
                algorithm_category_to_medical_term,
                medical_term_to_ICD_codes,
            )
            df_available = df.filter(F.col(f"{block}_availability") == True).na.fill(
                value=False
            )
            df_available = df_available.withColumn(f"{block}_label", eval(algo_rule))
            df = df.join(
                df_available.select([PATIENTS["COL_UUID"], f"{block}_label"]),
                on=PATIENTS["COL_UUID"],
                how="left",
            )

            final_df = df.select(
                [PATIENTS["COL_UUID"]]
                + [
                    col
                    for col in df.columns
                    if col.endswith("_availability") or col.endswith("_label")
                ]
            )
    return final_df


def get_availability_and_label(
    algo_name: str,
    level: str,
    input_data_path: str,
    input_filename: str,
    output_data_path: str,
    output_filename: str,
):
    """Retrieves availability and labels for a given LSD cohort and saves the information.

    Args:
        algo_name (str): Name of algorithm studied (LSD_type and country)
        level (str): Rule or block (one rule is made of several blocks)
        input_data_path (str): Path to input data
        input_filename (str): Name of input file (feature matrix)
        output_data_path (str): Path to store output data
        output_filename (str): Name of output file
    """
    df = read_spark_file(input_data_path, input_filename)
    global_rule = GLOBAL_ALGORITHM_RULE[algo_name]
    algorithm_blocks = ALGORITHM_BLOCKS[algo_name]
    algorithm_block_to_category = ALGORITHM_BLOCK_TO_CATEGORY[algo_name]
    algorithm_category_to_medical_term = ALGORITHM_CATEGORY_TO_MEDICAL_TERM[algo_name]
    medical_term_to_icd_codes = MEDICAL_TERM_TO_ICD_CODES[algo_name]
    global_rule = get_global_rule(
        df,
        global_rule,
        algorithm_block_to_category,
        algorithm_category_to_medical_term,
        medical_term_to_icd_codes,
    )
    df_labels = get_label(
        df,
        level,
        global_rule,
        algorithm_blocks,
        algorithm_block_to_category,
        algorithm_category_to_medical_term,
        medical_term_to_icd_codes,
    )
    write_spark_file(df_labels, output_data_path, output_filename)

def save_flagged_cohorts(LSD: str, cohorts_of_interest: list, timerange: str):
    """Saves flagged cohorts and all flagged patients for each LSD.
    Args:
        LSD (str): Name of LSD studied
        cohorts_of_interest (list): List of the cohorts to look at
        timerange (str): Must be one of "before_index_date", "just_before_index_date", or "lifetime". Must match the timerange used for implementation.
    """
    final_df_list = []
    for cohort in cohorts_of_interest:
        tmp = read_spark_file(os.path.join(COHORT_DATA_PATH, LSD), f"{LSD}_{cohort}")
        labels = read_spark_file(
            os.path.join(ALGO_OUTPUT_DATA_PATH, LSD),
            f"{LSD}_{cohort}_final_labels_{timerange}_per_rule",
        )
        flagged_patients = (
            labels.where(F.col("global_label") == True)
            .select(COL_UUID)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        tmp_flagged = tmp.where(F.col(COL_UUID).isin(flagged_patients))
        tmp_flagged = tmp_flagged.withColumn('COHORT_TYPE', F.lit(cohort))
        write_spark_file(
            tmp_flagged,
            os.path.join(COHORT_DATA_PATH, LSD),
            f"{LSD}_{cohort}_flagged_{timerange}",
        )

        final_df_list.append(tmp_flagged)
    # find common columns
    common_columns = set(final_df_list[0].columns)
    for df in final_df_list[1:]:
        common_columns = common_columns.intersection(set(df.columns))
    common_columns = list(common_columns)

    # concatenate all dataframes
    final_df = final_df_list[0]
    final_df = final_df.select(*common_columns)
    for df in final_df_list[1:]:
        df = df.select(*common_columns)
        final_df = final_df.unionAll(df)
    write_spark_file(
        final_df,
        os.path.join(COHORT_DATA_PATH, LSD),
        f"{LSD}_all_flagged_cohorts_{timerange}",
    )
