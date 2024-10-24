# Databricks notebook source
# from config.variables import base
# from config.algo import global_algo_conf
# from src.1_cohort_creation import 1_0_utils_cohort, 1_3_final_cohorts

# COMMAND ----------

# MAGIC %run ../../config/algo/global_algo_conf

# COMMAND ----------

# MAGIC %run ../../config/variables/base

# COMMAND ----------

# MAGIC %run ../../src/1_cohort_creation/1_0_utils_cohort

# COMMAND ----------

# MAGIC %run ../../src/1_cohort_creation/1_3_final_cohorts

# COMMAND ----------

def get_attrition_stats(
    algo_name: str,
    cohort: str,
    config_age: dict,
    config_enrollment: dict,
    config_sampling: dict,
    intermediate_data_path: str,
    intermediate_LSD_cohort_filename: str,
    intermediate_cohort_filename: str,
) -> pd.DataFrame:
    """Retrieves a table of attrition stats after each step of cohort creation

    Args:
        algo_name (str): Name of algorithm studied
        cohort (str) : Type of cohort studied
        config_age (dict): Configuration information about age
        config_enrollment (dict): Configuration information about enrollment duration
        config_sampling (dict): Configuration information about sampling
        intermediate_data_path (str): Path to intermediate data
        intermediate_LSD_cohort_filename (str): Name of intermediate LSD cohort
        intermediate_cohort_filename (str): Name of intermediate cohort (in most case control cohort)

    Returns:
        pd.DataFrame: Dataframe with attrition chart for each cohort
    """
    LSD_name = algo_name.split('_')[0]
    patients_table = read_spark_file(CONFIG_PATH_DATABASE, PATIENTS["TABLE_NAME"])
    intermediate_LSD_cohort = read_spark_file(
        intermediate_data_path, intermediate_LSD_cohort_filename
    )

    # exclusion
    exclusion_config = {
        COL_AGE_AT_INDEX_DATE: config_age,
        COL_ENROLLMENT_DURATION: config_enrollment,
    }

    df_specific_LSD = get_specific_LSD(intermediate_LSD_cohort, LSD_name)
    df_certain_other_LSD = get_certain_other_LSD(intermediate_LSD_cohort, LSD_name)

    if cohort == "cohort":
      # inclusion
      count = get_LSD_cohort(
        df_specific_LSD,
        df_certain_other_LSD,
        patients_table,
        exclusion_config,
        get_len=True,
      )

    elif cohort == "like_cohort":
      count = get_LSD_like_cohort(
          df_specific_LSD,
          df_certain_other_LSD,
          patients_table,
          exclusion_config,
          get_len=True,
      )

    else:
      count = dict()
      df_cohort = read_spark_file(
          intermediate_data_path, intermediate_cohort_filename
      )
      count["Initial"] = df_cohort.count()

      # remove patients not meeting quality checks
      df_cohort = get_patients_in_quality_checks(df_cohort)
      count["Quality checks"] = df_cohort.count()

      for col_to_filter, config in exclusion_config.items():
          df_cohort = get_patients_in_criteria(df_cohort, col_to_filter, config)
          count[col_to_filter] = df_cohort.count()

    if cohort == "control_cohort":
      count["Random sampling"] = (
          read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_cohort").count() * config_sampling
      )

    df_count = pd.DataFrame(
        count, index=[f"{algo_name}_{cohort}"]
    ).T.reset_index()

    return df_count


def compute_stat_demogs(df: pd.DataFrame, conf_demogs: dict) -> pd.DataFrame:
    """Computes demographic statistics on given columns

    Args:
        df (pd.DataFrame): Dataframe with information to compute statistics
        conf_demogs (dict): Configuration information about features relative to demographics

    Returns:
        pd.DataFrame: Basic statistics on demographics
    """
    for key in conf_demogs.keys():
        cols = conf_demogs[key]
        if key == "continuous":
            stats_cont = pd.DataFrame(df[cols].describe())
        elif key == "discrete":
            stats_disc = pd.DataFrame({var: df[var].value_counts() for var in cols})
            stats_disc.loc["count", :] = stats_disc.sum(axis=0)
    stats_all = pd.concat([stats_cont, stats_disc], axis=1).reset_index()
    return stats_all


def create_age_group(
    df: pd.DataFrame, col_age: str, bins: list, labels: list
) -> pd.DataFrame:
    """Divides population in several age groups and counts

    Args:
        df (pd.DataFrame): Dataframe with information about patients and their age
        col_age (str): Column with age information
        bins (list): Bins for age repartition
        labels (list): Labels for age repartition

    Returns:
        pd.DataFrame: Dataframe with counts and percentages per age bin
    """
    df[f"{col_age}_GROUP"] = pd.cut(
        df[col_age], bins=bins, labels=labels, include_lowest=True, right=True
    )
    return df


def get_stats_demogs(
    algo_name: str,
    cohort: str,
    timerange: str = None,
    level: str = None,
    flagged: bool = False,
    all_flagged: bool = False,
) -> pd.DataFrame:
    """Returns statistics on demographic information

    Args:
        algo_name (str): Name of algorithm studied
        cohort (str): Name of cohort of interest
        timerange (str, optional): Which timerange to consult (before_index_date, just_before_index_date, lifetime). Defaults to None, used only for flagged patients.
        level (str, optional): Must be one of rules or block. Defaults to None, used only for flagged patients.
        flagged (bool, optional): Boolean to indicate if the statistics must be performed on flagged patients or not. Defaults to False.
        all_flagged (bool, optional): Boolean to indicate if the statistics must be performed on all the flagged patients at once. Defaults to False.

    Returns:
        pd.DataFrame: Datafrale with demographic statistics.
    """
    LSD_name = algo_name.split('_')[0]
    if not flagged and not all_flagged:
        df_LSD = read_spark_file(
            os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{cohort}"
        )
    if flagged and not all_flagged:
        df_LSD = read_spark_file(
          os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{cohort}_flagged_{timerange}"
        )
    if all_flagged and not flagged:
        df_LSD = read_spark_file(
          os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_all_flagged_cohorts_{timerange}"
        )

    df_LSD = df_LSD.toPandas()
    df_LSD[COL_DATE_OF_DEATH] = pd.to_datetime(df_LSD[COL_DATE_OF_DEATH])
    df_LSD[COL_INDEX_DATE] = pd.to_datetime(df_LSD[COL_INDEX_DATE])

    df_LSD = create_age_group(
        df_LSD, COL_AGE_AT_INDEX_DATE, CONF_AGE_BINS, CONF_AGE_LABELS
    )
    age_groups = pd.DataFrame(
        df_LSD[f"{COL_AGE_AT_INDEX_DATE}_GROUP"].value_counts()
    ).reset_index()
    age_groups_pct = pd.DataFrame(
        df_LSD[f"{COL_AGE_AT_INDEX_DATE}_GROUP"].value_counts() / df_LSD.PTID.nunique()
    ).reset_index()
    age_groups_all = age_groups.merge(age_groups_pct, on="index", how="left")
    age_groups_all = age_groups_all.rename(
        columns={
            "index": "AGE_GROUPS",
            f"{COL_AGE_AT_INDEX_DATE}_GROUP_x": "AGE_GROUPS_COUNT",
            f"{COL_AGE_AT_INDEX_DATE}_GROUP_y": "AGE_GROUPS_PCT",
        }
    )

    df_stats = compute_stat_demogs(df_LSD, CONF_STATS_DEMOG)
    df_stats.loc[0, [COL_DECEASED_INDICATOR, COL_GENDER]] = df_stats.loc[
        0, [COL_DECEASED_INDICATOR, COL_GENDER]
    ].values
    df_stats.loc[1:, [f"{COL_DECEASED_INDICATOR}(%)", f"p_{COL_GENDER}"]] = (
        df_stats.loc[1:, [COL_DECEASED_INDICATOR, COL_GENDER]]
        .apply(lambda x: x / x.sum() * 100, axis=0)
        .values
    )
    df_stats.loc[1:, COL_LOOKBACK_PERIOD] /= 365
    df_stats.loc[1:, COL_ENROLLMENT_DURATION] /= 365

    if cohort == "cohort":
        df_stats.loc[0, "N_CERTAIN_DIAGS_ONLY"] = len(
            df_LSD[(df_LSD["N_CERTAIN_DIAGS"] >= 2) & (df_LSD["N_TREATMENTS"] == 0)]
        )

        df_stats.loc[0, "N_TREATMENTS_ONLY"] = len(
            df_LSD[(df_LSD["N_CERTAIN_DIAGS"] < 2) & (df_LSD["N_TREATMENTS"] > 0)]
        )

        df_stats.loc[0, "N_DIAGS_AND_TREATMENTS"] = len(
            df_LSD[(df_LSD["N_CERTAIN_DIAGS"] >= 2) & (df_LSD["N_TREATMENTS"] > 0)]
        )

    if cohort == "like_cohort":
        main_LSD_cohort = read_spark_file(
            INTERMEDIATE_DATA_PATH, "intermediate_LSD_cohort", pandas_output=True
        )
        df_patients = df_LSD[[COL_UUID, "UNCERTAINTY_REASON"]]
        df_patients_other_certain = df_patients[
            df_patients["UNCERTAINTY_REASON"] == "other certain LSD diagnosis"
        ]
        df_patients_uncertain = df_patients[
            df_patients["UNCERTAINTY_REASON"] == "uncertain LSD diagnosis"
        ]
        df_stats.loc[0, "UNCERTAIN_DIAG"] = len(df_patients_uncertain)
        df_stats.loc[0, "OTHER_CERTAIN_DIAG"] = len(df_patients_other_certain)
        df_stats = df_stats.set_index("index")

        df_main = main_LSD_cohort[(main_LSD_cohort.LSD_GROUP != LSD_name)]
        df_main = df_main[
            df_main.PTID.isin(df_patients_other_certain[COL_UUID].to_list())
        ]

        # keep most prevalent
        df_main["rank"] = (
            df_main.sort_values("N_CERTAIN_DIAGS", ascending=False)
            .groupby([COL_UUID])
            .cumcount()
        )
        df_main = df_main[df_main["rank"] == 0]
        df_repartition = pd.DataFrame(df_main.LSD_GROUP.value_counts())
        for LSD_GROUP in df_repartition.reset_index()["index"]:
          df_stats.loc[LSD_GROUP, "COUNT_OTHER_CERTAIN_DIAG"] = df_repartition.loc[
              LSD_GROUP, "LSD_GROUP"
          ]
        df_stats = df_stats.reset_index()

    return df_stats, age_groups_all


# COMMAND ----------

