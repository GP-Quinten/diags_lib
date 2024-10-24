# Databricks notebook source
import os
import pyspark
import itertools
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from scipy.stats import mannwhitneyu, shapiro, ttest_ind

# COMMAND ----------

def compute_generic_stats_on_labs(
    algo_name: str,
    cohort: str,
    level: str,
    timerange: str,
    flagged: bool = False,
    all_flagged: bool = False,
) -> pd.DataFrame:
    """Computes generic statistics (mean, quartiles, number of lab per patient...) on each lab for a given LSD and cohort.

    Args:
        algo_name (str) : Name of algorithm studied
        cohort (str) : Type of cohort studied. If all_flagged = True, cohort must be a list of the cohorts combined in all flagged.
        patient_uuid (str): Unique descriptor of patients
        level (str): Per rule or per block (several blocks per rules)
        timerange (str): Default = "before_index_date". Can be "lifetime" or "just_before_index_date" as wel
        flagged (bool): Default = False. If True, computes statistics on each flagged cohort
        all_flagged (bool): Default = False. If True, computes statistics on all flagged patients without any cohort distinction

    Returns:
        pd.DataFrame: Returns a dataframe with one row per lab and the following statistics :
            - test name
            - unit of test
            - number of patients having the lab at least once
            - percentage of patents having the lab at least once
            - mean, std, median, Q1, Q3 and median of number of tests per patient
            - mean and std value of test
            - first quartile, median and third quartile value of test
            - min and max value of test
        all statistics are performed on the test of reference ie the closest test to index date

    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    patient_uuid = labs_config["COL_UUID"]

    if not all_flagged:
        df_features = read_spark_file(
            os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
            f"{algo_name}_{cohort}_combined_labs_{timerange}",
        )

    if flagged and not all_flagged:
        # Filter patients that have been flagged by the algorithm
        tmp_cohort = read_spark_file(
            os.path.join(COHORT_DATA_PATH, algo_name),
            f"{algo_name}_{cohort}_flagged_{timerange}",
        )
        prediction = (
            tmp_cohort.select(patient_uuid)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        df_features = df_features.where(F.col(patient_uuid).isin(prediction))
        nb_ptids = len(prediction)

    if all_flagged and not flagged:
        features_df_list = []
        for cohort_type in cohort:
            df_features = read_spark_file(
                os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
                f"{algo_name}_{cohort_type}_combined_labs_{timerange}",
            )
            features_df_list.append(df_features)

        df_features = features_df_list[0]
        for df in features_df_list[1:]:
            df_features = df_features.unionAll(df)

        # Filter patients that have been flagged by the algorithm
        tmp_cohort = read_spark_file(
            os.path.join(COHORT_DATA_PATH, algo_name),
            f"{algo_name}_all_flagged_cohorts_{timerange}",
        )
        prediction = (
            tmp_cohort.select(patient_uuid)
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        df_features = df_features.where(
            F.col(patient_uuid).isin(prediction)
        )

        nb_ptids = len(prediction)

    if not flagged and not all_flagged:
        nb_ptids = len(
            read_spark_file(os.path.join(COHORT_DATA_PATH, algo_name), f"{algo_name}_{cohort}")
            .select(patient_uuid)
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    # Compute statistics
    df_features_exp = df_features.select(
        patient_uuid, F.explode("TEST_RESULTS_IN_WINDOW")
    )

    df_features_exp_grouped = df_features_exp.groupBy(patient_uuid).count()

    df_features_exp_stats = df_features_exp_grouped.agg(
        F.expr("mean(count)").alias("mean"),
        F.expr("stddev(count)").alias("std"),
        F.expr("percentile(count, 0.25)").alias("Q1"),
        F.expr("percentile(count, 0.5)").alias("median"),
        F.expr("percentile(count, 0.75)").alias("Q3"),
    )

    mean_N_test_per_patient = df_features_exp_stats.select("mean").collect()[0][0]
    std_N_test_per_patient = df_features_exp_stats.select("std").collect()[0][0]
    median_N_test_per_patient = df_features_exp_stats.select("median").collect()[0][0]
    q1_N_test_per_patient = df_features_exp_stats.select("Q1").collect()[0][0]
    q3_N_test_per_patient = df_features_exp_stats.select("Q3").collect()[0][0]
    IQ_N_test_per_patient = q3_N_test_per_patient - q1_N_test_per_patient

    df_all_tests = pd.DataFrame()

    df_all_tests.loc[0, "Measures per patients"] = df_features_exp_grouped.count()
    df_all_tests.loc[0, "Measures per patients (%)"] = df_features_exp_grouped.count() / nb_ptids
    df_all_tests.loc[0, "Mean measures per patients"] = mean_N_test_per_patient
    df_all_tests.loc[0, "Std measures per patients"] = std_N_test_per_patient
    df_all_tests.loc[0, "Median measures per patients"] = median_N_test_per_patient
    df_all_tests.loc[0, "Q1 measures per patients"] = q1_N_test_per_patient
    df_all_tests.loc[0, "Q3 measures per patients"] = q3_N_test_per_patient
    df_all_tests.loc[0, "IQ measures per patients"] = IQ_N_test_per_patient

    labs = (
        df_features.select(labs_config["COL_INFO"])
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    df_labs = pd.DataFrame()
    for lab in labs:
        df_lab = df_features.filter(F.col(labs_config["COL_INFO"]) == lab)

        df_labs.loc[lab, "Test name"] = lab
        df_labs.loc[lab, "Unit"] = (
            df_lab.select(labs_config["COL_UNIT"]).distinct().collect()[0][0]
        )

        df_labs.loc[lab, "Number of patients with at least one measure"] = df_lab.select(patient_uuid).distinct().count()
        df_labs.loc[lab, "Percentage of patients with at least one measure"] = (
            df_lab.select(patient_uuid).distinct().count() / nb_ptids
        )

        df_labs_count = (
            df_lab.select(
                [patient_uuid, F.explode("TEST_RESULTS_IN_WINDOW").alias("all_tests")]
            )
            .groupBy(patient_uuid)
            .count()
        )

        df_labs.loc[lab, "Mean measures per patients"] = (
            df_labs_count.select("count").agg({"count": "mean"}).collect()[0][0]
        )

        df_labs.loc[lab, "Std measures per patient"] = (
            df_labs_count.select("count").agg({"count": "stddev"}).collect()[0][0]
        )

        df_labs.loc[lab, "Max measures per patient"] = (
            df_labs_count.select("count").agg({"count": "max"}).collect()[0][0]
        )

        df_labs.loc[lab, "Median measures per patient"] = (
            df_labs_count.select("count")
            .agg(F.expr("approx_percentile(count, 0.5)"))
            .collect()[0][0]
        )

        df_labs.loc[lab, "Q1 measures per patient"] = (
            df_labs_count.select("count")
            .agg(F.expr("approx_percentile(count, 0.25)"))
            .collect()[0][0]
        )

        df_labs.loc[lab, "Q3 measures per patient"] = (
            df_labs_count.select("count")
            .agg(F.expr("approx_percentile(count, 0.75)"))
            .collect()[0][0]
        )

        df_labs.loc[lab, "IQ measures per patient"] = (
            df_labs.loc[lab, "Q3 measures per patient"] - df_labs.loc[lab, "Q1 measures per patient"]
        )

        df_labs.loc[lab, "Mean value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg({"TEST_RESULT_REF": "mean"})
            .collect()[0][0]
        )
        df_labs.loc[lab, "Std value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg({"TEST_RESULT_REF": "stddev"})
            .collect()[0][0]
        )
        df_labs.loc[lab, "Min value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg({"TEST_RESULT_REF": "min"})
            .collect()[0][0]
        )
        df_labs.loc[lab, "q1 value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg(F.expr("approx_percentile(TEST_RESULT_REF, 0.25)"))
            .collect()[0][0]
        )
        df_labs.loc[lab, "Median value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg(F.expr("approx_percentile(TEST_RESULT_REF, 0.5)"))
            .collect()[0][0]
        )
        df_labs.loc[lab, "q3 value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg(F.expr("approx_percentile(TEST_RESULT_REF, 0.75)"))
            .collect()[0][0]
        )
        df_labs.loc[lab, "Max value"] = (
            df_lab.select("TEST_RESULT_REF")
            .agg({"TEST_RESULT_REF": "max"})
            .collect()[0][0]
        )

    return df_all_tests, df_labs


def Labs_mannwhitneyu_test(Labs_cohort: pd.DataFrame, balanced_samples: bool = False):
    """
    The Mann-Whitney U test is a nonparametric test of the null hypothesis that the distribution underlying sample x is the same as the          distribution underlying sample y. It is often used as a test of of difference in location between distributions.
    Retrieves a table with test outcomes (statistic and pvalue) for each couple (outcome | TEST_NAME)

    Args:
        Labs_cohort (pd.DataFrame) : 1 row / ( outcome | TEST_NAME | PTID )
        balanced_samples (bool) : Wether or not to equilibrate populations during statistical test. Default to False.
    Returns:
        table 1 row /  ( outcome | statistic_test | TEST_NAME | statistic | p_value )
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]

    df = Labs_cohort.select("LABEL", labs_config["COL_INFO"], "TEST_RESULT_REF")
    df = df.toPandas()

    result_data = {}
    analysis_group = df.groupby(labs_config["COL_INFO"])
    row_nb = 0
    for idx, df in analysis_group:

        # get data to test
        if balanced_samples:
            df_neg = df.query("LABEL == 0")
            df_pos = df.query("LABEL == 1")
            x = (
                df_neg[["TEST_RESULT_REF"]]
                .sample(n=min(len(df_neg), len(df_pos)), random_state=123)
                .values
            )
            y = (
                df_pos[["TEST_RESULT_REF"]]
                .sample(n=min(len(df_neg), len(df_pos)), random_state=123)
                .values
            )
        else:
            x = df.query("LABEL == 0")[["TEST_RESULT_REF"]].values
            y = df.query("LABEL == 1")[["TEST_RESULT_REF"]].values

        # mannwhitneyu test
        test = mannwhitneyu(x, y, alternative="two-sided")

        # add results to dictionary
        result_data[row_nb] = ["mannwhitneyu", idx, test.statistic[0], test.pvalue[0]]
        row_nb += 1
    df_result = pd.DataFrame.from_dict(
        data=result_data,
        orient="index",
        columns=["statistic_test", labs_config["COL_INFO"], "statistic", "pvalue"],
    )

    mySchema = StructType(
        [
            StructField("statistic_test", StringType(), True),
            StructField(labs_config["COL_INFO"], StringType(), True),
            StructField("statistic", FloatType(), True),
            StructField("pvalue", FloatType(), True),
        ]
    )

    df = spark.createDataFrame(df_result, schema=mySchema)
    return df


def Labs_shapiro_test(Labs_cohort: pd.DataFrame):
    """
    The Shapiro-Wilk test tests the null hypothesis that the data was drawn from a normal distribution.
    Retrieves a table with test outcomes (statistic and pvalue) for each couple (outcome | TEST_NAME)
    Args:
        Labs_cohort 1 row / ( outcome | TEST_NAME | PTID )
    Returns:
        table 1 row /  ( outcome | TEST_NAME | statistic | p_value )
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df = Labs_cohort.select("LABEL", labs_config["COL_INFO"], "TEST_RESULT_REF")
    df = df.toPandas()

    df_results = pd.DataFrame()
    result_data = {}
    analysis_group = df.groupby(["LABEL", labs_config["COL_INFO"]])
    row_nb = 0
    for idx, df in analysis_group:

        # get data to test
        x = df[["TEST_RESULT_REF"]].values

        # normality test (shapiro test)
        shapiro_test = shapiro(x)

        # add results to dictionary
        result_data[row_nb] = [
            idx[0],
            idx[1],
            shapiro_test.statistic,
            shapiro_test.pvalue,
        ]
        row_nb += 1
    df_result = pd.DataFrame.from_dict(
        data=result_data,
        orient="index",
        columns=["LABEL", labs_config["COL_INFO"], "statistic", "pvalue"],
    )

    mySchema = StructType(
        [
            StructField("LABEL", IntegerType(), True),
            StructField(labs_config["COL_INFO"], StringType(), True),
            StructField("statistic", FloatType(), True),
            StructField("pvalue", FloatType(), True),
        ]
    )

    df = spark.createDataFrame(df_result, schema=mySchema)
    return df


def Labs_shapiro_test_pvalue_filt_outcome(
    Labs_shapiro_test: pd.DataFrame, alpha_shapiro=0.05
):
    """
    Retrieves a table with labs flagged:
        shapiro = 1 : pvalues of both outcomes are under a level alpha_shapiro
        shapiro = 0: at least 1 outcome's pvalue is above a level alpha_shapiro
    In a first place are flagged couple's (outcome, TEST_NAME), then TEST_NAME
    Args:
        Labs_shapiro_test 1 row /  ( outcome | TEST_NAME | statistic | p_value )
    Returns:
        1 row /  ( TEST_NAME | outcome |statistic | p_value | shapiro_outcome | shapiro )
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df = Labs_shapiro_test

    df = df.withColumn(
        "shapiro_outcome",
        F.when((df.pvalue < alpha_shapiro), F.lit(0)).otherwise(F.lit(1)),
    )

    window = Window.partitionBy(labs_config["COL_INFO"])
    df = df.withColumn("shapiro", F.min(F.col("shapiro_outcome")).over(window))
    return df


def Labs_ttest_ind(Labs_cohort: pd.DataFrame):
    """
    This is a two-sided test for the null hypothesis that 2 independent samples have identical average (expected) values. This test assumes      that the populations have identical variances by default.
    Retrieves a table with test outcomes (statistic and pvalue) for each couple (outcome | TEST_NAME)
    Args:
        Labs_cohort 1 row / ( outcome | TEST_NAME | PTID )
    Returns:
        table 1 row /  ( outcome | statistic_test | TEST_NAME | statistic | p_value )
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df = Labs_cohort.select("LABEL", labs_config["COL_INFO"], "TEST_RESULT_REF")
    df = df.toPandas()

    result_data = {}
    analysis_group = df.groupby(labs_config["COL_INFO"])
    row_nb = 0
    for idx, df in analysis_group:

        # get data to test
        x = df.query("LABEL == 0")[["TEST_RESULT_REF"]].values
        y = df.query("LABEL == 1")[["TEST_RESULT_REF"]].values

        # ttest test
        test = ttest_ind(x, y, alternative="two-sided")
        # add results to dictionary
        result_data[row_nb] = ["ttest_ind", idx, test.statistic[0], test.pvalue[0]]
        row_nb += 1
    df_result = pd.DataFrame.from_dict(
        data=result_data,
        orient="index",
        columns=["statistic_test", labs_config["COL_INFO"], "statistic", "pvalue"],
    )

    mySchema = StructType(
        [
            StructField("statistic_test", StringType(), True),
            StructField(labs_config["COL_INFO"], StringType(), True),
            StructField("statistic", FloatType(), True),
            StructField("pvalue", FloatType(), True),
        ]
    )

    df = spark.createDataFrame(df_result, schema=mySchema)
    return df


def Labs_significance_test(
    Labs_mannwhitneyu_test: pd.DataFrame,
    Labs_ttest_ind: pd.DataFrame,
    Labs_shapiro_test_pvalue_filt_outcome: pd.DataFrame,
    alpha=0.05,
):
    """
    Retrieves a table with signigicance test.
    If sample passes shapiro test at a level of alpha, then significance test is chosen as mannwhitneyu, else ttest.
    Args:
        Labs_mannwhitneyu_test 1 row /  ( outcome | statistic_test | TEST_NAME | statistic | p_value )
        Labs_ttest_ind 1 row /  ( outcome | statistic_test | TEST_NAME | statistic | p_value )
        Labs_shapiro_test_pvalue_filt_outcome  1 row /  ( TEST_NAME | outcome |statistic | p_value | shapiro_outcome | shapiro )
    Returns:
        table 1 row / (TEST_NAME | statistic_test | statistic | pvalue | shapiro )

    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df_shapiro = Labs_shapiro_test_pvalue_filt_outcome.select(
        labs_config["COL_INFO"], "shapiro"
    ).distinct()
    df_mannwhitneyu = Labs_mannwhitneyu_test
    df_ttest = Labs_ttest_ind
    df = df_mannwhitneyu.unionByName(df_ttest)
    df = df.join(df_shapiro, on=labs_config["COL_INFO"], how="inner")

    df = df.withColumn(
        "significance_test",
        F.when((df.shapiro == 1), F.lit("ttest")).otherwise(F.lit("mannwhitneyu")),
    )
    df = df.filter(F.col("statistic_test") == F.col("significance_test"))
    df = df.drop("significance_test")

    df_alpha = df.withColumn(
        "significance_outcome",
        F.when((df.pvalue >= alpha), F.lit(0)).otherwise(F.lit(1)),
    )

    return df


def get_stats_tests_on_labs(
    algo_name: str, comparaison_tuple: tuple, BALANCED_SAMPLES: bool, timerange: str
) -> pd.DataFrame:
    """Computes statistical tests on the labs of 2 types of cohorts (shapiro, mann whitney U, ttest).

    Args:
        algo_name (str) : Name of algorithm studied
        comparaison_tuple (tuple): types of cohorts compared
        BALANCED_SAMPLES (bool): Wether or not to equilibrate populations during statistical test
        timerange (str): Must be one of ["before_index_date","just_before_index_date","lifetime"]

    Returns:
        pd.DataFrame: Dataframe with type of test practiced, its value, the pvalue associated.
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]

    tmp_pop1 = read_spark_file(
        os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
        f"{algo_name}_{comparaison_tuple[0]}_combined_labs_{timerange}",
    ).select([labs_config["COL_UUID"], labs_config["COL_INFO"], "TEST_RESULT_REF"])

    tmp_pop2 = read_spark_file(
        os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
        f"{algo_name}_{comparaison_tuple[1]}_combined_labs_{timerange}",
    ).select([labs_config["COL_UUID"], labs_config["COL_INFO"], "TEST_RESULT_REF"])

    tmp_pop1 = tmp_pop1.withColumn("LABEL", F.lit(1))
    tmp_pop2 = tmp_pop2.withColumn("LABEL", F.lit(0))

    pop1VSpop2 = tmp_pop1.union(tmp_pop2)
    df_labs_mannwhitneyu_test = Labs_mannwhitneyu_test(
        pop1VSpop2, balanced_samples=BALANCED_SAMPLES
    )

    df_labs_shapiro_test = Labs_shapiro_test(pop1VSpop2)

    df_labs_shapiro_test_pvalue_filt_outcome = Labs_shapiro_test_pvalue_filt_outcome(
        df_labs_shapiro_test
    )
    df_labs_ttest_ind = Labs_ttest_ind(pop1VSpop2)

    # tests aggregation
    df_labs_significance_test = Labs_significance_test(
        Labs_mannwhitneyu_test=df_labs_mannwhitneyu_test,
        Labs_ttest_ind=df_labs_ttest_ind,
        Labs_shapiro_test_pvalue_filt_outcome=df_labs_shapiro_test_pvalue_filt_outcome,
    )
    return df_labs_significance_test


def get_labs_availability(algo_name: str, cohorts: list, timeranges: list) -> pd.DataFrame:
    """Gets availability (number of patients with enough data to go through) for a given LSD an all its cohorts

    Args:
        algo_name (str): Name of al studied
        cohorts (list): List of cohorts to study
        timeranges (list): List of timeranges of interest

    Returns:
        pd.DataFrame: Dataframe with statistics on availability for each cohort and timerange
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df_stats = pd.DataFrame(data=None)
    for cohort in cohorts:
        df_stats_tmp = pd.DataFrame(data=None)
        for timerange in timeranges:
            df_combined = read_spark_file(
                os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
                f"{algo_name}_{cohort}_labs_feature_matrix_{timerange}",
            )
            n_cohort = df_combined.count()
            df_agg = df_combined.agg(
                *[
                    F.count(F.when(~F.isnull(c), c)).alias(c)
                    for c in df_combined.columns
                ]
            )
            n_labs = df_agg.toPandas().to_dict(orient="records")[0]
            n_labs.pop(labs_config["COL_UUID"])
            for lab, value in n_labs.items():
                df_stats_tmp.loc[
                    f"algorithm_constraints_{timerange}", lab
                ] = f"{value} ({round(value/n_cohort*100, 1)}%)"
        df_stats_tmp = df_stats_tmp.reset_index().rename(columns={"index": "method"})
        df_stats_tmp.insert(0, "cohort_type", cohort)
        df_stats = pd.concat([df_stats, df_stats_tmp])
    return df_stats


def get_labs_flagged(
    algo_name: str, cohorts: list, level: str, timeranges: list, flagged=False
) -> pd.DataFrame:
    """Returns the amount of labs outside of threshold for a given LSD

    Args:
        algo_name (str): Name of algorithm studied
        cohorts (list): List of cohort types to study
        level (str): Per rule or per block (several blocks per rules)
        timeranges (list): List of timeranges of interest
        flagged (bool, optional): Wether or not to look at only the flagged patients. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with N / % of people going outside threshold for each lab of the algorithm
    """
    labs_config = DATABASES_FEATURE_CONFIG["labs"]
    df_stats = pd.DataFrame(data=None)
    for cohort in cohorts:
        df_stats_tmp = pd.DataFrame(data=None)
        for timerange in timeranges:
          df_combined = read_spark_file(
            os.path.join(FEATURE_DATA_PATH, algo_name, "labs"),
            f"{algo_name}_{cohort}_labs_feature_matrix_{timerange}",
          )
          if flagged:
              # Filter on flagged patients
              tmp_cohort = read_spark_file(
              os.path.join(ALGO_OUTPUT_DATA_PATH, algo_name),
              f"{algo_name}_{cohort}_final_labels_{timerange}_per_{level}",
            )
              prediction = (
                  tmp_cohort.filter(F.col("global_label") == True)
                  .select(labs_config["COL_UUID"])
                  .rdd.flatMap(lambda x: x)
                  .collect()
              )
              print(len(prediction))

              df_combined = df_combined.where(
                  F.col(labs_config["COL_UUID"]).isin(prediction)
              )

          n_cohort = df_combined.count()
          df_agg_not_null = df_combined.agg(
              *[
                  F.count(F.when(~F.isnull(c), c)).alias(c)
                  for c in df_combined.columns
              ]
          )
          n_labs_not_null = df_agg_not_null.toPandas().to_dict(orient="records")[0]
          n_labs_not_null.pop(labs_config["COL_UUID"])
          df_agg_positive = df_combined.agg(
              *[
                  F.count(F.when(F.col(c) == True, c)).alias(c)
                  for c in df_combined.columns
              ]
          )
          n_labs_positive = df_agg_positive.toPandas().to_dict(orient="records")[0]
          n_labs_positive.pop(labs_config["COL_UUID"])
          for lab, value in n_labs_positive.items():
              if flagged:
                  df_stats_tmp.loc[
                      f"algorithm_constraints_{timerange}", lab
                  ] = f"{value} ({round(value/n_cohort*100, 1)}%)"
              else:
                  df_stats_tmp.loc[
                      f"algorithm_constraints_{timerange}", lab
                  ] = f"{value} ({round(value/n_labs_not_null[lab]*100, 1)}%)"
        df_stats_tmp = df_stats_tmp.reset_index().rename(columns={"index": "method"})
        df_stats_tmp.insert(0, "cohort_type", cohort)
        df_stats = pd.concat([df_stats, df_stats_tmp])
    return df_stats
