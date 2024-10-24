# The HELIOS Diagnostic Library

## Library objectives 

The Helios library is designed to assess and apply medical-driven algorithms tailored to detect patients who are suffering from specific lysosomal storage diseases (LSD).

In particular, it includes:
* [a Russian medical-driven algorithm to detect ASMD patients](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/wiki/Medical‐driven-algorithm-‐-ASMD-Russia)
* [a Canadian medical-driven algorithm to detect Pompe patients](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/wiki/Medical‐driven-algorithm-‐-Pompe-Canada)
* [an Australian medical-driven algorithm to detect Gaucher patients](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/wiki/Medical‐driven-algorithm-‐-Gaucher-Australia)

The library offers two modes of operation:
- An algorithm assessment mode, which enables users to assess the algorithm's performance for a specific data provider.
- A screening mode, which allows users to apply the algorithm on a specific cohort and identify new potential patients.

The library has undergone testing and development on the Optum EHR database, but can be adapted to other data providers if needed.

Please refer to the [wiki](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/wiki) of this repository for more details.

## Library operating modes

### Algorithm assessment

We advice to use:
- `TIMERANGE = "before_index_date"` we you want to look at any clinical characteristics before index date.
- `TIMERANGE = "just_before_index_date"` and to adjust the number of months before index date to look at [here](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/blob/main/config/variables/feature_creation.py#L23). It allows more precise time window and stricter application conditions but have bigger impact on data availability.

We do not advice to use:
- `TIMERANGE = "lifetime"` since events in database can be biased after index date for LSD patients.

By default LSD patients are retrieved if two certains diagnoses or one treatment are found in the database. This rule can be changed by updating the following variables:
- `NB_CERTAIN_DIAGS` [here](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/blob/main/config/algo/global_LSD_conf.py#L23)
- `NB_TREATMENTS` [here](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/blob/main/config/algo/global_LSD_conf.py#L25)
- `LSD_GOAL_STATUSES` [here](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/blob/main/config/algo/global_LSD_conf.py#L29) which defines the diagnosis statuses that you want to use as "certain".

### Screening

For screening, patients are not LSD diagnosed, so there is no index date. However we artificially set the index date as the last event in the database.

Hence, you can either use `TIMERANGE = "lifetime"` or `TIMERANGE = "before_index_date"`, the results will be the same. If you only want to look at some recent events use `TIMERANGE = "just_before_index_date"` instead and adjust the number of months before index date to look at [here](https://github.com/Sanofi-GitHub/RareDisease-GitHub-Helios_diagnostic_library/blob/main/config/variables/feature_creation.py#L23).

