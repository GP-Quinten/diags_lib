# Databricks notebook source
FABRY_ICD10_CODES = ["E7521"]
FABRY_NDC_CODES = []
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
FABRY_NDC_CODES = [code[:9] for code in FABRY_NDC_CODES]
FABRY_CODES = FABRY_ICD10_CODES + FABRY_NDC_CODES

# COMMAND ----------

NIEMANN_PICK_WITHOUT_ASMD_ICD10_CODES = ["E75242", "E75243", "E75248"] # types C, D and others
NIEMANN_PICK_WITHOUT_ASMD_NDC_CODES = []
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
NIEMANN_PICK_WITHOUT_ASMD_NDC_CODES = [code[:9] for code in NIEMANN_PICK_WITHOUT_ASMD_NDC_CODES]
NIEMANN_PICK_WITHOUT_ASMD_CODES = NIEMANN_PICK_WITHOUT_ASMD_ICD10_CODES + NIEMANN_PICK_WITHOUT_ASMD_NDC_CODES

# COMMAND ----------

GANGLIOSIDOSIS_ICD10_CODES = ["E750", "E7500", "E7501", "E7502", "E7509", "E7519"]
GANGLIOSIDOSIS_NDC_CODES = []
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
GANGLIOSIDOSIS_NDC_CODES = [code[:9] for code in GANGLIOSIDOSIS_NDC_CODES]
GANGLIOSIDOSIS_CODES = GANGLIOSIDOSIS_ICD10_CODES + GANGLIOSIDOSIS_NDC_CODES

# COMMAND ----------

OTHER_LSD_ICD10_CODES = ["E751", "E7510", "E7511", "E7523", "E7526", "E7529", "E753", "E754", "E755", "E756", "E760", "E7601", "E7602", "E7603", "E761", "E7621", "E76210", "E76211", "E76219", "E762", "E7622", "E7629", "E763", "E770", "E771"]
OTHER_LSD_NDC_CODES = []
# ensure NDC codes have 9 digits at maximum since a startswith method is used to identify patients with such treatments
OTHER_LSD_NDC_CODES = [code[:9] for code in OTHER_LSD_NDC_CODES]
OTHER_LSD_CODES = OTHER_LSD_ICD10_CODES + OTHER_LSD_NDC_CODES

# COMMAND ----------

