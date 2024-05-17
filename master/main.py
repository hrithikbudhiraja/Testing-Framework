# Databricks notebook source
# MAGIC %run "/Workspace/Repos/hrithik@sagar1001.onmicrosoft.com/Testing-Framework/master/utilities/config_file"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/hrithik@sagar1001.onmicrosoft.com/Testing-Framework/master/utilities/test_functions_pyspark"

# COMMAND ----------

import pyspark.sql.functions as fun

def main():
    # getting configuration details
    config_details = Config('100 CSV records')
    config_details_required = config_details.get_config_details()

    # creating input file
    source_df = config_details.create_source_dataframe(config_details_required)

    # creating output file
    target_df = config_details.create_target_dataframe(config_details_required)

    display(target_df)

    pk_field = config_details.get_pk_field(config_details_required)
    print(pk_field)

    # pyspark tests
    count_validation(source_df, target_df)
    null_validation(source_df, target_df)
    duplicate_check(source_df, target_df)

main()

# COMMAND ----------

config_details = Config('100 CSV records')
config_details_required = config_details.get_config_details()
source_df = config_details.create_source_dataframe(config_details_required)
target_df = config_details.create_target_dataframe(config_details_required)
pk_field = config_details.get_pk_field(config_details_required)
pk_field
