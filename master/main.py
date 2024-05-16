# Databricks notebook source
# MAGIC %md #### Read the file stored in a storage account
# MAGIC * Requires storage_account_name and storage_account_access_key
# MAGIC * create a csv file path using the above parameters
# MAGIC * create a spark dataframe

# COMMAND ----------

# MAGIC %run "/Workspace/dq_validation_tests/utilities/config_file"

# COMMAND ----------

# MAGIC %run "/Workspace/dq_validation_tests/utilities/test_functions"

# COMMAND ----------

import pyspark.sql.functions as fun
def main():
    ## creating input file
    config_input = Config('100 CSV records')
    df_input = config_input.create_dataframe()
    pk_field_input = config_input.get_pk_field()

    ## creating output file
    config_output = Config('output_copy')
    df_output = config_output.create_dataframe()
    pk_field_output = config_output.get_pk_field()
    df_output_new = df = df_output.drop('Order Priority') ## Used to get mismatches in comparision test

    ## tests are called
    null_validation(df_output)
    check_duplicates(df_output)
    duplicate_check_on_specific_column(df_output, pk_field_output)
    duplicate_check_on_specific_column(df_output, "Item Type")
    compare_schemas_and_write_mismatches_to_table(df_input, df_output_new)
    primary_key_check(df_output,[pk_field_output])
main()

# COMMAND ----------


