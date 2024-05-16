# Databricks notebook source
from utilities.config import Config
from utilities.test_function_pyspark import *

import pyspark.sql.functions as fun

def main():
    # creating input file
    config_input = Config('100 CSV records')
    df_input = config_input.create_dataframe()
    pk_field_input = config_input.get_pk_field()

    # creating output file
    config_output = Config('output_copy')
    df_output = config_output.create_dataframe()
    pk_field_output = config_output.get_pk_field()
    df_output_new = df = df_output.drop('Order Priority') ## Used to get mismatches in comparision test

    # tests are called
    count_validation(source_df, target_df)
    null_validation(df_output)
    check_duplicates(df_output)
main()

# COMMAND ----------


