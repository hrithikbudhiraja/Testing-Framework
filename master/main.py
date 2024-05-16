# Databricks notebook source
# install great_expectations
pip install great_expectations

from utilities.config import Config
from utilities.test_functions_pyspark import *
from utilities.test_functions_great_expectations import *

import pyspark.sql.functions as fun

def main():
    # getting configuration details
    config_details = Config('100 CSV records')
    config_details_required = config_details.get_config_details()

    # creating input file
    source_df = config_details.create_source_dataframe(config_details_required)

    # creating output file
    target_df = config_details.create_target_dataframe(config_details_required)

    # pyspark tests
    count_validation(source_df, target_df)
    null_validation(target_df)
    check_duplicates(target_df)

    # great_expectations dataframes
    ge_df = ge.dataset.SparkDFDataset(source_df)
    ge_df = ge.dataset.SparkDFDataset(target_df)

    # great expectation tests
    

main()

# COMMAND ----------


