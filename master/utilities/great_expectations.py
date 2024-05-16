# Databricks notebook source
pip install great_expectations

# COMMAND ----------

import great_expectations as ge

# COMMAND ----------

# MAGIC %run "/Workspace/dq_validation_tests/utilities/config_file"

# COMMAND ----------

# MAGIC %md #### Read the data

# COMMAND ----------

config_input = Config('100 CSV records')
df_input = config_input.create_dataframe()
display(df_input)

# COMMAND ----------

## Get pk field
pk_field = config_input.get_pk_field()
print(pk_field)

# COMMAND ----------

# MAGIC %md #### Convert the spark dataframe to a great expectations dataframe

# COMMAND ----------

ge_df = ge.dataset.SparkDFDataset(df_input)

# COMMAND ----------

# MAGIC %md #### Implementing primary key check

# COMMAND ----------

def primary_key_check(df):
    pk_field = 'Order ID'
    df.expect_column_values_to_be_unique(pk_field)
    df.expect_column_values_to_not_be_null(pk_field)
    df.get_expectation_suite()
    df.save_expectation_suite('order.data.expectations.json')
    test_results = df.validate(expectation_suite="order.data.expectations.json")
    return test_results

primary_key_check(ge_df)

# COMMAND ----------

## Checking if compound columns (combination of different columns) are unique

ge_df.expect_compound_columns_to_be_unique(['Country','Order ID'])

# COMMAND ----------

import pandas as pd

# COMMAND ----------

#Checking if the data types are matching
def check_data_types(df, data_types):
    results = {}

    ge_df = ge.from_pandas(df)

    # Iterate over the specified data types dictionary
    for column, dtype in data_types.items():
        if dtype == 'int':
            result = ge_df.expect_column_values_to_be_of_type(column, 'int')
        elif dtype == 'float':
            result = ge_df.expect_column_values_to_be_of_type(column, 'float')
        elif dtype == 'str':
            result = ge_df.expect_column_values_to_be_of_type(column, 'str')
        elif dtype == 'bool':
            result = ge_df.expect_column_values_to_be_of_type(column, 'boolean')
        else:
            raise ValueError(f"Unsupported data type specified: {dtype}")
        
        # Store the success flag for each column
        results[column] = result.success
 
    return results

# COMMAND ----------

    data = {
        'ID': [1, 2, 3, 4],               # Expected int
        'Temperature': [20.5, 22.1, 21.6, 19.9],  # Expected float
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana'], # Expected str
        'Active': [True, False, True, False] # Expected bool
    }
    df_1 = pd.DataFrame(data)
    
    # Expected data types for each column
    expected_types = {
        'ID': 'int',
        'Temperature': 'float',
        'Name': 'str',
        'Active': 'bool'
    }
    
    type_check_results = check_data_types(df_1, expected_types)
    print(type_check_results)

# COMMAND ----------

# MAGIC %md #### Useful expectations

# COMMAND ----------

# if a particular column exists or not
ge_df.expect_column_to_exist("Country")

# COMMAND ----------

from pyspark.sql.types import DoubleType

## Creating a temp df to change the datatype of Unit Price column from string to double type
df_cast_temp = df_input.withColumn("Unit Price", df_input["Unit Price"].cast(DoubleType()))

# COMMAND ----------

## creating temporary great expectations dataframe
ge_df_cast_temp = ge.dataset.SparkDFDataset(df_cast_temp)

## checking if the max column value for the column is between 1 and 1000
ge_df_cast_temp.expect_column_max_to_be_between("Unit Price", 1, 1000)

# COMMAND ----------

## checking if the length of clumn values equals the provided value
ge_df.expect_column_value_lengths_to_equal("Order ID", 9)

# COMMAND ----------

## Checks if all column values are in the set provided
ge_df.expect_column_values_to_be_in_set("Country", ["Australia", "Iceland"])

# COMMAND ----------

## Checks if the combination of columns is unique
ge_df.expect_select_column_values_to_be_unique_within_record(["Country", "Region"])

# COMMAND ----------

# MAGIC %md #### Duplicate check

# COMMAND ----------

## Check for duplicate rows in the table
def check_for_duplicates(df, column_list=None):
 
    results = {}
 
    if column_list is None:
        # Check for full row duplicates if no column list is provided
        # Creating a hash of each row to simulate an expectation for row uniqueness
        df['row_hash'] = df.apply(lambda row: hash(tuple(row)), axis=1)
        result = df.expect_column_values_to_be_unique(column='row_hash')
        results['full_row'] = result.success
        df.drop('row_hash', axis=1, inplace=True)  # Clean up temporary hash column
    else:
        # Check each specified column for unique values
        for column in column_list:
            result = df.expect_column_values_to_be_unique(column)
            results[column] = result
 
    return results
 
duplicate_check_results = check_for_duplicates(ge_df, ['Order ID', 'Country'])
print(duplicate_check_results)
