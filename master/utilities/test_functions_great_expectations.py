# Databricks notebook source
# MAGIC %md #### Convert the spark dataframe to a great expectations dataframe

# COMMAND ----------

pip install great_expectations

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/hrithik@sagar1001.onmicrosoft.com/Testing-Framework/master/utilities/config_file"

# COMMAND ----------

import great_expectations as ge

# getting configuration details
config_details = Config('100 CSV records')
config_details_required = config_details.get_config_details()

# creating input file
source_df = config_details.create_source_dataframe(config_details_required)

# creating output file
target_df = config_details.create_target_dataframe(config_details_required)

pk_field = config_details.get_pk_field(config_details_required)

source_ge_df = ge.dataset.SparkDFDataset(source_df)
target_ge_df = ge.dataset.SparkDFDataset(target_df)





# COMMAND ----------

def count_validation_ge(source_df, source_ge_df, target_ge_df):
    source_df_count = source_df.count()
    target_ge_df.expect_table_row_count_to_equal(10)
    target_ge_df.save_expectation_suite('order.data.expectations.json')
    test_results = target_ge_df.validate(expectation_suite="order.data.expectations.json")
    return test_results

def primary_key_check_ge(df, pk_field):
    df.expect_column_values_to_be_unique(pk_field)
    df.expect_column_values_to_not_be_null(pk_field)
    df.get_expectation_suite()
    df.save_expectation_suite('order.data.expectations.json')
    test_results = df.validate(expectation_suite="order.data.expectations.json")
    return test_results

primary_key_check_ge(source_ge_df, pk_field)

# COMMAND ----------

from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

context_root_dir = ":/dbfs/great_expectations/"

# COMMAND ----------

context = ge.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

dataframe_datasource = context.sources.add_or_update_spark(
    name="testing_data_asset",
)

# COMMAND ----------

dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="data",
    dataframe = source_df,
)

# COMMAND ----------

batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

expectation_suite_name = "tests"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

source_df

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="Order ID")

# COMMAND ----------

validator.expect_column_to_exist(
    column="Order ID"
)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "testing_checkpoint"
expectation_suite_name = "tests"
checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)

# COMMAND ----------

context.add_or_update_checkpoint(checkpoint=checkpoint)

# COMMAND ----------

checkpoint_result = checkpoint.run()
checkpoint_result

# COMMAND ----------

print(checkpoint.get_config().to_yaml_str())

# COMMAND ----------

from great_expectations import ExpectationSuite, expect_column_values_to_be_equal

suite = ExpectationSuite("compare_source_target_data")

expectation = expect_column_values_to_be_equal(
    source_table = source_ge_df,
    source_column = pk_field,
    target_table = target_ge_df,
    target_column = pk_field
)

suite.add_expectation(expectation)

# COMMAND ----------

# #Checking if the data types are matching
# def check_data_types(df, data_types):
#     results = {}

#     ge_df = ge.from_pandas(df)

#     # Iterate over the specified data types dictionary
#     for column, dtype in data_types.items():
#         if dtype == 'int':
#             result = ge_df.expect_column_values_to_be_of_type(column, 'int')
#         elif dtype == 'float':
#             result = ge_df.expect_column_values_to_be_of_type(column, 'float')
#         elif dtype == 'str':
#             result = ge_df.expect_column_values_to_be_of_type(column, 'str')
#         elif dtype == 'bool':
#             result = ge_df.expect_column_values_to_be_of_type(column, 'boolean')
#         else:
#             raise ValueError(f"Unsupported data type specified: {dtype}")
        
#         # Store the success flag for each column
#         results[column] = result.success
 
#     return results

# COMMAND ----------

    # data = {
    #     'ID': [1, 2, 3, 4],  # Expected int
    #     'Temperature': [20.5, 22.1, 21.6, 19.9],  # Expected float
    #     'Name': ['Alice', 'Bob', 'Charlie', 'Diana'], # Expected str
    #     'Active': [True, False, True, False] # Expected bool
    # }
    # df_1 = pd.DataFrame(data)
    
    # # Expected data types for each column
    # expected_types = {
    #     'ID': 'int',
    #     'Temperature': 'float',
    #     'Name': 'str',
    #     'Active': 'bool'
    # }
    
    # type_check_results = check_data_types(df_1, expected_types)
    # print(type_check_results)

# COMMAND ----------

# # if a particular column exists or not
# ge_df.expect_column_to_exist("Country")

# COMMAND ----------

# from pyspark.sql.types import DoubleType

# ## Creating a temp df to change the datatype of Unit Price column from string to double type
# df_cast_temp = df_input.withColumn("Unit Price", df_input["Unit Price"].cast(DoubleType()))

# COMMAND ----------

# ## creating temporary great expectations dataframe
# ge_df_cast_temp = ge.dataset.SparkDFDataset(df_cast_temp)

# ## checking if the max column value for the column is between 1 and 1000
# ge_df_cast_temp.expect_column_max_to_be_between("Unit Price", 1, 1000)

# COMMAND ----------

# ## checking if the length of clumn values equals the provided value
# ge_df.expect_column_value_lengths_to_equal("Order ID", 9)

# COMMAND ----------

# ## Checks if all column values are in the set provided
# ge_df.expect_column_values_to_be_in_set("Country", ["Australia", "Iceland"])

# COMMAND ----------

# ## Checks if the combination of columns is unique
# ge_df.expect_select_column_values_to_be_unique_within_record(["Country", "Region"])
