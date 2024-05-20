# Databricks notebook source
# MAGIC %run "/Workspace/Repos/hrithik@sagar1001.onmicrosoft.com/Testing-Framework/master/utilities/config_file"

# COMMAND ----------

import great_expectations as ge
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

# getting configuration details
config_details = Config('orders_input')
config_details_required = config_details.get_config_details()

# creating input file
source_spark_df = config_details.create_source_dataframe(config_details_required)
source_df = source_spark_df.toPandas()

# creating output file
target_spark_df = config_details.create_target_dataframe(config_details_required)
target_df = target_spark_df.toPandas()

pk_field = config_details.get_pk_field(config_details_required)

# Creating great expectations dataframe
source_ge_df = ge.from_pandas(source_df)
target_ge_df = ge.from_pandas(target_df)


# COMMAND ----------

context_root_dir = ":/dbfs/great_expectations/"

# COMMAND ----------

context = ge.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

datasource = context.sources.add_pandas(name="testing_data_asset_pandas")

# COMMAND ----------

data_asset = datasource.add_dataframe_asset(name= "data")

# COMMAND ----------

batch_request = data_asset.build_batch_request(dataframe = source_df)

# COMMAND ----------

# creating a validator
expectation_suite_name = "ge_tests_expectation"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name = expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

# Check if column exists or not
validator.expect_column_to_exist(column="Order ID")

# COMMAND ----------

# Check if column is not null
validator.expect_column_values_to_not_be_null(column="Order ID")

# COMMAND ----------

# validate count between source and target tables
source_count = source_df.shape[0]
validator.expect_table_row_count_to_equal(source_count)

# COMMAND ----------

# to check if there are duplicates in a column
validator.expect_column_values_to_be_unique("Country")

# COMMAND ----------

validator.expect_column_values_to_be_of_type("Country", "str")

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

checkpoint_name = "testing_checkpoint"

checkpoint = Checkpoint(
    name = checkpoint_name,
    run_name_template = "%Y%m%d-%H%M%S-my-run-name-template",
    data_context = context,
    batch_request = batch_request,
    expectation_suite_name = expectation_suite_name,
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
