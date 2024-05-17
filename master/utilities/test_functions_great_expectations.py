# Databricks notebook source
# MAGIC %run "/Workspace/Repos/hrithik@sagar1001.onmicrosoft.com/Testing-Framework/master/utilities/config_file"

# COMMAND ----------

# getting configuration details
config_details = Config('100 CSV records')
config_details_required = config_details.get_config_details()

# creating input file
source_df = config_details.create_source_dataframe(config_details_required)

# creating output file
target_df = config_details.create_target_dataframe(config_details_required)

pk_field = config_details.get_pk_field(config_details_required)

# Creating great expectations dataframe
source_ge_df = ge.dataset.SparkDFDataset(source_df)
target_ge_df = ge.dataset.SparkDFDataset(target_df)


# COMMAND ----------

import great_expectations as ge
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
source_count = source_df.count()
validator.expect_table_row_count_to_equal(source_count)

# COMMAND ----------

# to check if there are duplicates in a column
validator.expect_column_values_to_be_unique("Country")

# COMMAND ----------

validator.expect_column_values_to_be_of_type("Country", str)

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

# COMMAND ----------

print(checkpoint.get_config().to_yaml_str())
