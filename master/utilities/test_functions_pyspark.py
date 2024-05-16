# Databricks notebook source
import pyspark.sql.functions as func


# COMMAND ----------

# Null check for all columns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Function to perform null validation
def null_validation(dataframe):
    null_counts = {}
    for column in dataframe.columns:
        nulls = dataframe.filter(col(column).isNull()).count()
        if nulls > 0:
            null_counts[column] = nulls
    
    if not null_counts:
        print("Null value check result - No null values found in any column.")
    else:
        for column, count in null_counts.items():
            print(f"Column '{column}' has {count} null values.")

# COMMAND ----------

# Duplicate check on whole table
def check_duplicates(dataframe, subset_columns=None):
    if subset_columns is None:
        # Check for duplicates across the entire DataFrame
        duplicates = dataframe.groupBy(dataframe.columns).count().filter("count > 1").drop('count')
    else:
        # Check for duplicates based on specified columns
        duplicates = dataframe.groupBy(subset_columns).count().filter("count > 1").drop('count')
    
    if duplicates.count() > 0:
        print("Duplicates in table check result - Duplicate records found:")
        duplicates.show()
    else:
        print("Duplicates in table check result - No duplicate records found.")


# COMMAND ----------

## Duplicate check on a particular column
pk_field = "Order ID"
non_pk_field = "Item Type"

def duplicate_check_on_specific_column(df, column):
    
    dupes = df.groupBy(column).agg(F.count("*").alias("duplicate_count"))
    dupes_with_count = dupes.filter(F.col("duplicate_count") > 1)
    if dupes_with_count.count() > 0:
        print(f"Duplicate check result - WARNING: Found potential duplicates in table:")
        dupes_with_count.show(truncate=False)
    else:
        print(f"Duplicate check result - NO duplicate records found in table.")

# COMMAND ----------

#Schema_Validation
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date, to_timestamp, lit, when,concat
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Initialize Spark session
spark = SparkSession.builder.appName("SchemaComparison").getOrCreate()
 

def compare_schemas_and_write_mismatches_to_table(source_df: DataFrame, target_df: DataFrame):
    """
    Compare the schemas of source and target DataFrames, specifically validating column names and data types,
    and write the details to a specified table.
    """
    source_schema = source_df.schema
    target_schema = target_df.schema
    
    #conf_obj=Config(input_args,source_dataset)
    date = datetime.now().strftime("%Y-%m-%d")
    time = datetime.now().strftime("%H:%M:%S")
    #pk_field=conf_obj.get_pk_fields()

    # Maps for easy access to data types by column name
    source_fields_map = {f.name.lower(): f.dataType.simpleString() for f in source_schema.fields}
    target_fields_map = {f.name.lower(): f.dataType.simpleString() for f in target_schema.fields}

    # Check for column name mismatches
    source_columns = set(source_fields_map.keys())
    target_columns = set(target_fields_map.keys())
    missing_in_target = source_columns - target_columns
    missing_in_source = target_columns - source_columns

    # Check for data type mismatches in common columns
    common_columns = source_columns.intersection(target_columns)
    type_mismatches = [(col, source_fields_map[col], target_fields_map[col]) 
                       for col in common_columns 
                       if source_fields_map[col] != target_fields_map[col]]

    # Prepare mismatch information
    mismatches_data = []
    for col in missing_in_target:
        mismatches_data.append(("missing_{}_in_target".format(col), col, source_fields_map[col], "None"))
    for col in missing_in_source:
        mismatches_data.append(("missing_{}_in_source".format(col), col, "None", target_fields_map[col]))
    for col, source_type, target_type in type_mismatches:
        mismatches_data.append(("{}_data_type_mismatch".format(col), col, source_type, target_type))

    # Convert mismatches to DataFrame
    schema = StructType([
        StructField("Mismatch", StringType(), True),
        StructField("Column_Name", StringType(), True),
        StructField("Source_Type", StringType(), True),
        StructField("Target_Type", StringType(), True)
    ])
    mismatches_df = spark.createDataFrame(mismatches_data, schema)
    display(mismatches_df)
 

# COMMAND ----------

## primary key check
## primary key should be unique 
## primary key should not have null records

def primary_key_check(df, primary_key_cols):
    # Total count of rows in the DataFrame
    total_count = df.count()

    # Count of distinct rows based on the candidate key
    distinct_count = df.select(primary_key_cols).distinct().count()

    # Null check for the primary key columns
    null_count = df.filter(col(primary_key_cols[0]).isNull()).count()

    # Checking if the primary key candidate is unique
    pk_unique_chk = distinct_count == total_count
    pk_null_chk = null_count == 0
    print("primary key check:")
    print(f"All records of the primary key are unique -- {pk_unique_chk}")
    print(f"No null values in primary key -- {pk_unique_chk}")



# COMMAND ----------

## duplicate_check

def duplicate_check(df, primary_key_columns):

    # Total count of rows in the file
    total_count = df.count()

    # 
    distinct_count = df.select(primary_key_cols).distinct().count()

