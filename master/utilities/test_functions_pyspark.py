# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func

# count validation
def count_validation(source_df, target_df):
    source_count = source_df.count() # source count
    target_count = target_df.count() # target count

    if source_count == target_count:
        print(f'source_count is: {source_count} and target_count is: {target_count} and the counts are matching')
    else:
        print(f'source_count is: {source_count} and target_count is: {target_count} and the counts are not matching')

# null validation
def null_validation(dataframe, columns):
    null_counts = {}
    for column in columns:
        nulls = dataframe.filter(col(column).isNull()).count()
        if nulls > 0:
            null_counts[column] = nulls
    
    if not null_counts:
        print("Null value check result - No null values found in any column.")
    else:
        for column, count in null_counts.items():
            print(f"Column '{column}' has {count} null values.")

## duplicate check
def duplicate_check(df, column):
    
    dupes = df.groupBy(column).agg(F.count("*").alias("duplicate_count"))
    dupes_with_count = dupes.filter(F.col("duplicate_count") > 1)
    if dupes_with_count.count() > 0:
        print(f"Duplicate check result - WARNING: Found potential duplicates in table:")
        dupes_with_count.show(truncate=False)
    else:
        print(f"Duplicate check result - NO duplicate records found in table.")
