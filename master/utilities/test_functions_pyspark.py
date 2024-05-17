# Databricks notebook source
def null_validation(source_df, target_df):
    validation_results = []
 
    # Iterate through columns and check for nulls in both source and target
    for column in source_df.columns:
        source_nulls = source_df.filter(source_df[column].isNull()).count()
        target_nulls = target_df.filter(target_df[column].isNull()).count()
    
        validation_results.append((column, source_nulls, target_nulls))
 
    # Convert validation results to a DataFrame
    results_df = spark.createDataFrame(validation_results, ["Column", "Source_Nulls", "Target_Nulls"])

    #Coalesce the DataFrame to a single partition
    results_df = results_df.coalesce(1)

    output_path = "abfss://output@stg1010.dfs.core.windows.net/null_validation/results.csv"
 
    # Write the results DataFrame to the specified path in CSV format
    results_df.write.csv(output_path, header=True, mode='overwrite')
    return results_df


# COMMAND ----------

def count_validation(source_df, target_df):

    #Get the count of rows in source and target files
    source_count = source_df.count() # source count
    target_count = target_df.count() # target count

    #Create a DataFrame with the results 
    validation_results =[(source_count, target_count)]
    results_df = spark.createDataFrame(validation_results, [ "Source_Count", "Target_Count"])

    #Coalesce the DataFrame to a single partition
    results_df = results_df.coalesce(1)

    output_path = "abfss://output@stg1010.dfs.core.windows.net/count_validation/results.csv"
 
    # Write the results DataFrame to the specified path in CSV format
    results_df.write.csv(output_path, header=True, mode='overwrite')
    return results_df

# COMMAND ----------

from pyspark.sql.functions import col, count, lit
def duplicate_check(source_df, target_df):
    # Identify duplicates in source and target DataFrames
    source_duplicates = source_df.groupBy(source_df.columns).count().filter(col("count") > 1)
    target_duplicates = target_df.groupBy(target_df.columns).count().filter(col("count") > 1)
 
    # Add a column to indicate source/target for clarity
    source_duplicates = source_duplicates.withColumn("SourceOrTarget", lit("Source"))
    target_duplicates = target_duplicates.withColumn("SourceOrTarget", lit("Target"))
 
    # Union the two DataFrames to get a combined result
    validation_results = source_duplicates.union(target_duplicates)
 
    # Select only the necessary columns for output
    results_df = validation_results.select(*source_df.columns, "SourceOrTarget")

    #Coalesce the DataFrame to a single partition
    results_df = results_df.coalesce(1)

    output_path = "abfss://output@stg1010.dfs.core.windows.net/duplicate_validation/results.csv"
 
    # Write the results DataFrame to the specified path in CSV format
    results_df.write.csv(output_path, header=True, mode='overwrite')
    return results_df


