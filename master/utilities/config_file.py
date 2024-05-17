# Databricks notebook source
config_delta_path = '/FileStore/tables/config_testing.csv'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import split,col
from datetime import datetime
# from configuration_details import *

class Config:
   def __init__(self,source_table_name):
        self.source_table_name = source_table_name
    
   def get_config_details(self):
       config_details_df = spark.read.csv(config_delta_path, header = True)
       config_details_required = config_details_df.filter(config_details_df["source_table_name"] == self.source_table_name).collect()
       return config_details_required
   
   def create_source_dataframe(self, config_details_required):
       ## source table details
       source_storage_account_name = config_details_required[0]["source_storage_account_name"]
       source_storage_access_key = config_details_required[0]["source_storage_access_key"]
       source_file_path = config_details_required[0]['source_file_path']

       ## read the source table details
       spark.conf.set(f"fs.azure.account.key.{source_storage_account_name}.dfs.core.windows.net", source_storage_access_key)
       source_file_system_name = config_details_required[0]["source_container_name"]
       source_csv_file_path = f"abfss://{source_file_system_name}@{source_storage_account_name}{source_file_path}"
       source_df= spark.read.csv(source_csv_file_path, header = True)
       return source_df
   
   def create_target_dataframe(self, config_details_required):
       ## target table details
       target_storage_account_name = config_details_required[0]["target_storage_account_name"]
       target_storage_access_key = config_details_required[0]["target_storage_access_key"]
       target_file_path = config_details_required[0]['target_file_path']

       ## read the target table details
       spark.conf.set(f"fs.azure.account.key.{target_storage_account_name}.dfs.core.windows.net", target_storage_access_key)
       target_file_system_name = config_details_required[0]["target_container_name"]
       target_csv_file_path = f"abfss://{target_file_system_name}@{target_storage_account_name}{target_file_path}"
       target_df = spark.read.csv(target_csv_file_path, header = True)
       return target_df

   def get_pk_field(self, config_details_required):
       pk_field = config_details_required[0]["pk_fields"]
       return pk_field
      
  





# COMMAND ----------

config_details = Config('100 CSV records')
config_details_required = config_details.get_config_details()
source_df = config_details.create_source_dataframe(config_details_required)
target_df = config_details.create_target_dataframe(config_details_required)
pk_field = config_details.get_pk_field(config_details_required)
pk_field


# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, ArrayType
# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.functions import split,col
# from datetime import datetime


# class Config:
#    def __init__(self, param1,table_name):
#         self.input_args = param1
#         self.table_name = table_name

#    def get_config_file_path(self):
       
#        df = spark.read.json(mapping_json_path) 
#        df=df.filter(df["file_path"].isNotNull())
#        rows=df.collect()
#        extracted_val=rows[0]["Config_file_path"]
#        return extracted_val 

#    def get_pk_fields_name(self):
#        df = spark.read.json(mapping_json_path) 
#        df=df.filter(df["PKFields"].isNotNull())
#        rows=df.collect()
#        extracted_val=rows[0]["PKFields"]
#        return extracted_val
  
#    #Reading from Config table w.r.t input_argument from user 
#    def readconfigasdf(self,config_path):
#        df = spark.read.format("delta").load(config_path)
#        df = df.filter((df["TestConfigID"]==self.input_args) & (df["SourceDataset"]==self.table_name))       
#        return df
  
#    def get_pk_fields(self):
#        pk_fileds_column_name=self.get_pk_fields_name()
#        config_path=self.get_config_file_path()
#        df=self.readconfigasdf(config_path)

#        df=df.filter(df[pk_fileds_column_name].isNotNull())    
#        if df.isEmpty():           
#            return "NA"
#        else:
#            rows=df.collect()
#            #print(rows)
#            extracted_val=rows[0]["PKFields"]
#            #print("extracted_val=rows[0][PKFields]=",extracted_val)
#            return extracted_val
      
  





# COMMAND ----------


