# Databricks notebook source
config_delta_path = '/FileStore/tables/config_testing.csv'

# COMMAND ----------

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
      
  




