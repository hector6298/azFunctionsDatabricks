# Databricks notebook source
def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("cloudFiles.schemaHints", 
                          """VendorID string, 
                             tpep_pickup_datetime date, 
                             tpep_dropoff_datetime date, 
                             passenger_count int""")
                  .load(data_source)
                  .select(["VendorId", 
                           "tpep_pickup_datetime", 
                           "tpep_dropoff_datetime", 
                           "passenger_count"])
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .table(table_name))
    return query

query = autoload_to_table(data_source = f"{DA.paths.working_dir}/yellow_trip_data",
                          source_format = "csv",
                          table_name = "target_table",
                          checkpoint_directory = f"/tmp/")
