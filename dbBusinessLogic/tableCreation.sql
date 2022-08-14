-- Databricks notebook source
CREATE TABLE yellow_trip_data (
  VendorID string,
  tpep_pickup_datetime date,
  tpep_dropoff_datetime date,
  passenger_count int
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://test@teststoragehector.blob.core.windows.net",
-- MAGIC   mount_point = "/mnt/test",
-- MAGIC   extra_configs = {"fs.azure.account.key.teststoragehector.blob.core.windows.net":"ZGn++HgXFhyEtuM0NVFX0cdsPEVA37T8HMKi8HG2qdc3v1HG/B+PKts7GVPUfkin3IYjDza0mX1Q+AStw6Ffgg=="})
