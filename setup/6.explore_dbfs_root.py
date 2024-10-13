# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

# dont keep customer data in filestore as it will be deleted when the workspace is dropped. Use for quick analysis on small set of data and dont wanna authenticate. Keep files in the folder that can be used for markdown language like charts images etc.