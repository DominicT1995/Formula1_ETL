# Databricks notebook source
# MAGIC %md
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret / password for the application
# MAGIC 1. Set Spark Config with App / Client Id, Directory / Tenant Id and Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get("formula1-scope", "formula1dl-client-id")
tenant_id = dbutils.secrets.get("formula1-scope", "formula1dl-tenant-id")
client_secret = dbutils.secrets.get("formula1-scope", "formula1dl-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dominicdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dominicdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dominicdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dominicdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dominicdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dominicdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dominicdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

