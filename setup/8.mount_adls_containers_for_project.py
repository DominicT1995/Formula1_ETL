# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get("formula1-scope", "formula1dl-client-id")
    tenant_id = dbutils.secrets.get("formula1-scope", "formula1dl-tenant-id")
    client_secret = dbutils.secrets.get("formula1-scope", "formula1dl-client-secret")

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount if mount already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storgage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw, Processed, and Presentation Containers

# COMMAND ----------

mount_adls('formula1dominicdl', 'raw')

# COMMAND ----------

mount_adls('formula1dominicdl', 'processed')

# COMMAND ----------

mount_adls('formula1dominicdl', 'presentation')

# COMMAND ----------

mount_adls('formula1dominicdl', 'demo')

# COMMAND ----------

