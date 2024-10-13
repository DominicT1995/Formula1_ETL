# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def reorder_partition_column(input_df, partition_column):
    col_list = []
    for col in input_df.schema.names:
        if col != partition_column:
            col_list.append(col)
    col_list.append(partition_column)

    return input_df.select(col_list)

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = reorder_partition_column(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.partitionBy(partition_column).mode("overwrite").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.partitionBy(partition_column).mode("overwrite").format("delta").saveAsTable(f"{db_name}.{table_name}")