# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Merge the required columns and add ingestion date to dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, concat, col

# COMMAND ----------

merged_column_races_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

ingest_races_df = add_ingestion_date(merged_column_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select only the required columns and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_final_df = ingest_races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("data_source"),col("file_date"), col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.partitionBy("race_year").mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")