-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dominicdl/processed";

-- COMMAND ----------

DESCRIBE DATABASE f1_processed;