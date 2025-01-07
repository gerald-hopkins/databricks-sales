# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gerald_hopkins_workspace.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify gold schema was created

# COMMAND ----------

display(spark.catalog.listDatabases())
