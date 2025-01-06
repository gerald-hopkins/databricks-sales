# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA gerald_hopkins_workspace.silver;

# COMMAND ----------

display(spark.catalog.listDatabases())
