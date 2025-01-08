# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gerald_hopkins_workspace.bronze.customer_raw
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gerald_hopkins_workspace.bronze.customer_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gerald_hopkins_workspace.bronze.customer_raw LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE gerald_hopkins_workspace.bronze.customer_raw;

# COMMAND ----------

dbutils.fs.rm('/tmp/checkpoint_bronze', recurse=True)

# COMMAND ----------

dbutils.fs.ls('/tmp/checkpoint_bronze')

# COMMAND ----------

dbutils.fs.ls('/tmp/schema_bronze/_schemas/')

# COMMAND ----------

dbutils.fs.ls('/tmp/schema_bronze/_schemas/0')

# COMMAND ----------

dbutils.fs.head('/tmp/schema_bronze/_schemas/0')

# COMMAND ----------

dbutils.fs.rm('/tmp/schema_bronze/_schemas/1')

# COMMAND ----------

dbutils.fs.ls('/tmp/')
