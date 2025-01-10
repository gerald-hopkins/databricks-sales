# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE the silver.customer table

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP the existing table first

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE gerald_hopkins_workspace.silver.customer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE the table as delta lake and with CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gerald_hopkins_workspace.silver.customer (
# MAGIC   c_custkey INT NOT NULL,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey INT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DOUBLE,
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   source_filename STRING,
# MAGIC   date_added TIMESTAMP,
# MAGIC   PRIMARY KEY (c_custkey)
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE gerald_hopkins_workspace.silver.customer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## If you drop the table from a stream, you must remove the checkpoint for the stream.

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/checkpoint-silver/", recurse=True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/tmp/checkpoint-silver/')

# COMMAND ----------

dbutils.fs.ls('tmp/')
