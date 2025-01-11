# Databricks notebook source
# MAGIC %md
# MAGIC # RESET all tables and checkpoints in the customer pipe

# COMMAND ----------

# MAGIC %md
# MAGIC ## First, drop the customer table in each Medallion layer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gerald_hopkins_workspace.silver.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gerald_hopkins_workspace.bronze.customer_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second, remove all checkpoint folders

# COMMAND ----------

dbutils.fs.rm('/tmp/checkpoint_bronze', recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/checkpoint-silver/", recurse=True)

# COMMAND ----------

dbutils.fs.rm('/tmp/checkpoint-gold-dim-customer/', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RE-CREATE all tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gerald_hopkins_workspace.bronze.customer_raw
# MAGIC USING DELTA;

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
# MAGIC   hash_value STRING, 
# MAGIC   PRIMARY KEY (c_custkey)
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gerald_hopkins_workspace.gold.dim_customer (
# MAGIC   dim_customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   c_custkey INT NOT NULL,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey INT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DOUBLE,
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   source_filename STRING,
# MAGIC   start_date TIMESTAMP NOT NULL,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN NOT NULL, 
# MAGIC   CONSTRAINT dim_customer_pk PRIMARY KEY (dim_customer_id)
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gerald_hopkins_workspace.gold.dim_customer 
# MAGIC SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gerald_hopkins_workspace.gold.dim_customer 
# MAGIC ALTER COLUMN is_current SET DEFAULT TRUE;
