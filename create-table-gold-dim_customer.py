# Databricks notebook source
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
# MAGIC   CONSTRAINT dim_customer_pk PRIMARY KEY (dim_customer_id)
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE gerald_hopkins_workspace.gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.dim_customer;

# COMMAND ----------

dbutils.fs.ls('/tmp/')

# COMMAND ----------

dbutils.fs.rm('/tmp/checkpoint-gold-dim-customer/', recurse=True)
