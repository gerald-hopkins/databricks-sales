# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_customer LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT c_custkey) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT source_filename) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for duplicate c_custkey's in gold.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c_custkey, count(1)
# MAGIC FROM gold.dim_customer
# MAGIC GROUP BY c_custkey
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY count(1) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_customer WHERE c_custkey = 148;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify that bronze.customer_raw still has records

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customer_raw limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify that checkpoint folder for moving data from silver to dim_customer still has logs

# COMMAND ----------

dbutils.fs.ls('tmp/checkpoint-gold-dim-customer/')

# COMMAND ----------

dbutils.fs.ls('tmp/') 

# COMMAND ----------

# MAGIC %md
# MAGIC ## use spark to run sql to show that spark stream can connect to tables

# COMMAND ----------

spark.sql("SELECT * FROM gold.dim_customer LIMIT 10")

# COMMAND ----------

spark.sql("SELECT * FROM gerald_hopkins_workspace.gold.dim_customer LIMIT 10")

# COMMAND ----------

spark.sql("SELECT * FROM gerald_hopkins_workspace.bronze.customer_raw LIMIT 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## check max and min start_date on gold.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(start_date),MIN(start_date) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(LEN(c_mktsegment)) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(LEN(c_phone)) FROM gold.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_customer
# MAGIC WHERE end_date IS NOT NULL
# MAGIC ORDER BY c_custkey, end_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_customer
# MAGIC WHERE c_custkey IN (32,115, 5435,50000)
# MAGIC ORDER BY c_custkey,start_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for mismatches of end_date and is_current. Both results should be 0 records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM gold.dim_customer
# MAGIC WHERE end_date IS NULL AND is_current = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM gold.dim_customer
# MAGIC WHERE end_date IS NOT NULL AND is_current = TRUE
