# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM silver.customer LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM silver.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT c_custkey) FROM silver.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT source_filename) FROM silver.customer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for duplicate c_custkey's in silver.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c_custkey, count(1)
# MAGIC FROM silver.customer
# MAGIC GROUP BY c_custkey
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY count(1) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customer WHERE c_custkey = 148;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify that bronze.customer_raw still has records

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customer_raw limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify that checkpoint folder for moving data from customer_raw to customer still has logs

# COMMAND ----------

dbutils.fs.ls('tmp/checkpoint-silver/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## use spark to run sql to show that spark stream can connect to tables

# COMMAND ----------

spark.sql("SELECT * FROM silver.customer LIMIT 10")

# COMMAND ----------

spark.sql("SELECT * FROM gerald_hopkins_workspace.silver.customer LIMIT 10")

# COMMAND ----------

spark.sql("SELECT * FROM gerald_hopkins_workspace.bronze.customer_raw LIMIT 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## check max and min date_added on silver.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(date_added),MIN(date_added) FROM silver.customer;
