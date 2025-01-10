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
# MAGIC ## Check for duplicate customer keys. There should be none in Silver layer.

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

# MAGIC %sql
# MAGIC SELECT MAX(date_added) FROM silver.customer;

# COMMAND ----------

dbutils.fs.ls('tmp/checkpoint-silver/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Did any changed or new records show up in the last stream?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customer WHERE date_added = '2025-01-09T21:12:03.564+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify that expected updates happened

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customer WHERE c_custkey IN (32,115)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_added, COUNT(1)
# MAGIC FROM silver.customer
# MAGIC GROUP BY date_added;
