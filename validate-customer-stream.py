# Databricks notebook source
# MAGIC %md
# MAGIC # Validate that data streamed successfully from landing to customer_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gerald_hopkins_workspace.bronze.customer_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## how many total records in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM bronze.customer_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## how many unique c_custkey's

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT c_custkey)
# MAGIC FROM bronze.customer_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## verify that date_added, source_filename have been added to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c_custkey, date_added, source_filename
# MAGIC FROM bronze.customer_raw
# MAGIC WHERE source_filename IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many source_files are represented in the records?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT source_filename
# MAGIC FROM bronze.customer_raw
# MAGIC ORDER BY source_filename;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which records came in a specific source file?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.customer_raw
# MAGIC WHERE source_filename LIKE '%cleaned.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ## validate proper handling of inserts and updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC (SELECT * FROM bronze.customer_raw
# MAGIC WHERE source_filename LIKE '%cleaned.csv') a 
# MAGIC JOIN bronze.customer_raw b 
# MAGIC ON a.c_custkey = b.c_custkey;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the runs of this stream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_added, count(1)
# MAGIC FROM bronze.customer_raw
# MAGIC GROUP BY date_added

# COMMAND ----------

# MAGIC %md
# MAGIC ## Any rescued data?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.customer_raw WHERE `_rescued_data` IS NOT NULL
