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
