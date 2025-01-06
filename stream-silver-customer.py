# Databricks notebook source
# MAGIC %md
# MAGIC # Stream customer data from delta bronze.customer_raw to delta silver.customer

# COMMAND ----------

from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Table names in Unity Catalog
bronze_table_name = "gerald_hopkins_workspace.bronze.customer_raw"  # Unity Catalog table name for Bronze
silver_table_name = "gerald_hopkins_workspace.silver.customer"      # Unity Catalog table name for Silver

# Start a streaming read from the Bronze table
bronze_stream = (
    spark.readStream.format("delta")
    .table(bronze_table_name)  # Use the Unity Catalog table name
)

# Function to upsert data into Silver table with schema evolution
def upsert_to_silver(batch_df, batch_id):
    """
    Perform upsert (update and insert) into the Silver Delta table.
    Handles schema evolution automatically.

    Args:
        batch_df: DataFrame representing the batch of streaming data.
        batch_id: Unique identifier for the batch.
    """
    # Deduplicate the batch by c_custkey
    batch_df_deduplicated = batch_df.dropDuplicates(["c_custkey"])

    # Check if the Silver table exists
    if DeltaTable.isDeltaTable(spark, silver_table_name):
        # Load the Silver table as a DeltaTable
        silver_table = DeltaTable.forName(spark, silver_table_name)

        # Merge the deduplicated batch into the Silver table
        silver_table.alias("silver").merge(
            batch_df_deduplicated.alias("bronze"),
            "silver.c_custkey = bronze.c_custkey"  # Match key for deduplication
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
    else:
        # Create the Silver table on the first run with schema evolution
        batch_df_deduplicated.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table_name)

# Write the stream and apply the upsert function
query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_to_silver)  # Use foreachBatch for batch processing
    .outputMode("update")            # Update output mode ensures incremental processing
    .option("checkpointLocation", "dbfs:/tmp/checkpoint-silver/")  # Checkpoint for streaming
    .start()
)

query.awaitTermination()

