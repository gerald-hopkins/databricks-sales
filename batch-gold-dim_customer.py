# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# Define table names
silver_table_name = "gerald_hopkins_workspace.silver.customer"
gold_table_name = "gerald_hopkins_workspace.gold.dim_customer"

# Read stream from Silver table using Change Data Feed (CDF)
silver_stream = (
    spark.readStream.format("delta")
    .option("readChangeData", "true")  # Enable CDF to track changes (inserts, updates)
    .table(silver_table_name)           # Specify the Delta table
)

def scd2_upsert_to_gold(batch_df, batch_id):
    """
    Perform SCD Type 2 upsert from Silver to Gold table.
    Handles new inserts, updates, and schema evolution.
    
    Args:
        batch_df: DataFrame representing the batch of streaming data.
        batch_id: Unique identifier for the batch.
    """
    print(f"Processing batch {batch_id}")
    display(batch_df)  # Display the batch data for debugging

    # Exclude the date_added column
    if "date_added" in batch_df.columns:
        batch_df = batch_df.drop("date_added")

    # Filter out rows where _change_type is update_preimage
    batch_df = batch_df.filter(batch_df["_change_type"] != "update_preimage")

    # Add SCD 2 metadata columns
    batch_df = batch_df.withColumn("start_date", current_timestamp().cast("timestamp")) \
                       .withColumn("end_date", lit(None).cast("timestamp")) \
                       .withColumn("is_current", lit(True).cast("boolean"))

    # Check if the Gold table exists
    if spark.catalog.tableExists(gold_table_name):
        gold_table = DeltaTable.forName(spark, gold_table_name)

        # Handle SCD Type 2 logic with MERGE
        gold_table.alias("gold").merge(
            batch_df.alias("silver"),
            "gold.c_custkey = silver.c_custkey AND gold.is_current = True"
        ).whenMatchedUpdate(
            condition="gold.is_current = True AND (" +
                      " OR ".join([f"gold.{col} <> silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current', '_change_type', '_commit_version', '_commit_timestamp']]) +
                      ")",
            set={
                "end_date": current_timestamp().cast("timestamp"),
                "is_current": lit(False).cast("boolean")
            }
        ).whenNotMatchedInsert(
            values={
                "c_custkey": "silver.c_custkey",
                **{col: f"silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current', '_change_type', '_commit_version', '_commit_timestamp']},
                "start_date": current_timestamp().cast("timestamp"),
                "end_date": lit(None).cast("timestamp"),
                "is_current": lit(True).cast("boolean")
            }
        ).execute()

        # Insert new records for rows where _change_type is update_postimage
        new_records = batch_df.filter(batch_df["_change_type"] == "update_postimage") \
                              .withColumn("start_date", current_timestamp().cast("timestamp")) \
                              .withColumn("end_date", lit(None).cast("timestamp")) \
                              .withColumn("is_current", lit(True).cast("boolean")) \
                              .drop("_change_type", "_commit_version", "_commit_timestamp")
        
        # Print the schema of new_records
        print('schema of new_records')
        new_records.printSchema()

        # Ensure new_records has only the target columns (excluding dim_customer_id)
        target_columns = [field.name for field in gold_table.toDF().schema.fields if field.name != "dim_customer_id"]
        new_records = new_records.select(*target_columns)
        
        # Append new records to the Gold table
        new_records.write.format("delta").mode("append").saveAsTable(gold_table_name)
    else:
        # If the Gold table does not exist, create it with SCD Type 2 logic
        print(f"Gold table '{gold_table_name}' does not exist. Creating schema with SCD2 logic...")
        batch_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table_name)

        # Handle SCD Type 2 logic with MERGE
        gold_table.alias("gold").merge(
            batch_df.alias("silver"),
            "gold.c_custkey = silver.c_custkey AND gold.is_current = True"
        ).whenMatchedUpdate(
            condition="gold.is_current = True AND (" +
                      " OR ".join([f"gold.{col} <> silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current', '_change_type', '_commit_version', '_commit_timestamp']]) +
                      ")",
            set={
                "end_date": current_timestamp().cast("timestamp"),
                "is_current": lit(False).cast("boolean")
            }
        ).whenNotMatchedInsert(
            values={
                "c_custkey": "silver.c_custkey",
                **{col: f"silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current', '_change_type', '_commit_version', '_commit_timestamp']},
                "start_date": current_timestamp().cast("timestamp"),
                "end_date": lit(None).cast("timestamp"),
                "is_current": lit(True).cast("boolean")
            }
        ).execute()
        
        # Insert new records for rows where _change_type is update_postimage
        new_records = batch_df.filter(batch_df["_change_type"] == "update_postimage") \
                              .withColumn("start_date", current_timestamp().cast("timestamp")) \
                              .withColumn("end_date", lit(None).cast("timestamp")) \
                              .withColumn("is_current", lit(True).cast("boolean")) \
                              .drop("_change_type", "_commit_version", "_commit_timestamp")
        
        # Print the schema of new_records
        print('schema of new_records')
        new_records.printSchema()

        # Ensure new_records has only the target columns (excluding dim_customer_id)
        target_columns = [field.name for field in gold_table.toDF().schema.fields if field.name != "dim_customer_id"]
        new_records = new_records.select(*target_columns)
        
        # Append new records to the Gold table
        new_records.write.format("delta").mode("append").saveAsTable(gold_table_name)

# Write the stream with SCD 2 upsert logic
query = (
    silver_stream.writeStream
    .foreachBatch(scd2_upsert_to_gold)  # Apply foreachBatch for batch processing
    .outputMode("update")               # Update ensures changes are captured
    .option("checkpointLocation", "dbfs:/tmp/checkpoint-gold-dim-customer/")  # Checkpoint location for the stream
    .trigger(availableNow=True)         # Process all available data and then stop
    .start()
)

# Wait for the query to finish
query.awaitTermination()


