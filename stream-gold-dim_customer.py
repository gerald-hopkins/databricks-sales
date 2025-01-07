# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# Define table names
silver_table_name = "gerald_hopkins_workspace.silver.customer"
gold_table_name = "gerald_hopkins_workspace.gold.dim_customer"
print(gold_table_name)

# Read stream from Silver table
silver_stream = (
    spark.readStream.format("delta")
    .table(silver_table_name)
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

    # Add SCD 2 metadata columns
    batch_df = batch_df.withColumn("start_date", current_timestamp()) \
                       .withColumn("end_date", lit(None).cast("timestamp")) \
                       .withColumn("is_current", lit(True))

    # Check if the Gold table exists
    if DeltaTable.isDeltaTable(spark, gold_table_name):
        gold_table = DeltaTable.forName(spark, gold_table_name)

        # Handle SCD Type 2 logic with MERGE
        gold_table.alias("gold").merge(
            batch_df.alias("silver"),
            "gold.c_custkey = silver.c_custkey AND gold.is_current = True"
        ).whenMatchedUpdate(
            condition="gold.is_current = True AND (" +
                      " OR ".join([f"gold.{col} <> silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current']]) +
                      ")",
            set={
                "end_date": "current_timestamp()",
                "is_current": "False"
            }
        ).whenNotMatchedInsert(
            values={
                "c_custkey": "silver.c_custkey",
                **{col: f"silver.{col}" for col in batch_df.columns if col not in ['start_date', 'end_date', 'is_current']},
                "start_date": "current_timestamp()",
                "end_date": "NULL",
                "is_current": "True"
            }
        ).execute()
    else:
        # Infer schema and create the Gold table
        print(f"Gold table '{gold_table_name}' does not exist. Creating schema...")
        batch_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table_name)

# Write the stream with SCD 2 upsert logic
query = (
    silver_stream.writeStream
    .foreachBatch(scd2_upsert_to_gold)  # Apply foreachBatch for batch processing
    .outputMode("update")               # Update ensures changes are captured
    .option("checkpointLocation", "dbfs:/tmp/checkpoint-gold-dim-customer/")  # Checkpoint location for the stream
    .start()
)

query.awaitTermination()
