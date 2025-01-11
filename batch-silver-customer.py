# Databricks notebook source
from pyspark.sql.functions import col, desc, row_number, sha2, concat_ws
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Table names in Unity Catalog
bronze_table_name = "gerald_hopkins_workspace.bronze.customer_raw"  # Unity Catalog table name for Bronze
silver_table_name = "gerald_hopkins_workspace.silver.customer"      # Unity Catalog table name for Silver

print('started')

# Function to deduplicate the batch DataFrame
def deduplicate_latest_records(batch_df):
    print('deduping batch')
    window_spec = Window.partitionBy("c_custkey").orderBy(desc("date_added"))
    return batch_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

# Function to upsert data into Silver table with schema evolution
def upsert_to_silver(batch_df, batch_id):
    print('starting upsert_to_silver')
    # Exclude _rescued_data column before applying deduplication
    batch_df = batch_df.drop("_rescued_data")
    
    # Deduplicate the batch DataFrame
    batch_df_deduplicated = deduplicate_latest_records(batch_df)

    # Now exclude the necessary columns
    excluded_columns = {"source_filename", "date_added"}  # _rescued_data already dropped
    hash_columns = [col_name for col_name in batch_df_deduplicated.columns if col_name not in excluded_columns]

    # Add a hash column for change detection
    batch_df_deduplicated = batch_df_deduplicated.withColumn(
        "hash_value", sha2(concat_ws("||", *[col(c) for c in hash_columns]), 256)
    )

    if spark.catalog.tableExists(silver_table_name):
        silver_table = DeltaTable.forName(spark, silver_table_name)
        silver_table.alias("silver").merge(
            batch_df_deduplicated.alias("bronze"),
            "silver.c_custkey = bronze.c_custkey"
        ).whenMatchedUpdate(
            condition="silver.hash_value != bronze.hash_value",
            set={col: f"bronze.{col}" for col in batch_df_deduplicated.columns}  # Include hash_value in the set clause
        ).whenNotMatchedInsert(
            values={col: f"bronze.{col}" for col in batch_df_deduplicated.columns}  # Include hash_value in the insert clause
        ).execute()
    else:
        # Create the Silver table on the first run
        print('creating silver table')
        batch_df_deduplicated.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table_name)

# Start a streaming read from the Bronze table
bronze_stream = (
    spark.readStream.format("delta")
    .table(bronze_table_name)
)

query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .trigger(once=True)
    .option("checkpointLocation", "dbfs:/tmp/checkpoint-silver/")
    .start()
)

query.awaitTermination()

