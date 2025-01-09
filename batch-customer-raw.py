# Databricks notebook source
from pyspark.sql import functions as F

# Path to your S3 bucket (mounted)
s3_path = "/mnt/databricks-tcph/landing/"

# Define the Delta table path from Unity Catalog
delta_table_name = "gerald_hopkins_workspace.bronze.customer_raw"

# Define checkpoint location
checkpoint_path = "/tmp/checkpoint_bronze/"

# Define schema location
schema_location = "/tmp/schema_bronze/"

# Read new files from the landing directory in S3 using cloudFiles with schema inference
raw_data = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv") 
    .option("cloudFiles.inferColumnTypes", "true")  # Enable schema inference
    .option("cloudFiles.schemaLocation", schema_location)  # Schema location for schema inference
    .load(s3_path)  # Path to the S3 bucket (landing folder)
)

# Add the source filename as a new column
raw_data_with_filename = raw_data.withColumn("source_filename", F.input_file_name())

# transformations on the data
transformed_data = raw_data_with_filename.withColumn("date_added", F.current_timestamp())

# Write the transformed data to the Delta table in Unity Catalog with schema evolution enabled
query = (
    transformed_data.writeStream
    .format("delta")  # Write to Delta format
    .outputMode("append")  # Append new data to the Delta table
    .option("checkpointLocation", checkpoint_path)  # Ensure you specify the checkpoint location
    .option("mergeSchema", "true")  # Enable schema evolution
    .trigger(once=True)  # Process all available data and stop
    .table(delta_table_name)  # The Unity Catalog Delta table name (fully qualified)
)

# Start the streaming job
query.awaitTermination()  # This will complete after processing all available data

