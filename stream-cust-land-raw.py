# Databricks notebook source
from pyspark.sql import functions as F

# Path to your S3 bucket (mounted or direct access)
s3_path = "/mnt/databricks-tcph/landing/"

# Define the Delta table path or Unity Catalog table name
delta_table_name = "gerald_hopkins_workspace.bronze.customer_raw"

# Define checkpoint location
checkpoint_path = "/tmp/checkpoint_bronze/"

# Define schema location
schema_location = "/tmp/schema_bronze/"

# Read new files from the landing directory in S3 using cloudFiles with schema inference
raw_data = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")  # Assuming CSV files; change format if necessary (e.g., Parquet)
    .option("cloudFiles.inferColumnTypes", "true")  # Enable schema inference
    .option("cloudFiles.schemaLocation", schema_location)  # Schema location for schema inference
    .load(s3_path)  # Path to the S3 bucket (landing folder)
)

# Add the source filename as a new column
raw_data_with_filename = raw_data.withColumn("source_filename", F.input_file_name())

# Optionally, perform any other transformations on the data (e.g., add columns)
transformed_data = raw_data_with_filename.withColumn("date_added", F.current_timestamp())

# Write the transformed data to the Delta table in Unity Catalog with schema evolution enabled
query = (
    transformed_data.writeStream
    .format("delta")  # Write to Delta format
    .outputMode("append")  # Append new data to the Delta table
    .option("checkpointLocation", checkpoint_path)  # Ensure you specify the checkpoint location
    .option("mergeSchema", "true")  # Enable schema evolution
    .table(delta_table_name)  # The Unity Catalog Delta table name (fully qualified)
)

# Start the streaming job
query.awaitTermination()  # Keep the stream running
