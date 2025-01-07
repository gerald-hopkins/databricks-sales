# Databricks notebook source
# MAGIC %md
# MAGIC # Chunk customer file and upload to S3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ran this code on my local Mac in Jupyter Notebook

# COMMAND ----------

import boto3
import pandas as pd
import os
import time
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
AWS_ACCESS_KEY = "my-access-key"  # Replace with AWS access key
AWS_SECRET_KEY = "my-secret-key"  # Replace with AWS secret key
BUCKET_NAME = "my-bucket-name"    # Replace with S3 bucket name
REGION_NAME = "my-region"         # Replace with S3 bucket region

# Create an S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
)

# Function to Upload File to S3
def upload_file_to_s3(file_path, bucket_name, s3_key):
    """
    Uploads a file to an S3 bucket.

    :param file_path: Local path to the file
    :param bucket_name: S3 bucket name
    :param s3_key: S3 object key (file name in the bucket)
    """
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"File {file_path} uploaded to S3 bucket {bucket_name} as {s3_key}")
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    except NoCredentialsError:
        print("Credentials not available.")

# Break Large CSV into Smaller Files and Upload to S3
def process_and_upload_large_csv(input_file, output_dir, bucket_name):
    """
    Splits a large CSV file into smaller files of 10 records each and uploads them to S3 every 1 minute.

    :param input_file: Path to the large input CSV file
    :param output_dir: Directory to save smaller files
    :param bucket_name: S3 bucket name
    """
    # Load the large CSV file into a Pandas DataFrame
    df = pd.read_csv(input_file)
    total_rows = len(df)
    chunk_size = 10
    num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size > 0 else 0)

    for i in range(num_chunks):
        # Create a smaller DataFrame chunk
        chunk = df.iloc[i * chunk_size : (i + 1) * chunk_size]
        output_file = os.path.join(output_dir, f"chunk_{i + 1}.csv")
        chunk.to_csv(output_file, index=False)

        # Upload the smaller file to S3
        s3_key = f"chunks/{os.path.basename(output_file)}"
        upload_file_to_s3(output_file, bucket_name, s3_key)

        # Wait for 1 minute before processing the next chunk
        if i < num_chunks - 1:
            print("Waiting 1 minute before uploading the next chunk...")
            time.sleep(60)

# Example Usage
input_csv = "/path/to/large_file.csv"      # Replace with the path to your large CSV file
output_directory = "/path/to/output_dir"  # Replace with a local directory to store smaller files
os.makedirs(output_directory, exist_ok=True)

process_and_upload_large_csv(input_csv, output_directory, BUCKET_NAME)

