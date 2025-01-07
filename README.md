# databricks-sales
This repo has my databricks spark stream and spark SQL code to stream sales data into a data lakehouse.

The following are my steps and corresponding source Databricks notebook files:

- Pulled data from the Databricks sample sets, chunked it, and loaded it into an S3 bucket with local python on my Mac.
- Mounted the S3 bucket onto DBFS and used it for my landing zone, s3://databricks-tcph/landing.
- Created the Medallion Bronze schema. (create-bronze-schema.py)
- Created the Raw customer table in the Bronze schema bronze.customer_raw. (create-customer-raw-table.py)
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed the customer data from the landing bucket to bronze.customer_raw (stream-cust-land-raw.py)
    * The stream is triggered by a new file landing in the landing folder in the mounted S3 bucket.
    * Uses schema inference and checkpointing
    * Adds columns for data_added and source_filename
- Created the Medallion Silver schema. (create-silver-schema.py)
- Created the 3NF customer table in the Silver schema silver.customer (create-table-silver-customer.py)
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed the customer data from customer_raw to customer (stream-silver-customer.py)
    * The stream is triggered by record changes in the bronze.customer_raw table
    * Uses schema inference and checkpointing
    * accepts source_filename from customer_raw; adds new column for date_added
    * deduplicates on c_custkey in read stream batches from customer_raw before write stream to silver.customer
- 

From there, I use spark streaming and AutoLoader to load the files through a Medallion layer architecture in Databricks Unity Catalog.

I use Delta Tables to handle schema evolution, time travel, and schema inference.

For example:
- bronze.customer_raw (csv format, no transformation except for date_added, source_filename)
- silver.customer  (3NF with no overt history; history only being available via Delta Lake time travel)
- gold.dim_customer (fully dimensional SCD2: keeps history overtly in the records)

I built a Workflow to orchestrate the three streaming tasks that load the Medallion layers.

I also include some notebooks to validate and troubleshoot the stream tasks.
