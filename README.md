# databricks-sales
This repo has my databricks spark stream and spark SQL code to stream sales data into a data lakehouse.
I used:
   * AutoLoader for incremental processing.
   * Delta Lake tables to handle schema evolution, schema inference, checkpointing and time travel.
   * Unity Catalog for governance and security.

The following are my steps and corresponding source Databricks notebook files:

- Pulled data from the Databricks sample sets, chunked it, and loaded it into an S3 bucket with local python on my Mac. (**chunk-upload-csv-s3.py**)
- Mounted the S3 bucket onto DBFS and used it for my landing zone, s3://databricks-tcph/landing.
- Created the Medallion Bronze schema. (**create-bronze-schema.py**)
- Created the Raw customer table in the Bronze schema bronze.customer_raw. (**create-customer-raw-table.py**)
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed with AutoLoader the customer data from the landing bucket to bronze.customer_raw (**stream-cust-land-raw.py**)
    * The stream is triggered by a new file landing in the landing folder in the mounted S3 bucket.
    * Uses schema inference and checkpointing
    * Adds columns for data_added and source_filename
- Created the Medallion Silver schema. (**create-silver-schema.py**)
- Created the 3NF customer table in the Silver schema silver.customer (**create-table-silver-customer.py**)
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed with AutoLoader the customer data from customer_raw to customer (**stream-silver-customer.py**)
    * The stream is triggered by record changes in the bronze.customer_raw table
    * Uses schema inference and checkpointing
    * accepts source_filename from customer_raw; adds new column for date_added
    * deduplicates on c_custkey in read stream batches from customer_raw before write stream to silver.customer
- TO DO
     * Gold Layer schema
     * gold.dim_customer
     * SCD2 loading
     * Stream load gold.dim_customer
     * Add stream task to stream-customer Workflow
  


Medallion Layers details:
- bronze.customer_raw (csv format, no transformation except for date_added, source_filename)
- silver.customer  (3NF with no overt history; history only being available via Delta Lake time travel)
- gold.dim_customer (fully dimensional SCD2: keeps history overtly in the records)

I built a Workflow to orchestrate the three streaming tasks that load the Medallion layers.

I also include some notebooks to validate and troubleshoot the stream tasks.
