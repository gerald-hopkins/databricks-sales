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
- Streamed with AutoLoader the customer data from the landing bucket to bronze.customer_raw (**batch-cust-land-raw.py**)
    * The stream is triggered by a new file landing in the landing folder in the mounted S3 bucket.
    * Uses schema inference and checkpointing
    * Adds columns for data_added and source_filename
- Created the Medallion Silver schema. (**create-silver-schema.py**)
- Created the 3NF customer table in the Silver schema silver.customer (**create-table-silver-customer.py**)
    * Adds a hash_value column to the table to compare to hash to incoming data from customer_raw
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed with AutoLoader the customer data from customer_raw to customer (**batch-silver-customer.py**)
    * The stream is triggered by record changes in the bronze.customer_raw table
    * Uses schema inference and checkpointing
    * accepts source_filename from customer_raw; adds new column for date_added
    * deduplicates on c_custkey in read stream batches from customer_raw before write stream to silver.customer
    * takes hash of incoming records and compares to hash_value of records in silver.customer that match on c_custkey
    * silver.customer table is set for change data feed (CDF) to track inserts, updates and deletes and pass that to downstream consumers
    * silver.customer will only contain one record for each c_custkey, the latest version of each record
- Created the dimensional customer table in the Gold schema gold.dim_customer (**create-table-gold-dim_customer.py**)
    * The table has its own autoincremented primary key (dim_customer_id) to track records in a SCD2 design
    * The table also contains start_date, end_date and is_current columns for historical record tracking SCD2.
    * This notebook file also contains code to drop the table if desired and to also remove the corresponding checkpoint directory so that the stream won't break.
- Streamed with AutoLoader the customer data from silver.customer to gold.dim_customer (**batch-gold-dim_customer.py**)
    * The stream reads the silver table's CDF log for SCD2 inserts and updates (no deletes in this pipe)
    * Uses schema inference and checkpointing
    * accepts source_filename from silver.customer; adds new columns in dataframe for start_date, end_date and is_current
  

TO DO:
- Add Sales fact and full pipeline from S3 Raw through all Medallion layers
  


Medallion Layers details:
- bronze.customer_raw (csv format, no transformation except for date_added, source_filename)
- silver.customer  (3NF with no overt history; history only being available via Delta Lake time travel)
- gold.dim_customer (fully dimensional SCD2: keeps history overtly in the records)

I built a Workflow to orchestrate the three streaming tasks that load the Medallion layers.

I also include some notebooks to validate and troubleshoot the stream tasks.

I include a notebook to reset the customer pipeline for testing purposes: tables and checkpoints. (reset-customer-pipe.py)
