# databricks-sales
This repo has the code for my databricks code to stream sales data into a data lakehouse.

I chose data from the Databricks sample sets, chunked it, and loaded it into an S3 bucket.

I mounted the S3 bucket onto DBFS and used it for my landing zone.

From there, I use spark streaming and AutoLoader to load the files through a Medallion layer architecture in Databricks Unity Catalog.

I use Delta Tables to handle schema evolution, time travel, and schema inference.

For example:
- bronze.customer_raw (csv format, no transformation except for date_added, source_filename)
- silver.customer  (3NF with no overt history; history only being available via Delta Lake time travel)
- gold.dim_customer (fully dimensional SCD2: keeps history overtly in the records)

I built a Workflow to orchestrate the three streaming tasks that load the Medallion layers.

I also include some notebooks to validate and troubleshoot the stream tasks.
