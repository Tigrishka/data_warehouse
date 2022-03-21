# Project 3: Data Warehouse
## Project Description
This project is aimed to help a startup called *Sparkify* move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The challenge is to build an ETL pipeline that extracts *Sparkify* data from **AWS S3**, stages them in **Redshift**, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


## Database Schema
The star schema includes one fact table - *songplays* and four dimention tables - *users*, *songs*, *artists*, *time*.<br>
All SQL queries to `CREATE` and `DROP`, as well as `STAGING` and `FINAL` tables are contained in the **sql_queries.py** and used by the following files in ETL pipeline:
1. The file **create_tables.py** uses functions `drop_tables` and `create_tables`. <br>
2. The file **etl.py** is where to load data from *S3* into staging tables on *Redshift* and then process that data into analytics tables on *Redshift*.<br> 

The song data dataset resides in S3 link `s3://udacity-dend/song_data` and populates *songs* and *artists* database tables.
The log data dataset resides in S3 link `s3://udacity-dend/log_data` and is loaded into *users* and *time* tables.<br>
The fact table *songplays* uses information from the *songs* table, *artists* table, and original log files located in the json path `s3://udacity-dend/log_json_path.json`.

## Files

**`dwh.cfg`** is a configuration file contained info about `Redshift`, `IAM` and `S3`. <br>
**`create_cluster.py`** creates IAM role, Redshift cluster, and allow TCP connection to the cluster endpoint from outside VPC
* the flag `--create` is to create resources
* the flag `--delete` is to delete resources

## ETL pipeline

1. Create tables and loaded data from S3 to staging tables on Redshift.
2. Load data from staging tables to analytics tables on Redshift.
3. Delete Redshift cluster when finished.

## How to run scripts

1. Set environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `DB_PASSWORD` in `dwh.cfg`

2. Create IAM role, Redshift cluster, and configure TCP connectivity by running the command
        
        python3 create_cluster.py --create
        
3. Complete the `dwh.cfg` with `[DB]HOST` (Endpoint) and `[IAM_ROLE]ARN`

4. To drop and create tables use the following command

        python3 create_tables.py
    
5. Run ETL pipeline **`etl.py`**

        python3 etl.py


6. Delete IAM role and Redshift cluster
        
        python3 create_cluster.py --delete