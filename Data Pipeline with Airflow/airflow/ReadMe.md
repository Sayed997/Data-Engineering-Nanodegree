#   -Udacity Data Engineer Nanodegree
##  -Project 5
##  -Data Pipelines with Apache Airflow
##  -Sparkify Automates with Airflow

### Project Summary

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. Apache Airflow is a workflow engine that will easily schedule and run your complex data pipelines. It will make sure that each task of your data pipeline will get executed in the correct order and each task gets the required resources.

The client would like a Data Engineer to build a higher grade dynamic ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of fact and dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Data quality is of great importance and a set of Data Quality checks are to be implemented to ensure data quality specifications are met. The data pipeline should be built from reusable tasks, have the capability to be monitored, and allow for easy backfills.

### Usage of Airflow

Airflow will be used to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step. All tasks will be completed within airflow and configured using a Directed Acyclic Graph. Tasks can be monitored using the Airflow UI.

### Datasets Residing in Amazon S3

Both files follow JSON format
1. Log data: `s3://udacity-dend/log_data`
2. Song data: `s3://udacity-dend/song_data`

### Project Content

1. `dags/udac_example_dag.py` Contains all imports, the DAG, tasks and task dependencies.
2. `plugins/operators` Contains all the custom operators that were built to perform specified tasks.
- Data Quality Operator: Performs Data quality check once tables are created to ensure specifications are met.
- Load Fact and Dimension Operator: These Operators use the SQL code in the helper file to process the data and load it into the target Fact/Dimension tables.
- Stage Redshift Operator: This operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.
3. `plugins/helpers/sql_queries.py` Contains all the SQL transformations that are used to perform the ETL, imported into tasks accordingly.
4. `create_tables.sql` Contains all the SQL queries that creates the tables needed in the Data Warehouse.

### Summary of Tables Created 

#### Staging tables
1. `staging_events` contains all the data from the JSON Log file
2. `staging_songs` contains all the data from the JSON song metadata file

#### Fact Table
3. `Songplays` records in log data associated with song plays

#### Dimension Tables
4. `Songs`  songs in music database
5. `Users` stores the users in the app
6. `Artists` stores the artists in the music database
7. `Time` stores the timestamps of records broken down into specific units of measurment


### Project Implemention

1. Prior to running the DAG in Airflow all tables in the `create_tables.sql` file need to be created manually on your Redshift Cluster.
2. To access the Airflow Web Server and Launch the Airflow UI you will need to run this `/opt/airflow/start.sh` command.
3. Once Airflow UI is accessed toggle the DAG to on to start. ETL Pipline should run seemlessly according to set schedule once started.



