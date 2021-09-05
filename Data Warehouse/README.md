#   -Udacity Data Engineer Nanodegree
##  -Project 3
##  -Data Warehouse with AWS
##  -Sparkify moves to the Cloud

### Project Summary

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in AWS S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The client would like a Data Engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transform data into a set of fact and dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Moving to the coud provides scalibility and elasticity out of the box and a lesser need for IT Staff for hardware and software maintainence

### Database Design
We will be extracting files from Amazon S3 and staging them in Redshift which is a cloud managed, column oriented, Massivley Parallel Porcessing database. Our staging tables will include all the data extracted from S3 in table format. Thereafter we will transform and load the data following the star schema model design to create fact and dimension tables for the client to continue analysing data efficiently and achieve their analytical objects. The star schema optimizes for faster aggregations and a smaller amount of joins compared to the 3NF data model design.

The tables in the database will be distributed and sorted to allow for a low amount of shuffling as well as faster query performance

### Summary of Tables
Our intention here was to create tables taking query efficiency and storage space amongst nodes into consideration

#### Staging tables(order by will not be used much so decided not to sort staging tables)
1. `staging_events` contains all the data from the JSON Log file
-- This was a larger table where distribution option 'key' was more appropriate
2. `staging songs` contains all the data from the JSON song metadata file
-- This was a larger table where distribution option 'key' was more appropriate

#### Fact Table(Table sorted for query performance)
3. `factSongplays` records in log data associated with song plays
-- This was a smaller table so we decided to use the 'all' distribution option

#### Dimension Tables(Tables sorted for query performance)
4. `dimSongs`  songs in music database
-- This was a larger table where distribution option 'key' was more appropriate
5. `dimUsers` stores the users in the app
-- This was a smaller table so we decided to use the 'all' distribution option
6. `dimArtists` stores the artists in the music database
-- This was a larger table where distribution option 'key' was more appropriate
7. `dimTime` stores the timestamps of records broken down into specific units of measurment
-- This was a larger table where distribution option 'key' was more appropriate

#### Implementation

In addition to the data files, the project workspace includes 5 files:

1. `create_cluster.ipynb` uses infrastructure as code to create redshift cluster, therafter a few sql queries are implemented to test database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.py` reads and processes files from song_data and log_data and loads them into your tables.
4. `dwh.cfg` contains cluster, credential and data path information in. This information is saved as variables and used for appropriate scripts
5. `sql_queries.py` contains all sql queries, and is imported into appropriate files

To Execute the program please run the `create_tables.py` file to create your database and tables thereafter run the `etl.py` to implement the ETL pipeline. All scripts should be run in the Terminal using the "python file.py" syntax for windows and "python3 file.py" for UNIX.

