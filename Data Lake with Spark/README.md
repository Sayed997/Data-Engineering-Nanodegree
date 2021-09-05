#   -Udacity Data Engineer Nanodegree
##  -Project 4
##  -Data Lake with AWS and Spark

### Project Summary
A music streaming app called Sparkify is experiencing exponential growth, their current data warehouse infrastructure does not meet the necessary storage and performance requirements any longer so they decided to use a distributed cluster with Spark as the main processing tool and shift to a Data Lake. The objective is to create a cost effective, performant and fault tolerant method of performing the entire ETL process on these large datasets.

The Client would like their Data Engineer to build an ETL pipeline that extracts their data from AWS S3, processes them using Spark, and loads the data back into AWS S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Data Summary
1. `song dataset` contains metadata about a song and the artist of that song in JSON format
-- Source: s3://udacity-dend/song_data
2. `log dataset` contains logs of events pertaining to songs on streaming app in JSON format
-- Source: s3://udacity-dend/log_data

### Summary of Tables
Our intention here was to create tables taking query efficiency and storage space amongst nodes into consideration
Partitioning of tables was applied to improve performance of Spark Jobs and reduce Data Skew


#### Dimension Tables
1. `dim_songs`  songs and related information in music database
- song_id, title, artist_id, year, duration
2. `dim_users` stores information on the users in the app
- user_id, first_name, last_name, gender, level
3. `dim_artists` stores the artists and related information in the music database
- artist_id, name, location, lattitude, longitude
4. `dim_time` stores the timestamps of records broken down into specific units of measurment
- start_time, hour, day, week, month, year, weekday

#### Fact Table
5. `fact_songplays` records in log data associated with song play events
-- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Implementation

Access to data files on AWS S3
- You will need to create an IAM Role with S3 read only Access
- You will need to create AWS credentials i.e., Secret Access Key and Access Key ID
- Input these credentials into the `dl.cfg` file

To Load transformed tables into S3 a personal S3 bucket needs to be created and the path needs to be provided in the
`output_data` variable in the `etl.py` file.

In addition to the data files, the project workspace includes 3 files:

1. `testlakesql.ipynb` notebook to test functionality of ETL processes locally on a small portion of the dataset  
2. `etl.py` reads and processes files from song_data and log_data and loads them into your tables.
3. `dl.cfg` contains credential information for AWS

To Execute the program and implement the ETL pipeline please run the `etl.py` file. All scripts should be run in the Terminal using the "python file.py" syntax for windows and "python3 file.py" for UNIX.







