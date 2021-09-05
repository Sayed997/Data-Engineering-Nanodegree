import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField,  DoubleType, StringType, IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    
    """ 
        The process_song_data function extracts song data in JSON file format from an AWS S3 bucket(input_data), transforms data into specified tables
        and then loads the tables back into a designated AWS S3 bucket(output_data) in parquet file format.
        Param1: Create a spark session 
        Param2: input data path where song data is stored 
        Param3: output data path where transformed data will be loaded
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    #specify schema for increased performance and control
    song_schema = StructType([
    StructField("artist_id", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_location", StringType()),
    StructField("artist_longitude", StringType()),
    StructField("artist_name", StringType()),
    StructField("duration", DoubleType()),
    StructField("num_songs", IntegerType()),
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("year", IntegerType()),
    ])
    
    # read song data file
    dfs = spark.read.json(song_data, schema=song_schema)
    
    # create temporary view of table in order to run SQL queries
    dfs.createOrReplaceTempView("song_table")

    # extract columns to create songs table
    dim_songs = spark.sql("""
                 SELECT song_id,
                 title,
                 artist_id,
                 year,
                 duration
                 FROM song_table
                 WHERE song_id IS NOT NULL
             """)
    
    # write songs table to parquet files partitioned by year and artist
    dim_songs.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+"songs")

    # extract columns to create artists table
    dim_artists = spark.sql("""
                     SELECT DISTINCT artist_id,
                     artist_name AS name,
                     artist_location AS location,
                     artist_latitude AS latitude,
                     artist_longitude AS longitude
                     FROM song_table
                     WHERE artist_id IS NOT NULL
                """)
    
    # write artists table to parquet files
    dim_artists.write.mode('overwrite').parquet(output_data+"artists")


def process_log_data(spark, input_data, output_data):
    
    """ The process_log_data function extracts log data in JSON file format from an AWS S3 bucket(input_data), transforms data into specified tables
        and then loads the tables back into a designated AWS S3 bucket(output_data) in parquet file format.
        Param1: Create a spark session 
        Param2: input data path where log data is stored 
        Param3: output data path where transformed data will be loaded
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    dfl = spark.read.json(log_data)
    
    # filter by actions for song plays
    dfl = dfl.filter(dfl.page == "NextSong")
    
    #create temporary view in order to run SQL queries
    dfl.createOrReplaceTempView("log_table")

    # extract columns for users table    
    dim_users = spark.sql("""
                  SELECT DISTINCT userId AS user_id,
                  firstName AS first_name,
                  lastName AS last_name,
                  gender,
                  level
                  FROM log_table
                  WHERE userId IS NOT NULL
              """)
    
    # write users table to parquet files
    dim_users.write.mode('overwrite').parquet(output_data+"users")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    #Convert ts field to timestamp
    time_convert = spark.sql("""
                 SELECT to_timestamp(ts/1000) as start_times
                 FROM log_table
                 WHERE ts IS NOT NULL
            """)
    
    #create temporary view of time_table to run SQL queries
    time_convert.createOrReplaceTempView("time_table")
    
    # extract columns to create time table
    dim_time = spark.sql("""
                       SELECT start_times as start_time,
                       hour(start_times) as hour,
                       dayofmonth(start_times) as day,
                       weekofyear(start_times) as week,
                       month(start_times) as month,
                       year(start_times) as year,
                       dayofweek(start_times) as weekday
                       FROM time_table
                    """)
    
    # write time table to parquet files partitioned by year and month
    dim_time.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs')

    # extract columns from joined song and log datasets to create songplays table 
    fact_songplays = spark.sql("""
                     SELECT monotonically_increasing_id() as songplay_id,
                     to_timestamp(lt.ts/1000) as start_time,
                     month(to_timestamp(lt.ts/1000)) as month,
                     year(to_timestamp(lt.ts/1000)) as year,
                     lt.userId as user_id,
                     lt.level as level,
                     st.song_id as song_id,
                     st.artist_id as artist_id,
                     lt.sessionId as session_id,
                     lt.location as location,
                     lt.userAgent as user_agent
                     FROM log_table lt
                     JOIN song_table st ON lt.song = st.title AND lt.artist = st.artist_name
                """)

    # write songplays table to parquet files partitioned by year and month
    fact_songplays.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dlake/"
    
    #input_data = "data/"
    #output_data = "data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
