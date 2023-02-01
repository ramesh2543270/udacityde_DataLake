import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, \
     DoubleType as Dbl, LongType as Long, StringType as Str, \
     IntegerType as Int, DecimalType as Dec, DateType as Date, \
     TimestampType as Stamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def def_song_schema():
    """
    Defines the schema for song.
    
    Returns the schema
    """
    song_data_schema = R([        
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("artist_name", Str()),        
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dec()),
        Fld("artist_longitude", Dec()),
        Fld("artist_location", Str()),
        Fld("num_songs", Int()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    return song_data_schema

def process_song_data(spark, input_data, output_data):
    """
    Function to process input song data from s3
    And then write the desired results back to s3 for subsequent usage
    
    Input param spark: the spark session object
    Input param input_data: S3 bucket path that has input data
    Output param output_data: S3 bucket path that will store output data
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    print("Reading song data.")    
    df = spark.read.json(song_data, schema = def_song_schema())

    # extract columns to create songs table , also drops duplicates in the process
    songs_table = df.select("artist_id",
                            "song_id",
                            "title",
                            "year",
                            "duration").dropDuplicates(["song_id"])
    print("Read song_table data.") 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table.parquet",
                              partitionBy = ["year", "artist_id"],
                              mode = "overwrite")
    print("Wrote song_table data.") 

    # extract columns to create artists table , also drops duplicate in the process
    artists_table = df.select("artist_id",
                              "artist_name",
                              "artist_location",
                              "artist_latitude",
                              "artist_longitude").dropDuplicates(["artist_id"])
    print("Read artists_table data.") 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet",
                                mode = "overwrite")
    print("Wrote artists_table data.")


def process_log_data(spark, input_data, output_data):
    """
    Function to Process log data by creating other tables in our star schema
    such as user and time table
    and writing the result to a given S3 bucket.
    
    Input param spark: spark session object
    Input param input_data: S3 bucket with input data
    Input param output_data: S3 bucket for output data
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    #Define log schema
    log_schema = R([
        Fld("userId", Str()),
        Fld("sessionId", Str()),
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("lastName", Str()),        
        Fld("gender", Str()),
        Fld("itemInSession", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("song", Str()),
        Fld("status", Str()),
        Fld("ts", Long()),
        Fld("userAgent", Str())        
    ])
    
    # read log data file
    print("Reading log data")
    df = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    print("Extracting columns for users table")
    users_table = df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level").dropDuplicates(["user_id"]) 
    
    # write users table to parquet files
    print("Writing parquet for users table")
    users_table.write.parquet(output_data + "users_table.parquet",
                              mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    print("extract columns to create time table")
    time_table = df.selectExpr("timestamp as start_time",
                               "hour(timestamp) as hour",
                               "dayofmonth(timestamp) as day",
                               "weekofyear(timestamp) as week",
                               "month(timestamp) as month",
                               "year(timestamp) as year",
                               "dayofweek(timestamp) as weekday"
                               ).dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    print("write time table to parquet files partitioned by year and month")
    time_table.write.parquet(output_data + "time_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema = def_song_schema())

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("song_data")
    df.createOrReplaceTempView("log_data")
    # Join song_data & log_data using song title , length, duration
    print("Join song_data & log_data using song title , length, duration")
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                ld.timestamp as start_time,
                                year(ld.timestamp) as year,
                                month(ld.timestamp) as month,
                                ld.userId as user_id,
                                ld.level as level,
                                sd.song_id as song_id,
                                sd.artist_id as artist_id,
                                ld.sessionId as session_id,
                                ld.location as location,
                                ld.userAgent as user_agent
                                FROM log_data ld
                                JOIN song_data sd
                                ON (ld.song = sd.title
                                AND ld.length = sd.duration
                                AND ld.artist = sd.artist_name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    print("write songplays table to parquet files partitioned by year and month")
    songplays_table.write.parquet(output_data + "songplays_table.parquet",
                                  partitionBy=["year", "month"],
                                  mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://romy1-dend/"
    print("Start - process_song_data")
    process_song_data(spark, input_data, output_data)
    print("End - process_song_data")
    print("Start - process_log_data")
    process_log_data(spark, input_data, output_data)
    print("End - process_log_data")


if __name__ == "__main__":
    main()
