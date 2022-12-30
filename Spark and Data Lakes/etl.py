# import configparser
from datetime import datetime
import os
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import time


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a new Spark Session with the configuration & Return the Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # define song_schema
    song_schema = T.StructType([
        T.StructField("artist_id", T.StringType()),
        T.StructField("artist_latitude", T.DoubleType()),
        T.StructField("artist_location", T.StringType()),
        T.StructField("artist_longitude", T.StringType()),
        T.StructField("artist_name", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("num_songs", T.IntegerType()),
        T.StructField("title", T.StringType()),
        T.StructField("year", T.IntegerType()),
    ])
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song/song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data, song_schema) 

    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", F.monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_fields = ["artist_id", 
                  "artist_name as name", 
                  "artist_location as location", 
                  "artist_latitude as latitude",
                  "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log/*.json')

    # read log data file
    df_log = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong') 

    # extract columns for users table
    user_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df_log.selectExpr(user_fields).dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")
    
    # create timestamp column from original timestamp column
    df_log = df_log.withColumn('start_time', (df_log['ts']/1000).cast('timestamp'))
    
    # enrich data for time table
    df_log = df_log.withColumn("hour", F.hour("start_time")) \
                   .withColumn("day", F.dayofmonth("start_time")) \
                   .withColumn("week", F.weekofyear("start_time")) \
                   .withColumn("month", F.month("start_time")) \
                   .withColumn("year", F.year("start_time")) \
                   .withColumn("weekday", F.dayofweek("start_time"))
    # extract columns to create time table
    time_table = df_log.select("start_time", "hour", "day", "week", "month", "year", "weekday", "ts")


    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_schema = T.StructType([
        T.StructField("artist_id", T.StringType()),
        T.StructField("artist_latitude", T.DoubleType()),
        T.StructField("artist_location", T.StringType()),
        T.StructField("artist_longitude", T.StringType()),
        T.StructField("artist_name", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("num_songs", T.IntegerType()),
        T.StructField("title", T.StringType()),
        T.StructField("year", T.IntegerType()),
    ])
    song_data = os.path.join(input_data, 'song/song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data, song_schema) 
    
    # read song_table & collect selected columns for latter usage to enrich songplays table 
    song_id_df = spark.read.parquet(os.path.join(output_data, "songs"))
    song_id_df = song_id_df.select(F.col("title"), F.col("song_id"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log.join(song_df, (df_log.song == song_df.title)\
                                    & (df_log.artist == song_df.artist_name)\
                                    & (df_log.length == song_df.duration), "inner")\
                        .select('start_time', 'userId', 'level', 'title',\
                                'artist_id', 'sessionId','location','userAgent',\
                                df_log['year'].alias('year'), df_log['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())
    # join song_id_df to get the song_id column
    songplays_table = songplays_table.join(song_id_df, (songplays_table.title == song_id_df.title), "left").drop(song_id_df.title).distinct()
    
    # select final columns for songplays_table
    songplays_table = songplays_table.select(
            F.col('start_time'),
            F.col('userId').alias('user_id'),
            F.col('level'),
            F.col('song_id'),
            F.col('artist_id'),
            F.col('sessionId').alias('session_id'),
            F.col('userAgent').alias('user_agent'),
            F.col('year'),
            F.col('month'),
            F.col('location')
            ) \
            .repartition("year", "month") \
            .dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songsplay/")


def main():
    spark = create_spark_session()
    input_data = "./data/"
    output_data = "./output-data/"
    
    print("\n")
    
    print("Processing song_data files")
    process_song_data(spark, input_data, output_data)
    
    print("Processing log_data files")
    process_log_data(spark, input_data, output_data)
    spark.stop()
    print('Success')


if __name__ == "__main__":
    main()
