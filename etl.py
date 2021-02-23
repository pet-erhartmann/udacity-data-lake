import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - start spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - define and read input files
    - create temp view
    - select and write parquet files for songs and artists
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # write song data to temp view song_data_table
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format("parquet").mode("overwrite").save(
        output_data + 'songs/song_data.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
        .drop_duplicates()

    # write artists table to parquet files
    artists_table.write.format("parquet").mode("overwrite").save(output_data + 'artists/artist_data.parquet')


def process_log_data(spark, input_data, output_data):
    """
    - define and read input files
    - filter on NextSong page only
    - select and write parquet files for user
    - add timestamp, datetime and uniqueId column
    - create temp view
    - select and write parquet files for time and songplays
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df[df.page == 'NextSong']

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates()

    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").save(output_data + 'users/user_data.parquet')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', F.to_timestamp(df.ts / 1000))

    # create datetime column from original timestamp column
    df = df.withColumn('datetime', F.to_date(df.timestamp))

    # create uniqueId column for log_data
    df = df.withColumn('uniqueId', monotonically_increasing_id())

    # write log data to temp view log_data_table
    df.createOrReplaceTempView("log_data_table")

    # extract columns to create time table
    time_table = spark.sql("""
        select DISTINCT
        timestamp
        , datetime AS start_time
        , hour(timestamp) AS hour
        , day(timestamp) AS day
        , weekofyear(timestamp) AS week
        , month(timestamp) AS month
        , year(timestamp) AS year
        , weekday(timestamp) AS weekday
        from log_data_table
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").mode("overwrite").save(
        output_data + 'time/time_data.parquet')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplay_table = spark.sql("""
        SELECT DISTINCT
            stg.uniqueId AS songplay_id,
            stg.ts AS start_time,
            month(stg.timestamp) AS month,
            year(stg.timestamp) AS year,
            stg.userId,
            stg.level,
            stg2.song_id,
            stg2.artist_id,
            stg.sessionId,
            stg.location,
            stg.userAgent
        FROM log_data_table stg
        LEFT JOIN song_data_table stg2
            ON stg.artist = stg2.artist_name
            AND stg.song = stg2.title
            AND stg.length = stg2.duration
        WHERE stg.userId IS NOT NULL
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").format("parquet").mode("overwrite").save(
        output_data + 'songplays/songplays_data.parquet')


def main():
    """
    - calls create_spark_session function
    - reads input and output locations from config file
    - runs process_song_data
    - runs process_log_data
    """
    spark = create_spark_session()

    input_data = config['STORAGE']['INPUT_DATA']
    output_data = config['STORAGE']['OUTPUT_DATA']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
