"""
This contains functions that process song and log data stored in S3.

Initially, this runs queries that load S3 data and save them into parquet files.
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Initialized Spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Ingest and process song data. """
    # get filepath to song data file
    # Note: it looks like the path is song-data NOT song_data as specified in the Udacity instructions
    # song_data = input_data + "song_data/*/*/*/*.json"
    song_data = input_data + "song-data/A/D/J/*.json"

    # read song data file
    # Reference: https://stackoverflow.com/a/60116904/975592
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # columns: song_id, title, artist_id, year, duration
    songs_table = df.select(
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    ).dropDuplicates()

    # create table from dataframe
    # Unsure if I need to create a table in this way?
    # songs_table.createOrReplaceTempView()

    # write songs table to parquet files partitioned by year and artist
    # Reference: https://stackoverflow.com/a/42023495/975592
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        output_data + 'songs_table.parquet',
        'overwrite'
    )

    # extract columns to create artists table
    # columns: artist_id, name, location, latitude, longitude
    artists_fields = [
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
    ]

    exprs = ["{} as {}".format(field, field.split("_")[1]) for field in artists_fields]
    artist_exprs = ['artist_id'] + exprs
    artists_clean = df.selectExpr(*artist_exprs).dropDuplicates()

    # write artists table to parquet files
    artists_clean.write.parquet(
        output_data + 'artists_table.parquet',
        'overwrite'
    )


def process_log_data(spark, input_data, output_data):
    """ Ingest and process log data. """
    # get filepath to log data file
    # log_data = input_data + "log-data/*/*/*/*.json"
    log_data = input_data + "log-data/2018/11/2018-11-30-events.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    # columns: user_id, first_name, last_name, gender, level
    users_fields = [
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]

    expr_mapping = {
        'userId': 'user_id',
        'firstName': 'first_name',
        'lastName': 'last_name',
        'gender': 'gender',
        'level': 'level'
    }

    exprs = ["{} as {}".format(field, expr_mapping[field]) for field in users_fields]
    users_clean = df.selectExpr(*exprs).dropDuplicates()

    # write users table to parquet files
    users_clean.write.parquet(
        output_data + 'users_table.parquet',
        'overwrite'
    )

    # create timestamp column from original timestamp column
    # original timestamp column is 'ts' and it is in milliseconds
    # reference: https://stackoverflow.com/a/748534/975592
    get_timestamp = udf(lambda x: str(x // 1000))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    # original timestamp column is 'ts'
    get_datetime = udf(lambda x: str(datetime.utcfromtimestamp(x // 1000).replace(microsecond=x % 1000 * 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))

    # extract columns to create time table
    # columns: start_time, hour, day, week, month, year, weekday
    time_table = df.select('datetime')\
        .withColumn('start_time', df.datetime)\
        .withColumn('hour', hour('datetime'))\
        .withColumn('day', dayofmonth('datetime'))\
        .withColumn('week', weekofyear('datetime'))\
        .withColumn('month', month('datetime'))\
        .withColumn('year', year('datetime'))\
        .withColumn('weekday', dayofweek('datetime'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table.parquet', 'overwrite')

    # read in song data to use for songplays table
    # song_data = input_data + "song_data/*/*/*/*.json"
    song_data = input_data + "song-data/A/D/J/*.json"
    song_df = spark.read.json(song_data)

    # join song and log datasets
    joined_df = df.join(
        song_df,
        df.song == song_df.title
    )

    # extract columns from joined song and log datasets to create songplays table
    # columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # this will draw from both dataframes

    # reference:
    # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html
    joined_df = joined_df.withColumn('songplay_id', monotonically_increasing_id())

    # Need to add year and month in order to partition parquet file
    joined_df = joined_df.withColumn('hour', hour('datetime'))
    joined_df = joined_df.withColumn('month', month('datetime'))

    songplays_table = joined_df.selectExpr(
        [
            'songplay_id',
            'datetime as start_time',
            'userId as user_id',
            'level',
            'song_id',
            'artist_id',
            'sessionId as session_id',
            'location',
            'userAgent as user_agent',
            'year',
            'month'
        ]
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplay_table.parquet', 'overwrite')


def main():
    """ Process song and log data """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output_data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    print("Script completed.")


if __name__ == "__main__":
    main()
