import configparser
from datetime import datetime
import os
import boto3
import time
import functools
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, \
    ShortType, LongType, FloatType, StringType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    dayofweek, date_format


def log_me(func):
    """Print the runtime of the decorated function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f'Executing {func.__name__}...')
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        run_time = time.perf_counter() - start_time
        print(f'Finished {func.__name__} in {run_time:.1f} secs\n')
        return value
    return wrapper


def etl_init():
    """Set aws credentials as environment variables and return input and output
    locations.
    """

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # set aws credentials as OS environment variables (used by spark and boto3)
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = config['AWS']['AWS_DEFAULT_REGION']

    # get input location
    if config.getboolean('INPUT', 'LOCAL_INPUT'):
        song_files = config['INPUT']['LOCAL_SONG_PATH']
        log_files = config['INPUT']['LOCAL_LOG_PATH']
    else:
        song_files, log_files = get_files_s3(config['INPUT']['S3_BUCKET'],
                                             config['INPUT']['S3_SONG_PREFIX'],
                                             config['INPUT']['S3_LOG_PREFIX'])
    # get output location
    if config.getboolean('OUTPUT', 'LOCAL_OUTPUT'):
        output_loc = config['OUTPUT']['LOCAL_OUTPUT_PATH']
    else:
        output_loc = 's3a://{}/{}'.format(config['OUTPUT']['S3_BUCKET'],
                                          config['OUTPUT']['S3_PREFIX'])

    return song_files, log_files, output_loc


def get_files_s3(s3_bucket, song_prefix, log_prefix):
    """Get list of input song and log files on S3.
    Args:
        s3_bucket (str): S3 bucket name
        song_prefix (str): S3 key prefix common to all song files
        log_prefix (str): S3 key prefix common to all log files
    Returns:
         song_files (list of str): list of full paths to song files on S3
         log_files (list of str): list of full paths to log files on S3
    """
    s3_common_path = f's3a://{s3_bucket}/'

    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(s3_bucket)

    song_objs = my_bucket.objects.filter(Prefix=song_prefix)
    song_files = ['{}{}'.format(
        s3_common_path, obj.key) for obj in song_objs if obj.key[-5:] == '.json']

    log_objs = my_bucket.objects.filter(Prefix=log_prefix)
    log_files = ['{}{}'.format(
        s3_common_path, obj.key) for obj in log_objs if obj.key[-5:] == '.json']

    return song_files, log_files


@log_me
def create_spark_session():
    """Create and return spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


@log_me
def read_song_data(spark, song_files):
    """Read song data to spark DataFrame.
    Args:
        spark (obj): SparkSession
        song_files (list of str): list of full paths to song files on S3
    Returns:
         song_df (obj): DataFrame with song data
    """
    song_df_schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', FloatType()),
        StructField('artist_longitude', FloatType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', FloatType()),
        StructField('year', ShortType())
    ])

    # read song data files to DataFrame
    song_df = spark.read.json(song_files,
                              schema=song_df_schema,
                              mode='DROPMALFORMED')
    return song_df


@log_me
def read_log_data(spark, log_files):
    """Read log data to spark DataFrame.
    Args:
        spark (obj): SparkSession
        log_files (list of str): list of full paths to log files on S3
    Returns:
         log_df (obj): DataFrame with log data
    """
    log_df_schema = StructType([
        StructField('artist', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('lastName', StringType()),
        StructField('length', FloatType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('page', StringType()),
        StructField('sessionId', LongType()),
        StructField('song', StringType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
    ])

    # read log data files to DataFrame
    log_df = spark.read.json(log_files,
                             schema=log_df_schema,
                             mode='DROPMALFORMED')

    return log_df.filter(col('page') == 'NextSong')


@log_me
def process_songs(spark, song_df, output_loc):
    """Extract song data from input DataFrame and write to output location as
    collection of parquet files.
    Args:
        spark (obj): SparkSession
        song_df (obj): DataFrame with input data
        output_loc (str): output location
    """
    # extract columns to create songs table
    songs_table = song_df \
        .select('song_id', 'title', 'artist_id', 'year', 'duration') \
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode(
        'append').parquet(f'{output_loc}songs')

    print(f'Processed {songs_table.count()} songs')


@log_me
def process_artists(spark, song_df, output_loc):
    """Extract artist data from input DataFrame and write to output location as
    collection of parquet files.
    Args:
        spark (obj): SparkSession
        song_df (obj): DataFrame with input data
        output_loc (str): output location
    """
    # extract columns to create artists table
    artists_table = song_df \
        .select('artist_id',
                col('artist_name').alias('name'),
                col('artist_location').alias('location'),
                col('artist_latitude').alias('latitude'),
                col('artist_longitude').alias('longitude')) \
        .dropDuplicates()

    # write artists table to parquet file
    artists_table.write.mode('append').parquet(f'{output_loc}artists')

    print(f'Processed {artists_table.count()} artists')


@log_me
def process_users(spark, log_df, output_loc):
    """Extract users data from input DataFrame and write to output location as
    collection of parquet files.
    Args:
        spark (obj): SparkSession
        log_df (obj): DataFrame with input data
        output_loc (str): output location
    """
    # extract columns to create users table
    users_table = log_df \
        .select(col('userId').alias('user_id'),
                col('firstName').alias('first_name'),
                col('lastName').alias('last_name'),
                'gender',
                'level') \
        .dropDuplicates()

    # write users table to parquet file
    users_table.write.mode('append').parquet(f'{output_loc}users')

    print(f'Processed {users_table.count()} users')


@log_me
def process_time(spark, log_df, output_loc):
    """Extract time data from input DataFrame and write to output location as
    collection of parquet files.
    Args:
        spark (obj): SparkSession
        log_df (obj): DataFrame with input data
        output_loc (str): output location
    """
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime(
        '%Y-%m-%d %H:%M:%S'))

    # extract columns to create time table
    time_table = log_df.select(get_datetime('ts').cast('timestamp').alias(
        'start_time'))
    time_table = time_table \
        .select('start_time',
                hour('start_time').alias('hour'),
                dayofmonth('start_time').alias('day'),
                weekofyear('start_time').alias('week'),
                month('start_time').alias('month'),
                year('start_time').alias('year'),
                dayofweek('start_time').alias('weekday')) \
        .dropDuplicates()

    # write time table to parquet file
    time_table.write.partitionBy('year', 'month').mode(
        'append').parquet(f'{output_loc}time')

    print(f'Processed {time_table.count()} timestamps')


@log_me
def process_songplays(spark, song_df, log_df, output_loc):
    """Extract songplays data from input DataFrames and write to output
     location as collection of parquet files.
    Args:
        spark (obj): SparkSession
        song_df (obj): DataFrame with song input data
        log_df (obj): DataFrame with log input data
        output_loc (str): output location
    """
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime(
        '%Y-%m-%d %H:%M:%S'))

    # extract columns to create songplays table
    join_condition = [log_df.song == song_df.title,
                      log_df.artist == song_df.artist_name,
                      log_df.length == song_df.duration]

    songplays_table = song_df.alias('s') \
        .join(log_df.alias('l'), join_condition) \
        .select(
            log_df.userId.alias('user_id'),
            get_datetime(col('l.ts')).cast('timestamp').alias('start_time'),
            year(get_datetime(col('l.ts')).cast('timestamp')).alias('year'),
            month(get_datetime(col('l.ts')).cast('timestamp')).alias('month'),
            'l.level',
            's.song_id',
            's.artist_id',
            col('l.sessionId').alias('session_id'),
            'l.location',
            col('l.userAgent').alias('user_agent')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write users table to parquet file
    songplays_table.write.partitionBy('year', 'month').mode(
        'append').parquet(f'{output_loc}songplays')

    print(f'Processed {songplays_table.count()} songplays')


def main():
    """Extract data from song and log files to analytics DataFrames and save
    the DataFrames as collections of parquet files."""
    # set aws credentials as env vars and get input and output locations
    song_files, log_files, output_loc = etl_init()

    # create spark session
    spark = create_spark_session()

    # read data from song and log files to DataFrames
    song_df = read_song_data(spark, song_files)
    log_df = read_log_data(spark, log_files)

    # Extract data to analytics DataFrames and save them as collections of
    # parquet files.
    process_songs(spark, song_df, output_loc)
    process_artists(spark, song_df, output_loc)
    process_users(spark, log_df, output_loc)
    process_time(spark, log_df, output_loc)
    process_songplays(spark, song_df, log_df, output_loc)


if __name__ == "__main__":
    main()
