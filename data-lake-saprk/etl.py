import configparser
from datetime import datetime
import os
import functools
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: pyspark.sql.session.SparkSession, input_data: str, output_data: str) -> None:
    '''
        Loading song data from S3, transfrom and save back to S3
    '''
    
    # helping function to load data
    def read_song_json(path_list: list) -> pyspark.sql.dataframe.DataFrame:
        df = [spark.read.json(f'{input_data}/song_data/A/{i}/*/*.json') for i in path_list]
        return functools.reduce(lambda a,b: a.union(b), df)
    
    # read song data file
    df = read_song_json(['A', 'B'])

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # extract columns to create artists table
    artists_table = (
        df
        .select(
            'artist_id', 
            col('artist_name').alias('name'), 
            col('artist_location').alias('location'), 
            col('artist_latitude').alias('lattitude'), 
            col('artist_longitude').alias('longitude')
        )
    )
    
    # save results
    DF_LIST, NAME_LIST = [songs_table, artists_table], ['songs_table', 'artists_table']

    for i,j in zip(DF_LIST, NAME_LIST):
        try:
            i.write.parquet(f'{output_data}/{j}.parquet', mode = 'overwrite')
            print(f'{j} is saved!\n')
        except Exception as e:
            print(e)


def process_log_data(spark: pyspark.sql.session.SparkSession, input_data: str, output_data: str) -> None:
    '''
        Loading log data from S3, transfrom and save back to S3
    '''

    # read log data file
    df = (
        spark.read.json(f'{input_data}/log_data/*/*/*.json')
        .where(col('page') == 'NextSong')
        .withColumn('date_time', (col('ts').cast('float') / 1000).cast('timestamp'))
    )
    df.persist()

    # extract columns for users table    
    users_table = (
        df
        .select(
            col('userId').alias('user_id'), 
            col('firstName').alias('first_name'), 
            col('lastName').alias('last_name'), 
            'gender', 
            'level'
        )
        .drop_duplicates()
    )

    # create datetime column from original timestamp column
    df = df.withColumn('date_time', (col('ts').cast('float') / 1000).cast('timestamp'))
    
    # extract columns to create time table
    time_table = (
        df
        .select(
            col('date_time').alias('start_time'),
            hour('date_time').alias('hour'),
            dayofyear('date_time').alias('day'),
            weekofyear('date_time').alias('week'),
            month('date_time').alias('month'),
            year('date_time').alias('year'),
            dayofweek('date_time').alias('weekday')
        )
    )

    # read in song data to use for songplays table
    def read_song_parquet(path: str, file_name: str) -> pyspark.sql.dataframe.DataFrame:
        return spark.read.parquet(f'{path}/{file_name}')
    
    NAME_LIST = ['songs_table', 'artists_table']
    for i in NAME_LIST:
        try:
            globals()[i] = read_song_parquet(output_data, i)
            print(f'{i} is readed.\n')
        except Exception as e:
            print(e)

    window = Window.orderBy(monotonically_increasing_id())
    
    # extract columns to create songplay table
    songplays_table = (
        df
        .join(songs_table.select('title', 'song_id', 'artist_id'), on = col('song') == col('title'), how = 'inner')
        .join(artists_table.select('artist_id'), on = 'artist_id', how = 'inner')
        .select(
            row_number().over(window).alias('songplay_id'),
            col('date_time').alias('start_time'), 
            col('userid').alias('user_id'), 
            'level', 
            'song_id', 
            'artist_id', 
            col('sessionid').alias('session_id'), 
            'location', 
            col('useragent').alias('user_agent') 
        )
    )

    # save results
    DF_LIST, NAME_LIST = [users_table, time_table, songplays_table], ['users_table', 'time_table', 'songplays_table']

    for i,j in zip(DF_LIST, NAME_LIST):
        try:
            i.write.parquet(f'{output_data}/{j}', mode = 'overwrite')
            print(f'{j} is saved!\n')
        except Exception as e:
            print(e)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-project-bucket-mt"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
