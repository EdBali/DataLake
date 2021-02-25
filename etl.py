import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import songplays_query

config = configparser.ConfigParser()
config.read('dl.cfg')

#add a note about os.environ
    # os.environ is a mapping object that represents the user's environmental variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Retrieves Json files from the s3 udacity bucket, processes them using spark, and loads into another S3 in form of parquet files. 
    The data in the Json files is separated into different dataframes which we conceptually look at as tables in this case.
    These tables are stored in parquet files, which are stored in an s3 bucket.

    Arguments: 
        spark (:obj:`SparkSession`): Spark session. 
            A spark session is an object in pyspark that we use if we want to manipulate pyspark data frames
        input_data (:obj:`str`): Directory for the JSON  files(udacity s3 bucket).
        output_data (:obj:`str`): Directory where to save the parquet files(personal s3 bucket).
    '''
    # get filepath to song data file

    #input_data = "s3a://udacity-dend/"
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    song_table_output_path = output_data + "songs_table/songs_table.parquet"
    songs_table.write.partitionBy("year","artist_id").parquet(path = song_table_output_path,mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "name", "location", "latitude", "longitude")
    
    # write artists table to parquet files
    artists_table_output_path = output_data + "artists_table/artists_table.parquet"
    artists_table.write.parquet(path = artists_table_output_path,mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Retrieves Json files from the s3 udacity bucket, processes them using spark, and loads into another S3 in form of parquet files. 
    The data in the Json files is separated into different dataframes which we conceptually look at as tables in this case.
    These tables are stored in parquet files, which are stored in an s3 bucket.

    Arguments: 
        spark (:obj:`SparkSession`): Spark session. 
            A spark session is an object in pyspark that we use if we want to manipulate pyspark data frames
        input_data (:obj:`str`): Directory for the JSON  files(udacity s3 bucket).
        output_data (:obj:`str`): Directory where to save the parquet files(personal s3 bucket).
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("staging_logs")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates(['userId','level'])
    
    # write users table to parquet files
    users_table_output_path = output_data + "users_table/user_table.parquet"
    users_table.write.parquet(path = users_table_output_path,mode = "overwrite")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
    'timestamp',
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    date_format('datetime', 'F').alias('weekday')
)
    
    # write time table to parquet files partitioned by year and month
    time_table_output_path = output_data + "time_table/time_table.parquet"
    time_table.write.partitionBy("year","month").parquet(path = time_table_output_path,mode = "overwrite") 

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table/songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs")
    songplays_table = spark.sql(songplays_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_output_path = output_data + "/songplays_table/songplays_table.parquet"
    songplays_table.write.partitionBy("year", "month").parquet(path = songplays_table_output_path, mode = "overwrite")


def main():
    create_spark_session()
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://eddie-wamutu/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
