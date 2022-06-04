import os
import click
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, regexp_replace, udf, lit, explode, array

udf_time_convert = udf(lambda x: time_convert(x), DateType())


def init_spark_connection(appname, sparkmaster, minio_url,
                          minio_access_key, minio_secret_key):
    """ Init Spark connection and set hadoop configuration to read
    data from MINIO.

    Args:
        appname: spark application name.
        sparkmaster: spark master url.
        minio_url: an url to access to MINIO.
        minio_access_key: specific access key to MINIO.
        minio_secret_key: specific secret key to MINIO.

    Return:
         sc: spark connection object
    """
    sc = SparkSession \
        .builder \
        .appName(appname) \
        .master(sparkmaster) \
        .getOrCreate()

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", minio_url)
    hadoop_conf.set("fs.s3a.access.key", minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
    return sc


def extract(sc, bucket_name, raw_data_path):
    """ Extract csv files from Minio.

    Args:
        sc: spark connection object.
        bucket_name: name of specific bucket in minio that contain data.
        raw_data_path: a path in bucket name that specifies data location.

    Return:
        df: raw dataframe.
    """
    uri = 's3a://' + os.path.join(bucket_name, raw_data_path)
    return sc.read.json(uri)


def transform(df, is_tweets, sc):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe
        is_tweets: bool for select tweets or users

    Return:
        df: processed dataframe
    """
    if is_tweets:
        df = transform_tweets(df, sc)
    else:
        df = transform_users(df)

    return df


def transform_users(df):
    add_fields_message = ['id', 'id_str', 'lang', 'name', 'screen_name', 'location', 'description', 'url', 'protected',
                          'followers_count', 'friends_count', 'listed_count', 'created_at', 'favourites_count',
                          'statuses_count', 'profile_image_url_https'
                          ]

    for field in add_fields_message:
        df = df.withColumn(field, col(f'message.{field}'))

    remove_space_fields = ['description', 'url', 'name', 'location']
    for field in remove_space_fields:
        df = df.withColumn(field, regexp_replace(col(field), " ", ""))

    remove_fields = ['message', 'kafka_consume_ts']

    for field in remove_fields:
        df = df.drop(field)

    df = df.withColumn('created_at', udf_time_convert(col("created_at")))
    df = df.dropDuplicates(['id'])
    return df


def time_convert(time):
    time_format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.datetime.strptime(time, time_format)


def add_missing_columns(df, ref_df):
    for col in ref_df.schema:
        if col.name not in df.columns:
            df = df.withColumn(col.name, lit(None).cast(col.dataType))
    df = df.drop('entities')
    return df


def transform_tweets(df, sc):
    df = df.drop('user')
    df = df.select('message')
    df = df.select(explode(array(col('message'))).alias('data')).select('data.*')
    new = df.filter(col("status.retweeted_status").isNotNull())
    new = new.select('status.retweeted_status')
    new = new.select(explode(array(col('retweeted_status'))).alias('data')).select('data.*')
    new = add_missing_columns(new, df)
    df = add_missing_columns(df, new)
    df.unionByName(new)
    df = df.dropDuplicates(['id'])
    df = df.withColumn('created_at', udf_time_convert(col("created_at")))
    columns = [item[0] for item in df.dtypes if item[1].startswith('string')]
    for column in columns:
        df = df.withColumn(column, regexp_replace(col(column), " ", ""))
    array_cols = ['status', 'friends_dic', 'followers_dic', 'place', 'extended_entities']
    for c in array_cols:
        df = df.drop(c)
    df = df.repartition('created_at')
    return df


def load(df, bucket_name, processed_data_path):
    """ Load clean dataframe to MINIO.

    Args:
        df: a processed dataframe.
        bucket_name: the name of specific bucket in minio that contain data.
        processed_data_path: a path in bucket name that
            specifies data location.

    Returns:
         Nothing!
    """
    # todo: change this function if
    df.write.partitionBy("created_at") \
        .mode("overwrite").csv('s3a://' + os.path.join(bucket_name, processed_data_path), header=True)


@click.command('ETL job')
@click.option('--appname', '-a', default='ETL Task', help='Spark app name')
@click.option('--sparkmaster', default='local',
              help='Spark master node address:port')
@click.option('--minio_url', default='local',
              help='import a module')
@click.option('--minio_access_key', default='xxxx')
@click.option('--minio_secret_key', default='xxxx')
@click.option('--bucket_name', default='xxxx')
@click.option('--raw_data_path', default='xxxx')
@click.option('--processed_data_path', default='xxxx')
@click.option('--is_tweets', default=True)
def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path, processed_data_path, is_tweets):
    sc = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    if is_tweets in ['False', 'false', 'f', 0]:
        is_tweets = False
    else:
        is_tweets = True

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path)

    # transform data to desired form
    clean_df = transform(df, is_tweets, sc)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()
