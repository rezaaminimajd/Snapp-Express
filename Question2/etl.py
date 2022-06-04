import os
import click
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, struct, when


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


def transform(df, is_tweets):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe

    Return:
        df: processed dataframe
    """
    if is_tweets:
        df = extract_tweets_fields(df)
    else:
        df = extract_users_fields(df)

    return df


def extract_tweets_fields(df):
    add_fields_message = ['id', 'id_str', 'lang']
    add_fields_user = [
        'name', 'screen_name','location', 'description', 'url', 'protected',
        'followers_count', 'friends_count', 'listed_count', 'created_at', 'favourites_count',
        'statuses_count',  'profile_image_url_https'
    ]

    for field in add_fields_message:
        print(field)
        df = df.withColumn(field, col(f'message.{field}'))

    for field in add_fields_user:
        print(field)
        df = df.withColumn(field, col(f'message.user.{field}'))

    remove_fields = ['message', 'kafka_consume_ts']

    for field in remove_fields:
        df = df.drop(field)
    return df


def extract_users_fields(df):
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
    df.write.csv('s3a://' + os.path.join(bucket_name, processed_data_path), header=True)


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

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path)

    # transform data to desired form
    clean_df = transform(df, is_tweets)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()
