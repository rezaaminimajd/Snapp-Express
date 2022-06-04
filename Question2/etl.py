import os
import click
from pyspark.sql import SparkSession


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

    return sc.read.csv('s3a://' + os.path.os.path.join(bucket_name,
                                                       raw_data_path),
                       header=True)


def transform(df):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe

    Return:
        df: processed dataframe
    """
    # todo: write the your code here
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
    df.write.csv('s3a://' + os.path.os.path.join(bucket_name,
                                                 processed_data_path),
                 header=True)


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
def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path, processed_data_path):

    sc = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path)

    # transform data to desired form
    clean_df = transform(df)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()
