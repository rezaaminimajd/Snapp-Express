#!/bin/bash
echo "What is etl.py path? (e.g Question2/etl.py)"
read etl_path
echo "minio url:"
read minio_url
echo "minio access key:"
read minio_access_key
echo "minio secret key:"
read minio_secret_key
echo "bucket name:"
read bucket_name
echo "raw data path:"
read raw_data_path
echo "processed data path:"
read processed_data_path
echo "etl tweets? (True or False -> etl users)"
read is_tweets

bash spark-submit \
--packages software.amazon.awssdk:s3:2.17.52,org.apache.hadoop:hadoop-aws:3.1.2 \
"$etl_path" \
--minio_url "$minio_url" \
--minio_access_key "$minio_access_key" \
--minio_secret_key "$minio_secret_key" \
--bucket_name "$bucket_name" \
--raw_data_path "$raw_data_path" \
--processed_data_path "$processed_data_path" \
--is_tweets "$is_tweets"