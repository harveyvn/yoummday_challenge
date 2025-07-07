import os
from minio import Minio
from app.utils.logging import log
from datetime import datetime
from app.models.spark_s3 import SparkS3


def upload_json_to_minio_with_spark(local_tsv_path: str, bucket_name: str, object_name: str):
    """
    Upload a JSON file to MinIO using Spark's s3a filesystem.

    :param local_tsv_path: Path to the local JSON file.
    :param bucket_name: MinIO bucket name.
    :param object_name: Object name inside the bucket (can include folders).
    """

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    app_name = "MinIOUpload"

    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' created.")
    else:
        log.info(f"Bucket '{bucket_name}' already exists.")

    spark = SparkS3(app_name=app_name, endpoint=endpoint, access_key=access_key, secret_key=secret_key).get_spark()
    df = spark.read.option("delimiter", "\t").option("header", "false").csv(local_tsv_path)
    log.info(f"Row count before write: {df.count()}")
    df.printSchema()
    df.show(10)
    s3a_path = f"s3a://{bucket_name}/{object_name}"
    df.write.mode("overwrite").parquet(s3a_path)
    log.info(f"Uploaded {local_tsv_path} to {s3a_path} on MinIO")
    spark.stop()


if __name__ == "__main__":
    file_names = ['userid-profile.tsv', 'userid-timestamp-artid-artname-traid-traname.tsv']
    local_file = "static_files/lastfm-dataset-1K"
    bucket = "datalake"

    for file_name in file_names:
        file_path = f"{local_file}/{file_name}"
        object_name = f"raw/{datetime.today().strftime('%Y%m%d')}/{file_name}"
        upload_json_to_minio_with_spark(file_path, bucket, object_name)
