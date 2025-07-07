import os
from pyspark.sql import SparkSession
from app.utils.logging import log
from app.models.spark_s3 import SparkS3


def read_json_from_minio_with_spark(bucket_name: str, s3_path: str):
    """
    Read a JSON file from MinIO using Spark's s3a filesystem.

    :param bucket_name: MinIO bucket name.
    :param s3_path: Object name inside the bucket (can include folders).
    :return: Spark DataFrame
    """
    log.info(f"Reading from {s3_path}")

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    app_name = "MinIORead"
    s3a_path = f"s3a://{bucket_name}/{s3_path}"

    spark_reader = SparkS3(app_name=app_name, endpoint=endpoint, access_key=access_key, secret_key=secret_key)
    spark = spark_reader.get_spark()
    log.info(f"Spark version: {spark.sparkContext.version}")

    log.info(f"Attempting to read from: {s3a_path}")

    try:
        # For multi-line JSON files, you might need to add .option("multiLine", "true")
        df = spark.read.parquet(s3a_path)
        log.info(f"Successfully read data from {s3a_path}")
        # df.printSchema()
        # df.show()
        return df, spark
    except Exception as e:
        log.error(f"Error reading JSON from MinIO: {e}")
        raise


def read_json_from_local(local_json_path: str):
    """
    Read a JSON file from the local filesystem using Spark.

    :param local_json_path: Path to the local JSON file.
    :return: Spark DataFrame
    """
    log.info(f"Read a JSON file from the local filesystem {local_json_path} using Spark.")

    app_name = "LocalRead"

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    if not os.path.exists(local_json_path):
        log.error(f"Local JSON file not found at: {local_json_path}")
        spark.stop()
        raise FileNotFoundError(f"Local JSON file not found: {local_json_path}")

    log.info(f"Attempting to read local JSON file: {local_json_path}")

    try:
        df = spark.read.parquet(local_json_path)
        log.info(f"Successfully read data from {local_json_path}")
        # df.printSchema()
        # df.show()
        return df, spark
    except Exception as e:
        log.error(f"Error reading local JSON file: {e}")
        raise
