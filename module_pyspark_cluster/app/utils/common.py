import os
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from app.utils.reader import read_json_from_local, read_json_from_minio_with_spark


def get_data(bucket_name: str, s3_path: str) -> Tuple[DataFrame, SparkSession]:
    env = os.getenv("ENV", "LOCAL")
    if env == "PRD":
        df_read, spark = read_json_from_minio_with_spark(bucket_name, s3_path)
    else:
        df_read, spark = read_json_from_local("static_files/dataset.txt")

    return df_read, spark
