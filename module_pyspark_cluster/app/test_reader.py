import os
from datetime import datetime
from app.utils.reader import read_json_from_local, read_json_from_minio_with_spark


if __name__ == "__main__":
    bucket = "datalake"
    s3_url = f"raw/{datetime.today().strftime('%Y%m%d')}/dataset.txt"

    env = os.getenv("ENV", "LOCAL")
    if env == "PRD":
        df_read = read_json_from_minio_with_spark(bucket, s3_url)
    else:
        df_read = read_json_from_local("static_files/dataset.txt")
